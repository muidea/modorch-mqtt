use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
const STATUS: &str = "$modorch/mqtt/control/status";
/// Bounded aggregate polling; no per-ClientID or per-Topic time series are generated.
#[derive(Clone, Debug)]
pub struct MetricsConfig {
    /// Metrics Module name.
    pub name: String,
    /// Core Module name.
    pub broker: String,
    /// Poll interval, 100 ms..1 hour.
    pub interval: Duration,
    /// Individual status Request deadline, at most the interval.
    pub timeout: Duration,
}
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            name: "mqtt-metrics".into(),
            broker: "mqtt-broker".into(),
            interval: Duration::from_secs(1),
            timeout: Duration::from_millis(500),
        }
    }
}
/// Most recently completed bounded snapshot.
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    /// Unix time of the last successful status response.
    pub timestamp_ms: u64,
    /// Latest Core gauges and monotonically accumulating Factory-local counters.
    pub broker: BrokerStatus,
    /// Poll failures observed by this Metrics Factory.
    pub poll_failures: u64,
}
/// Read-only local metrics observation. The host chooses its own HTTP exporter.
#[derive(Clone)]
pub struct MetricsHandle(Arc<Mutex<(Option<MetricsSnapshot>, u64)>>);
impl MetricsHandle {
    /// Latest successful snapshot, absent before the first successful poll.
    pub fn snapshot(&self) -> Option<MetricsSnapshot> {
        self.0.lock().unwrap().0.clone()
    }
    /// Render fixed-cardinality Prometheus text without sensitive labels.
    pub fn prometheus(&self) -> String {
        let Some(sample) = self.snapshot() else {
            return String::new();
        };
        let s = &sample.broker;
        let values = [
            ("storage_commits_total", s.storage_commits),
            ("storage_commit_failures_total", s.storage_commit_failures),
            (
                "storage_commit_duration_micros_total",
                s.storage_commit_duration_micros,
            ),
            ("storage_commit_max_micros", s.storage_commit_max_micros),
            ("connections", s.connections as u64),
            ("peer_buckets", s.peer_buckets as u64),
            ("rate_limited_connections_total", s.rate_limited_connections),
            ("sessions", s.sessions as u64),
            ("subscriptions", s.subscriptions as u64),
            ("pending_messages", s.pending_messages as u64),
            ("pending_bytes", s.pending_bytes as u64),
            ("inflight", s.inflight as u64),
            ("incoming_qos2", s.incoming_qos2 as u64),
            ("incoming_qos2_bytes", s.incoming_qos2_bytes as u64),
            ("queued_qos0", s.queued_qos0 as u64),
            ("retained", s.retained as u64),
            ("will_records", s.will_records as u64),
            ("memory_used_bytes", s.memory_used_bytes as u64),
            ("accepted_publications_total", s.accepted_publications),
            ("received_publications_total", s.received_publications),
            ("sent_publications_total", s.sent_publications),
            ("bytes_received_total", s.bytes_received),
            ("bytes_sent_total", s.bytes_sent),
            ("auth_rejections_total", s.auth_rejections),
            ("timeouts_total", s.timeouts),
            ("slow_writers_total", s.slow_writers),
            ("reconnects_total", s.reconnects),
            ("triggered_wills_total", s.triggered_wills),
            ("rejected_connections_total", s.rejected_connections),
            ("rejected_publications_total", s.rejected_publications),
            ("dropped_qos0_total", s.dropped_qos0),
            ("dropped_oversized_total", s.dropped_oversized),
            ("inbound_observed_total", s.inbound_observed),
            ("inbound_unobserved_total", s.inbound_unobserved),
            ("inbound_partial_total", s.inbound_partial),
            ("publish_records", s.publish_records as u64),
            ("metrics_poll_failures_total", sample.poll_failures),
        ];
        let mut output = String::new();
        for (name, value) in values {
            use std::fmt::Write;
            let _ = writeln!(output, "modorch_mqtt_{name} {value}");
        }
        output
    }
}
/// Optional Metrics factory; memory survives Module reconstruction, not process exit.
pub struct MetricsFactory {
    config: Arc<MetricsConfig>,
    handle: MetricsHandle,
}
fn descriptor(config: &MetricsConfig) -> ComponentDescriptor {
    let mut d = ComponentDescriptor::new(ComponentRef::module(&config.name));
    d.requires.push(ComponentRef::module(&config.broker));
    d
}
impl MetricsFactory {
    /// Validate interval, timeout and identities without starting a poller.
    pub fn new(config: MetricsConfig) -> Result<Self, MqttError> {
        if [&config.name, &config.broker]
            .into_iter()
            .any(|s| s.is_empty() || s.len() > 128 || s.contains(['/', '\0']))
            || config.name == config.broker
            || config.interval < Duration::from_millis(100)
            || config.interval > Duration::from_secs(3600)
            || config.timeout.is_zero()
            || config.timeout > config.interval
        {
            return Err(MqttError::Configuration(
                "invalid Metrics configuration".into(),
            ));
        }
        Ok(Self {
            config: Arc::new(config),
            handle: MetricsHandle(Arc::new(Mutex::new((None, 0)))),
        })
    }
    /// Observe snapshots or render text without exposing a listener.
    pub fn handle(&self) -> MetricsHandle {
        self.handle.clone()
    }
}
impl ModuleFactory for MetricsFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Metrics {
            config: self.config.clone(),
            handle: self.handle.clone(),
            task: None,
        }))
    }
}
struct Metrics {
    config: Arc<MetricsConfig>,
    handle: MetricsHandle,
    task: Option<TaskHandle>,
}
#[async_trait]
impl Lifecycle for Metrics {
    async fn begin_shutdown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(task) = &self.task {
            task.cancel();
        }
        Ok(())
    }
    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        if let Some(task) = &self.task {
            let _ = task.wait_finished().await;
        }
        Ok(())
    }
}
#[async_trait]
impl Module for Metrics {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    async fn activate_ingress(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
        let config = self.config.clone();
        let handle = self.handle.clone();
        let hub = ctx.event_hub.external();
        self.task = Some(ctx.tasks.spawn(move |cancel| async move {
            let mut ticks = tokio::time::interval(config.interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased; _=cancel.cancelled()=>break, _=ticks.tick()=>() }
                let response = async {
                    hub.request::<_, BrokerStatus>(
                        STATUS,
                        Destination::Component(ComponentRef::module(&config.broker)),
                        (),
                        MessageOptions {
                            payload_bytes: Some(0),
                            ..Default::default()
                        },
                    )?
                    .wait(config.timeout)
                    .await
                };
                let result =
                    tokio::select! { biased; _=cancel.cancelled()=>break, result=response=>result };
                let mut stored = handle.0.lock().unwrap();
                match result {
                    Ok(status) => {
                        stored.0 = Some(MetricsSnapshot {
                            timestamp_ms: unix_time_ms(),
                            broker: status.as_ref().clone(),
                            poll_failures: stored.1,
                        })
                    }
                    Err(_) => {
                        stored.1 = stored.1.saturating_add(1);
                        let failures = stored.1;
                        if let Some(sample) = &mut stored.0 {
                            sample.poll_failures = failures;
                        }
                    }
                }
            }
            Ok(())
        })?);
        Ok(())
    }
}
