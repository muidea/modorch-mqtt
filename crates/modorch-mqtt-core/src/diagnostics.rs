//! Explicit, bounded connection failure diagnostics without client data.
use modorch_mqtt_api::MqttError;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Queued,
    Transport,
    Connect,
    Authenticate,
    SessionAuthorize,
    SubscriptionAuthorize,
    WillAuthorize,
    AttachGate,
    AttachCommit,
    Connected,
    PacketGate,
    PacketCommit,
    OutboundGate,
    OutboundCommit,
}
const NAMES: &[&str] = &[
    "queued",
    "transport",
    "connect",
    "authenticate",
    "session-authorize",
    "subscription-authorize",
    "will-authorize",
    "attach-gate",
    "attach-commit",
    "connected",
    "packet-gate",
    "packet-commit",
    "outbound-gate",
    "outbound-commit",
];
pub(crate) struct Trace {
    id: usize,
    stage: AtomicUsize,
    changed_ms: AtomicU64,
    started: Instant,
}
pub(crate) type Diagnostics = Option<Arc<Trace>>;

pub(crate) fn configured() -> Diagnostics {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    (std::env::var_os("MODORCH_MQTT_DIAGNOSTICS").as_deref() == Some(std::ffi::OsStr::new("1")))
        .then(|| {
            Arc::new(Trace {
                id: NEXT.fetch_add(1, Ordering::Relaxed),
                stage: AtomicUsize::new(Stage::Queued as usize),
                changed_ms: AtomicU64::new(0),
                started: Instant::now(),
            })
        })
}
pub(crate) fn phase(trace: &Diagnostics, stage: Stage) {
    if let Some(trace) = trace {
        let previous = trace.stage.swap(stage as usize, Ordering::Relaxed);
        let now = trace
            .started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let before = trace.changed_ms.swap(now, Ordering::Relaxed);
        if now.saturating_sub(before) >= 250 {
            emit(
                trace,
                previous,
                &format!("phase completed after {}ms", now.saturating_sub(before)),
            );
        }
    }
}
fn emit(trace: &Trace, stage: usize, reason: &str) {
    #[cfg(test)]
    let context = std::thread::current().name().unwrap_or("test").to_owned();
    #[cfg(not(test))]
    let context = "runtime";
    eprintln!(
        "mqtt connection diagnostic: trace={} context={context} stage={} elapsed_ms={} reason={reason}",
        trace.id,
        NAMES[stage],
        trace.started.elapsed().as_millis()
    );
}
pub(crate) fn failure(trace: &Diagnostics, error: &MqttError) {
    if let Some(trace) = trace {
        // Domain errors contain fixed protocol labels; omit configuration/IO
        // strings, identities, peer addresses and all packet/credential content.
        let reason = match error {
            MqttError::Protocol(label)
            | MqttError::Capacity(label)
            | MqttError::Unsupported(label) => *label,
            MqttError::Storage { .. } => "storage failure",
            MqttError::NotAuthorized => "not authorized",
            MqttError::Closed => "closed",
            MqttError::SessionConflict => "session conflict",
            MqttError::Io(_) => "transport IO",
            MqttError::Configuration(_) => "configuration",
        };
        emit(trace, trace.stage.load(Ordering::Relaxed), reason);
    }
}
