use crate::{config::StorageProviderConfig, connection::SharedState};
use bytes::Bytes;
use modorch::{
    ComponentRef, Destination, EventHub, ExternalEventHub, MessageOptions, TaskScheduler,
};
use modorch_mqtt_api::*;
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Clone, Default)]
pub(crate) struct Persistence {
    gate: Arc<AsyncMutex<()>>,
    writer: Arc<Mutex<Option<Writer>>>,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
}
struct Writer {
    epoch: u64,
    version: u64,
    digest: [u8; 32],
}
#[derive(Clone, Copy)]
pub(crate) enum Hub<'a> {
    External(&'a ExternalEventHub),
    Internal(&'a EventHub),
}
fn failed() -> MqttError {
    MqttError::Protocol("persistent storage unavailable")
}
pub(crate) async fn call(
    hub: Hub<'_>,
    config: &StorageProviderConfig,
    request: StoreRequest,
) -> Result<StoreReply, MqttError> {
    let operation = match &request {
        StoreRequest::Begin { .. } => "begin",
        StoreRequest::Page { .. } => "page",
        StoreRequest::Complete { .. } => "complete",
        StoreRequest::Abort { .. } => "abort",
        StoreRequest::Commit { .. } => "commit",
        StoreRequest::Query { .. } => "query",
    };
    let failure = |failure| {
        // Keep diagnostics typed and sanitized; never retain Hub error detail,
        // SQL, paths, capabilities, image bytes or application identifiers.
        #[cfg(test)]
        eprintln!("store diagnostic: operation={operation} failure={failure:?}");
        MqttError::Storage { operation, failure }
    };
    let bytes = match &request {
        StoreRequest::Commit { image, .. } => image.len() + 512,
        _ => 512,
    };
    let options = MessageOptions {
        payload_bytes: Some(bytes),
        ..Default::default()
    };
    let destination = Destination::Component(ComponentRef::module(&config.component));
    let envelope = StoreEnvelope {
        access: config.access.clone(),
        request,
    };
    let handle = match hub {
        Hub::External(h) => {
            h.request::<_, StoreResult>(STORAGE_TOPIC, destination, envelope, options)
        }
        Hub::Internal(h) => {
            h.request::<_, StoreResult>(STORAGE_TOPIC, destination, envelope, options)
        }
    }
    .map_err(|_| failure(StoreCallFailure::Admission))?;
    handle
        .wait(config.timeout)
        .await
        .map_err(|error| {
            failure(match error {
                modorch::FrameworkError::CanceledBeforeStart(_) => {
                    StoreCallFailure::CanceledBeforeStart
                }
                modorch::FrameworkError::Indeterminate(_) => StoreCallFailure::Indeterminate,
                _ => StoreCallFailure::Transport,
            })
        })?
        .as_ref()
        .clone()
        .map_err(|error| failure(StoreCallFailure::Provider(error)))
}
// Cap serialization before Vec growth; the caller reserves four image bounds for
// DTOs, serialization, transport and recovery scratch against the same Budget.
struct ImageBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for ImageBuffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other("snapshot bound"));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Persistence {
    pub fn set_tasks(&self, tasks: TaskScheduler) {
        *self.tasks.lock().unwrap() = Some(tasks);
    }
    pub async fn begin(&self, state: &SharedState) -> Result<Transaction, MqttError> {
        let lock = self.gate.clone().lock_owned().await;
        let mut state_lock = state.lock().unwrap();
        if state_lock.storage_poisoned {
            return Err(failed());
        }
        let enabled = state_lock.config.storage.is_some();
        // Deferred detach records retain the original connection permit/memory;
        // no new admission can create an unbounded queue during a Store await.
        while let Some(d) = state_lock.deferred.pop_front() {
            state_lock.detach(&d.client, d.connection, d.graceful);
        }
        Ok(Transaction {
            persistence: self.clone(),
            state: state.clone(),
            _lock: lock,
            complete: !enabled,
            started: std::time::Instant::now(),
        })
    }
    pub async fn shutdown(&self, state: &SharedState) {
        let _lock = self.gate.lock().await;
        let mut state = state.lock().unwrap();
        while let Some(d) = state.deferred.pop_front() {
            state.detach(&d.client, d.connection, d.graceful);
        }
        // Durable mode already persisted armed Wills. A shutdown does not start
        // a new disk promise; recovery normalizes lost TCP owners before listen.
        state.process_wills();
    }
    pub async fn recover(&self, state: &SharedState, hub: &EventHub) -> Result<(), MqttError> {
        let config = state.lock().unwrap().config.storage.clone();
        let Some(config) = config else { return Ok(()) };
        let _lock = self.gate.lock().await;
        state.lock().unwrap().deferred.clear();
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|_| failed())?;
        let recovery_id = random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let external = hub.external();
        let mut epoch_to_abort = None;
        #[cfg(test)]
        let (mut recovered_pages, mut recovered_bytes) = (0, 0);
        let work = async {
            let reply = call(
                Hub::External(&external),
                &config,
                StoreRequest::Begin {
                    recovery_id: recovery_id.clone(),
                },
            )
            .await?;
            let StoreReply::Recovery {
                epoch,
                version,
                bytes,
                digest,
            } = reply
            else {
                return Err(failed());
            };
            epoch_to_abort = Some(epoch);
            if bytes > config.max_snapshot_bytes {
                return Err(MqttError::Capacity("recovery image"));
            }
            let budget = state.lock().unwrap().budget.clone();
            let _charge = budget.reserve(config.max_snapshot_bytes * 4)?;
            let mut image = Vec::with_capacity(bytes);
            let mut done = false;
            while !done {
                let reply = call(
                    Hub::External(&external),
                    &config,
                    StoreRequest::Page {
                        epoch,
                        offset: image.len(),
                        max_bytes: config.recovery_page_bytes,
                    },
                )
                .await?;
                let StoreReply::Page {
                    epoch: e,
                    version: v,
                    offset,
                    data,
                    done: end,
                } = reply
                else {
                    return Err(failed());
                };
                if e != epoch
                    || v != version
                    || offset != image.len()
                    || data.len() > config.recovery_page_bytes
                    || data.len() > bytes - image.len()
                    || (data.is_empty() && !end)
                    || (end != (image.len() + data.len() == bytes))
                {
                    return Err(failed());
                }
                image.extend_from_slice(&data);
                #[cfg(test)]
                {
                    recovered_pages += 1;
                    recovered_bytes = image.len();
                }
                done = end;
            }
            if image.len() != bytes || Sha256::digest(&image).as_slice() != digest {
                return Err(failed());
            }
            if !image.is_empty() {
                let tasks = self.tasks.lock().unwrap().clone().ok_or_else(failed)?;
                let (snapshot, _workspace_charge) = tasks
                    .spawn_cpu(move |_| {
                        // Cancellation of recovery must not release the reservation
                        // while the managed CPU job is still decoding the image.
                        let charge = _charge;
                        serde_json::from_slice::<crate::state::snapshot::Snapshot>(&image)
                            .map(|snapshot| (snapshot, charge))
                            .map_err(crate::framework)
                    })
                    .map_err(|_| failed())?
                    .wait_finished()
                    .await
                    .map_err(|_| failed())?;
                state.lock().unwrap().restore(snapshot)?;
            } else {
                let mut s = state.lock().unwrap();
                // A new durable provider cannot silently claim pre-existing memory state.
                if s.status().sessions != 0
                    || s.status().retained != 0
                    || s.status().publish_records != 0
                {
                    return Err(failed());
                }
                s.storage_poisoned = false;
            }
            if !matches!(
                call(
                    Hub::External(&external),
                    &config,
                    StoreRequest::Complete { epoch }
                )
                .await?,
                StoreReply::Ready
            ) {
                return Err(failed());
            }
            *self.writer.lock().unwrap() = Some(Writer {
                epoch,
                version,
                digest,
            });
            Ok(())
        };
        let result = match tokio::time::timeout(config.recovery_timeout, work).await {
            Ok(result) => result,
            Err(_) => {
                #[cfg(test)]
                eprintln!(
                    "store diagnostic: operation=recovery failure=RecoveryDeadline pages={recovered_pages} bytes={recovered_bytes}"
                );
                Err(MqttError::Storage {
                    operation: "recovery",
                    failure: StoreCallFailure::RecoveryDeadline,
                })
            }
        };
        if result.is_err() {
            state.lock().unwrap().storage_poisoned = true;
            if let Some(epoch) = epoch_to_abort {
                // Compensation uses the lifecycle causal Hub, not a new external root.
                let _ = call(Hub::Internal(hub), &config, StoreRequest::Abort { epoch }).await;
            }
        }
        result
    }
}
pub(crate) struct Transaction {
    persistence: Persistence,
    state: SharedState,
    _lock: OwnedMutexGuard<()>,
    complete: bool,
    started: std::time::Instant,
}
impl Transaction {
    pub async fn commit(mut self, hub: Hub<'_>) -> Result<(), MqttError> {
        if self.complete {
            return Ok(());
        }
        let (config, snapshot, charge) = {
            let state = self.state.lock().unwrap();
            let config = state.config.storage.clone().ok_or_else(failed)?;
            let charge = state.budget.reserve(config.max_snapshot_bytes * 4)?;
            (config, state.snapshot(), charge)
        };
        let tasks = self
            .persistence
            .tasks
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(failed)?;
        let limit = config.max_snapshot_bytes;
        let (image, digest, _workspace_charge) = tasks
            .spawn_cpu(move |cancel| {
                let workspace_charge = charge;
                let mut buffer = ImageBuffer {
                    bytes: Vec::with_capacity(limit),
                    limit,
                };
                serde_json::to_writer(&mut buffer, &snapshot).map_err(crate::framework)?;
                if cancel.is_cancelled() {
                    return Err(crate::framework("snapshot encoding canceled"));
                }
                let digest: [u8; 32] = Sha256::digest(&buffer.bytes).into();
                Ok((Bytes::from(buffer.bytes), digest, workspace_charge))
            })
            .map_err(|_| failed())?
            .wait_finished()
            .await
            .map_err(|_| failed())?;
        let (epoch, version, unchanged) = {
            let writer = self.persistence.writer.lock().unwrap();
            let w = writer.as_ref().ok_or_else(failed)?;
            (w.epoch, w.version, w.digest == digest)
        };
        if !unchanged {
            let operation_id = format!("{epoch}:{}", version.checked_add(1).ok_or_else(failed)?);
            let request = StoreRequest::Commit {
                epoch,
                expected_version: version,
                operation_id: operation_id.clone(),
                digest,
                image,
            };
            let result = call(hub, &config, request.clone()).await;
            let mut last_failure = None;
            let confirmed = match result {
                Ok(StoreReply::Committed { version: v }) if v == version + 1 => true,
                original => {
                    if let Err(error) = original {
                        last_failure = Some(error);
                    }
                    // Even a business I/O error can follow a disk commit. Resolve the
                    // same operation before any later transaction or ACK is permitted.
                    match call(
                        hub,
                        &config,
                        StoreRequest::Query {
                            epoch,
                            operation_id,
                            digest,
                        },
                    )
                    .await
                    {
                        Ok(StoreReply::Committed { version: v }) if v == version + 1 => true,
                        Ok(StoreReply::Unknown) => match call(hub, &config, request).await {
                            Ok(StoreReply::Committed { version: v }) if v == version + 1 => true,
                            Err(error) => {
                                last_failure = Some(error);
                                false
                            }
                            _ => false,
                        },
                        Err(error) => {
                            last_failure = Some(error);
                            false
                        }
                        _ => false,
                    }
                }
            };
            if !confirmed {
                return Err(last_failure.unwrap_or_else(failed));
            }
            *self.persistence.writer.lock().unwrap() = Some(Writer {
                epoch,
                version: version + 1,
                digest,
            });
        }
        if !unchanged {
            self.state
                .lock()
                .unwrap()
                .record_storage_commit(self.started.elapsed(), true);
        }
        self.complete = true;
        Ok(())
    }
}
impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = self.state.lock().unwrap();
            state.storage_poisoned = true;
            state.record_storage_commit(self.started.elapsed(), false);
            let generation = state.generation;
            state.stop(generation);
        }
    }
}
