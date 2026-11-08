//! Optional SQLite storage Module. All SQL runs on the host blocking-I/O Scheduler.
//! FULL synchronous rollback-journal transactions acknowledge durable commits only.
#![forbid(unsafe_code)]
#![deny(missing_docs)]
use bytes::Bytes;
use modorch::{prelude::*, *};
use modorch_mqtt_api::*;
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

/// One database belongs to one broker, with an exclusive SQLite connection lock.
#[derive(Clone, Debug)]
pub struct SqliteStorageConfig {
    /// Storage Module identity.
    pub name: String,
    /// Exact broker Module identity, part of database format identity.
    pub broker: String,
    /// Application-global subscription ID.
    pub subscription_id: u64,
    /// Host-owned secret capability shared with Core, never sent to network clients.
    pub access: StoreAccessKey,
    /// SQLite file in an existing host-selected directory.
    pub database: PathBuf,
    /// Maximum encoded snapshot, 1 KiB..64 MiB.
    pub max_snapshot_bytes: usize,
    /// Physical envelope for main database, rollback journal and overhead reserve.
    pub disk_budget_bytes: u64,
}
impl SqliteStorageConfig {
    /// Validate quotas before opening any files.
    pub fn validate(&self) -> Result<(), MqttError> {
        if [&self.name, &self.broker]
            .iter()
            .any(|s| s.is_empty() || s.len() > 128 || s.contains(['/', '\0']))
            || self.name == self.broker
            || self.database.as_os_str().is_empty()
            || self.database == std::path::Path::new(":memory:")
            || self.database.to_string_lossy().starts_with("file:")
            || self.access.0 == [0; 32]
            || !(1024..=64 * 1024 * 1024).contains(&self.max_snapshot_bytes)
            || self.disk_budget_bytes < 3 * (self.max_snapshot_bytes as u64 + 1024 * 1024)
            || self.disk_budget_bytes > 1024 * 1024 * 1024 * 1024
        {
            return Err(MqttError::Configuration(
                "invalid SQLite storage configuration".into(),
            ));
        }
        Ok(())
    }
}
/// Factory retains no live database outside Module lifecycle.
pub struct SqliteStorageFactory {
    config: Arc<SqliteStorageConfig>,
}
impl SqliteStorageFactory {
    /// Validate without opening files or starting any tasks.
    pub fn new(config: SqliteStorageConfig) -> Result<Self, MqttError> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
        })
    }
}
fn descriptor(c: &SqliteStorageConfig) -> ComponentDescriptor {
    let mut d = ComponentDescriptor::new(ComponentRef::module(&c.name));
    d.queue_capacity = 16;
    d
}
impl ModuleFactory for SqliteStorageFactory {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
        Ok(Box::new(Storage {
            config: self.config.clone(),
            database: Arc::new(Mutex::new(None)),
            tasks: Arc::new(Mutex::new(None)),
        }))
    }
}
struct Storage {
    config: Arc<SqliteStorageConfig>,
    database: Arc<Mutex<Option<Database>>>,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
}
fn failure() -> FrameworkError {
    FrameworkError::InvalidConfig("SQLite storage unavailable or invalid".into())
}
#[async_trait]
impl Lifecycle for Storage {
    async fn setup(&mut self, ctx: &SetupContext) -> FrameworkResult<()> {
        let config = self.config.clone();
        let database = ctx
            .tasks
            .spawn_blocking_io(move |_| Database::open(&config).map_err(|_| failure()))?
            .wait_finished()
            .await?;
        *self.database.lock().unwrap() = Some(database);
        *self.tasks.lock().unwrap() = Some(ctx.tasks.clone());
        Ok(())
    }
    async fn quiesce(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
        self.tasks.lock().unwrap().take();
        // Root shutdown cancels newly scheduled jobs. Wait for the existing SQL
        // job to finish, then release its idle, autocommit DELETE-journal handle.
        // Closing this handle runs no SQL transaction or WAL checkpoint.
        loop {
            match self.database.try_lock() {
                Ok(mut database) => {
                    database.take();
                    return Ok(());
                }
                Err(std::sync::TryLockError::WouldBlock) => (),
                Err(_) => return Err(failure()),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
impl Module for Storage {
    fn descriptor(&self) -> ComponentDescriptor {
        descriptor(&self.config)
    }
    fn handlers(&mut self) -> FrameworkResult<Option<HandlerRegistration>> {
        Ok(Some(HandlerRegistration::Serial {
            handler: Box::new(StorageHandler {
                config: self.config.clone(),
                database: self.database.clone(),
                tasks: self.tasks.clone(),
            }),
            subscriptions: vec![SubscriptionSpec::new(
                self.config.subscription_id,
                self.descriptor().component(),
                STORAGE_TOPIC,
            )],
        }))
    }
}
struct StorageHandler {
    config: Arc<SqliteStorageConfig>,
    database: Arc<Mutex<Option<Database>>>,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
}
#[async_trait]
impl SerialHandler for StorageHandler {
    async fn handle(
        &mut self,
        ctx: &HandlerContext,
        message: MessageEnvelope,
    ) -> FrameworkResult<HandlerReply> {
        if message.kind != MessageKind::Request
            || message.destination != Destination::Component(ctx.component.clone())
        {
            return Ok(Arc::new(Err::<StoreReply, _>(StoreError::NotAuthorized)));
        }
        let envelope = message.payload::<StoreEnvelope>()?;
        // Capability is checked even for apparently trusted component sources.
        let mut mismatch = 0u8;
        for (a, b) in envelope.access.0.iter().zip(self.config.access.0) {
            mismatch |= a ^ b;
        }
        if mismatch != 0 {
            return Ok(Arc::new(Err::<StoreReply, _>(StoreError::NotAuthorized)));
        }
        let request = envelope.request.clone();
        let database = self.database.clone();
        let tasks = self.tasks.lock().unwrap().clone();
        let result = if let Some(tasks) = tasks {
            match tasks.spawn_blocking_io(move |cancel| {
                if cancel.is_cancelled() {
                    return Ok(Err(StoreError::Unavailable));
                }
                let mut lock = database.lock().unwrap();
                Ok(match lock.as_mut() {
                    Some(db) => db.execute(request),
                    None => Err(StoreError::Unavailable),
                })
            }) {
                Ok(task) => task
                    .wait_finished()
                    .await
                    .unwrap_or(Err(StoreError::Unavailable)),
                Err(_) => Err(StoreError::Unavailable),
            }
        } else {
            Err(StoreError::Unavailable)
        };
        Ok(Arc::new(result))
    }
}
struct Database {
    connection: Connection,
    max_snapshot: usize,
}
fn sql_error(error: rusqlite::Error) -> StoreError {
    if matches!(error,rusqlite::Error::SqliteFailure(ref e,_) if e.code==rusqlite::ErrorCode::DiskFull)
    {
        StoreError::Capacity
    } else {
        StoreError::Unavailable
    }
}
struct Meta {
    epoch: u64,
    version: u64,
    recovery: String,
    ready: i64,
    bytes: usize,
    digest: Vec<u8>,
    last_id: String,
    last_digest: Vec<u8>,
    last_version: u64,
}
impl Database {
    fn open(c: &SqliteStorageConfig) -> Result<Self, StoreError> {
        let connection = Connection::open(&c.database).map_err(sql_error)?;
        connection
            .busy_timeout(Duration::from_millis(100))
            .map_err(sql_error)?;
        // Do not initialize an unrelated database. An existing database with a
        // different page size would invalidate the physical quota calculation.
        let tables: Vec<String> = connection
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(sql_error)?
            .query_map([], |r| r.get(0))
            .map_err(sql_error)?
            .collect::<Result<_, _>>()
            .map_err(sql_error)?;
        if !tables.is_empty() && tables != ["broker"] {
            return Err(StoreError::Conflict);
        }
        connection
            .pragma_update(None, "page_size", 4096)
            .map_err(sql_error)?;
        let page_size: u64 = connection
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(sql_error)?;
        if page_size != 4096 {
            return Err(StoreError::Conflict);
        }
        connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA locking_mode=EXCLUSIVE; PRAGMA temp_store=MEMORY; PRAGMA secure_delete=ON;
            CREATE TABLE IF NOT EXISTS broker (id INTEGER PRIMARY KEY CHECK(id=1), name TEXT NOT NULL, epoch INTEGER NOT NULL, version INTEGER NOT NULL, recovery TEXT NOT NULL, ready INTEGER NOT NULL, image BLOB NOT NULL, digest BLOB NOT NULL, last_id TEXT NOT NULL, last_digest BLOB NOT NULL,last_version INTEGER NOT NULL);").map_err(sql_error)?;
        let pages = (c.disk_budget_bytes - 1024 * 1024) / 3 / 4096;
        let existing: u64 = connection
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .map_err(sql_error)?;
        if existing > pages {
            return Err(StoreError::Capacity);
        }
        connection
            .pragma_update(None, "max_page_count", pages)
            .map_err(sql_error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO broker VALUES(1,?1,0,0,'',0,X'',?2,'',X'',0)",
                params![c.broker, Sha256::digest([]).as_slice()],
            )
            .map_err(sql_error)?;
        let name: String = connection
            .query_row("SELECT name FROM broker WHERE id=1", [], |r| r.get(0))
            .map_err(sql_error)?;
        if name != c.broker {
            return Err(StoreError::Conflict);
        }
        let result = Self {
            connection,
            max_snapshot: c.max_snapshot_bytes,
        };
        let meta = result.meta()?;
        if meta.bytes > c.max_snapshot_bytes {
            return Err(StoreError::Capacity);
        }
        // Verify the latest image before serving any page or acquiring a writer.
        let image: Vec<u8> = result
            .connection
            .query_row("SELECT image FROM broker WHERE id=1", [], |r| r.get(0))
            .map_err(sql_error)?;
        if Sha256::digest(&image).as_slice() != meta.digest {
            return Err(StoreError::Corrupt);
        }
        Ok(result)
    }
    fn meta(&self) -> Result<Meta, StoreError> {
        self.connection.query_row("SELECT epoch,version,recovery,ready,length(image),digest,last_id,last_digest,last_version FROM broker WHERE id=1",[],|r|Ok(Meta{epoch:r.get(0)?,version:r.get(1)?,recovery:r.get(2)?,ready:r.get(3)?,bytes:r.get(4)?,digest:r.get(5)?,last_id:r.get(6)?,last_digest:r.get(7)?,last_version:r.get(8)?})).map_err(sql_error)
    }
    fn execute(&mut self, request: StoreRequest) -> StoreResult {
        let m = self.meta()?;
        match request {
            StoreRequest::Begin { recovery_id } => {
                if recovery_id.is_empty() || recovery_id.len() > 128 {
                    return Err(StoreError::Invalid);
                }
                let epoch = if recovery_id == m.recovery {
                    if m.ready != 0 {
                        return Err(StoreError::Conflict);
                    }
                    m.epoch
                } else {
                    let epoch = m
                        .epoch
                        .checked_add(1)
                        .filter(|v| *v <= i64::MAX as u64)
                        .ok_or(StoreError::Capacity)?;
                    self.connection
                        .execute(
                            "UPDATE broker SET epoch=?1,recovery=?2,ready=0 WHERE id=1",
                            params![epoch, recovery_id],
                        )
                        .map_err(sql_error)?;
                    epoch
                };
                Ok(StoreReply::Recovery {
                    epoch,
                    version: m.version,
                    bytes: m.bytes,
                    digest: m.digest.try_into().map_err(|_| StoreError::Corrupt)?,
                })
            }
            StoreRequest::Page {
                epoch,
                offset,
                max_bytes,
            } => {
                if epoch != m.epoch || m.ready != 0 {
                    return Err(StoreError::Fenced);
                }
                if max_bytes == 0 || max_bytes > 1024 * 1024 || offset > m.bytes {
                    return Err(StoreError::Invalid);
                }
                let data: Vec<u8> = self
                    .connection
                    .query_row(
                        "SELECT coalesce(substr(image,?1,?2),X'') FROM broker WHERE id=1",
                        params![offset + 1, max_bytes],
                        |r| r.get(0),
                    )
                    .map_err(sql_error)?;
                let done = offset + data.len() == m.bytes;
                Ok(StoreReply::Page {
                    epoch,
                    version: m.version,
                    offset,
                    data: Bytes::from(data),
                    done,
                })
            }
            StoreRequest::Complete { epoch } | StoreRequest::Abort { epoch } => {
                if epoch != m.epoch {
                    return Err(StoreError::Fenced);
                }
                let ready = if matches!(request, StoreRequest::Complete { .. }) {
                    if m.ready < 0 {
                        return Err(StoreError::Fenced);
                    }
                    1i64
                } else {
                    if m.ready == 1 {
                        return Err(StoreError::Conflict);
                    }
                    -1i64
                };
                self.connection
                    .execute("UPDATE broker SET ready=?1 WHERE id=1", [ready])
                    .map_err(sql_error)?;
                Ok(StoreReply::Ready)
            }
            StoreRequest::Query {
                epoch,
                operation_id,
                digest,
            } => {
                if epoch != m.epoch {
                    return Err(StoreError::Fenced);
                }
                if operation_id == m.last_id {
                    if m.last_digest != digest {
                        return Err(StoreError::Conflict);
                    }
                    Ok(StoreReply::Committed {
                        version: m.last_version,
                    })
                } else {
                    Ok(StoreReply::Unknown)
                }
            }
            StoreRequest::Commit {
                epoch,
                expected_version,
                operation_id,
                digest,
                image,
            } => {
                if epoch != m.epoch || m.ready != 1 {
                    return Err(StoreError::Fenced);
                }
                if operation_id.is_empty() || operation_id.len() > 128 {
                    return Err(StoreError::Invalid);
                }
                if operation_id == m.last_id {
                    if m.last_digest != digest || Sha256::digest(&image).as_slice() != digest {
                        return Err(StoreError::Conflict);
                    }
                    return Ok(StoreReply::Committed {
                        version: m.last_version,
                    });
                }
                if expected_version != m.version {
                    return Err(StoreError::Conflict);
                }
                if image.len() > self.max_snapshot {
                    return Err(StoreError::Capacity);
                }
                if Sha256::digest(&image).as_slice() != digest {
                    return Err(StoreError::Corrupt);
                }
                let version = m
                    .version
                    .checked_add(1)
                    .filter(|v| *v <= i64::MAX as u64)
                    .ok_or(StoreError::Capacity)?;
                let tx = self.connection.transaction().map_err(sql_error)?;
                tx.execute("UPDATE broker SET version=?1,image=?2,digest=?3,last_id=?4,last_digest=?3,last_version=?1 WHERE id=1",params![version,image.as_ref(),digest.as_slice(),operation_id]).map_err(sql_error)?;
                tx.commit().map_err(sql_error)?;
                Ok(StoreReply::Committed { version })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> SqliteStorageConfig {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SqliteStorageConfig {
            name: "store".into(),
            broker: "broker".into(),
            subscription_id: 30,
            access: StoreAccessKey([7; 32]),
            database: std::env::temp_dir()
                .join(format!("mqtt-store-{}-{n}.sqlite", std::process::id())),
            max_snapshot_bytes: 4096,
            disk_budget_bytes: 4 * 1024 * 1024,
        }
    }
    fn begin(db: &mut Database, id: &str) -> (u64, u64) {
        match db
            .execute(StoreRequest::Begin {
                recovery_id: id.into(),
            })
            .unwrap()
        {
            StoreReply::Recovery { epoch, version, .. } => (epoch, version),
            _ => panic!(),
        }
    }
    fn commit(epoch: u64, version: u64, id: &str, data: &[u8]) -> StoreRequest {
        StoreRequest::Commit {
            epoch,
            expected_version: version,
            operation_id: id.into(),
            digest: Sha256::digest(data).into(),
            image: Bytes::copy_from_slice(data),
        }
    }
    #[test]
    fn sqlite_epoch_cas_commit_query_and_recovery_abort_are_fenced_and_idempotent() {
        let c = config();
        let mut db = Database::open(&c).unwrap();
        let (e, v) = begin(&mut db, "first");
        assert_eq!(begin(&mut db, "first"), (e, v));
        assert_eq!(
            db.execute(commit(e, v, "one", b"first")).unwrap_err(),
            StoreError::Fenced
        );
        db.execute(StoreRequest::Complete { epoch: e }).unwrap();
        db.execute(commit(e, v, "one", b"first")).unwrap();
        assert!(matches!(
            db.execute(commit(e, v, "one", b"first")).unwrap(),
            StoreReply::Committed { version: 1 }
        ));
        assert_eq!(
            db.execute(commit(e, v, "one", b"changed")).unwrap_err(),
            StoreError::Conflict
        );
        assert_eq!(
            db.execute(commit(e, v, "different", b"changed"))
                .unwrap_err(),
            StoreError::Conflict
        );
        assert!(matches!(
            db.execute(StoreRequest::Query {
                epoch: e,
                operation_id: "one".into(),
                digest: Sha256::digest(b"first").into()
            })
            .unwrap(),
            StoreReply::Committed { version: 1 }
        ));
        let (new, version) = begin(&mut db, "second");
        assert!(new > e);
        assert_eq!(version, 1);
        assert_eq!(
            db.execute(commit(e, 1, "old", b"stale")).unwrap_err(),
            StoreError::Fenced
        );
        assert_eq!(
            db.execute(StoreRequest::Page {
                epoch: e,
                offset: 0,
                max_bytes: 2
            })
            .unwrap_err(),
            StoreError::Fenced
        );
        let mut image = Vec::new();
        for offset in [0, 2, 4] {
            let StoreReply::Page { data, done, .. } = db
                .execute(StoreRequest::Page {
                    epoch: new,
                    offset,
                    max_bytes: 2,
                })
                .unwrap()
            else {
                panic!()
            };
            image.extend(data);
            assert_eq!(done, offset == 4);
        }
        assert_eq!(image, b"first");
        db.execute(StoreRequest::Abort { epoch: new }).unwrap();
        db.execute(StoreRequest::Abort { epoch: new }).unwrap();
        assert_eq!(
            db.execute(StoreRequest::Complete { epoch: new })
                .unwrap_err(),
            StoreError::Fenced
        );
        drop(db);
        let mut db = Database::open(&c).unwrap();
        let (last, v) = begin(&mut db, "third");
        assert!(last > new);
        assert_eq!(v, 1);
        drop(db);
        std::fs::remove_file(c.database).unwrap();
    }
    #[test]
    fn sqlite_capacity_integrity_and_exclusive_database_identity_are_enforced() {
        let c = config();
        let mut db = Database::open(&c).unwrap();
        assert!(Database::open(&c).is_err());
        let (e, v) = begin(&mut db, "recovery");
        db.execute(StoreRequest::Complete { epoch: e }).unwrap();
        assert_eq!(
            db.execute(commit(e, v, "large", &vec![0; 4097]))
                .unwrap_err(),
            StoreError::Capacity
        );
        assert_eq!(db.meta().unwrap().version, v);
        let mut bad = commit(e, v, "digest", b"valid");
        if let StoreRequest::Commit { digest, .. } = &mut bad {
            *digest = [0; 32];
        }
        assert_eq!(db.execute(bad).unwrap_err(), StoreError::Corrupt);
        db.execute(commit(e, v, "ok", b"valid")).unwrap();
        let pages: u64 = db
            .connection
            .query_row("PRAGMA max_page_count", [], |r| r.get(0))
            .unwrap();
        assert!(pages * 4096 * 3 + 1024 * 1024 <= c.disk_budget_bytes);
        db.max_snapshot = 65536;
        let current: u64 = db
            .connection
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        db.connection
            .pragma_update(None, "max_page_count", current)
            .unwrap();
        assert_eq!(
            db.execute(commit(e, v + 1, "disk-full", &vec![7; 65536]))
                .unwrap_err(),
            StoreError::Capacity
        );
        assert_eq!(db.meta().unwrap().version, v + 1);
        db.connection
            .execute("UPDATE broker SET image=X'00' WHERE id=1", [])
            .unwrap();
        drop(db);
        assert!(matches!(Database::open(&c), Err(StoreError::Corrupt)));
        std::fs::remove_file(c.database).unwrap();
    }
    #[test]
    fn sqlite_refuses_unrelated_databases_and_nonstandard_pages_without_rewriting_them() {
        let c = config();
        let connection = Connection::open(&c.database).unwrap();
        connection
            .execute_batch("CREATE TABLE application_data (value TEXT); INSERT INTO application_data VALUES ('keep');")
            .unwrap();
        drop(connection);
        let original = std::fs::read(&c.database).unwrap();
        assert!(matches!(Database::open(&c), Err(StoreError::Conflict)));
        assert_eq!(std::fs::read(&c.database).unwrap(), original);
        std::fs::remove_file(&c.database).unwrap();

        let connection = Connection::open(&c.database).unwrap();
        connection
            .execute_batch("PRAGMA page_size=8192; CREATE TABLE broker (id INTEGER);")
            .unwrap();
        drop(connection);
        let original = std::fs::read(&c.database).unwrap();
        assert!(matches!(Database::open(&c), Err(StoreError::Conflict)));
        assert_eq!(std::fs::read(&c.database).unwrap(), original);
        std::fs::remove_file(&c.database).unwrap();
    }
}
