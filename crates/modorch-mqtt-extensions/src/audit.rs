use modorch::{FrameworkError, FrameworkResult, TaskScheduler};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Optional bounded durable Admin audit, separate from the Broker state database.
#[derive(Clone, Debug)]
pub struct SqliteAuditConfig {
    /// Dedicated local SQLite file; unrelated databases and URI paths are rejected.
    pub path: PathBuf,
    /// Maximum retained rows, 16..1000000; oldest rows are deleted transactionally.
    pub max_records: usize,
    /// Physical main-file page limit. DELETE journal can need up to one extra file.
    pub max_database_bytes: usize,
}
impl SqliteAuditConfig {
    /// Check path and retention/page limits without opening a file.
    pub fn validate(&self) -> bool {
        let path = self.path.as_os_str().to_string_lossy();
        !path.is_empty()
            && !path.starts_with("file:")
            && path != ":memory:"
            && (16..=1000000).contains(&self.max_records)
            && (65536..=1024 * 1024 * 1024).contains(&self.max_database_bytes)
    }
}
#[derive(Clone)]
pub(crate) struct AuditLog {
    config: SqliteAuditConfig,
    connection: Arc<Mutex<Option<rusqlite::Connection>>>,
    tasks: Arc<Mutex<Option<TaskScheduler>>>,
}
fn failure() -> FrameworkError {
    FrameworkError::InvalidConfig("persistent Admin audit unavailable".into())
}
impl AuditLog {
    pub fn new(config: SqliteAuditConfig) -> Self {
        Self {
            config,
            connection: Arc::new(Mutex::new(None)),
            tasks: Arc::new(Mutex::new(None)),
        }
    }
    pub async fn open(&self, tasks: &TaskScheduler) -> FrameworkResult<()> {
        let this = self.clone();
        tasks.spawn_blocking_io(move|_|Ok((||{
            let db=rusqlite::Connection::open(&this.config.path).map_err(|_|failure())?;
            db.busy_timeout(std::time::Duration::from_secs(2)).map_err(|_|failure())?;
            let id:i64=db.query_row("PRAGMA application_id",[],|r|r.get(0)).map_err(|_|failure())?;
            let tables:i64=db.query_row("SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",[],|r|r.get(0)).map_err(|_|failure())?;
            if id!=0x4d514155&&(id!=0||tables!=0){return Err(failure());}
            db.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA locking_mode=EXCLUSIVE; PRAGMA page_size=4096; PRAGMA application_id=1297170773; CREATE TABLE IF NOT EXISTS audit(seq INTEGER PRIMARY KEY, ts INTEGER NOT NULL, source TEXT, operation TEXT NOT NULL, phase TEXT NOT NULL, success INTEGER NOT NULL);").map_err(|_|failure())?;
            let page_size:i64=db.query_row("PRAGMA page_size",[],|r|r.get(0)).map_err(|_|failure())?;
            if page_size!=4096{return Err(failure());}
            db.pragma_update(None,"max_page_count",(this.config.max_database_bytes/page_size as usize) as i64).map_err(|_|failure())?;
            let pages:i64=db.query_row("PRAGMA page_count",[],|r|r.get(0)).map_err(|_|failure())?;
            let rows:i64=db.query_row("SELECT count(*) FROM audit",[],|r|r.get(0)).map_err(|_|failure())?;
            if pages as usize*4096>this.config.max_database_bytes||rows as usize>this.config.max_records{return Err(failure());}
            *this.connection.lock().unwrap()=Some(db);Ok(())
        })()))?.wait_finished().await??;
        *self.tasks.lock().unwrap() = Some(tasks.clone());
        Ok(())
    }
    pub async fn append(
        &self,
        source: Option<String>,
        operation: &'static str,
        phase: &'static str,
        success: bool,
    ) -> FrameworkResult<()> {
        let this = self.clone();
        let tasks = self.tasks.lock().unwrap().clone().ok_or_else(failure)?;
        tasks.spawn_blocking_io(move|_|Ok((||{
            let mut db=this.connection.lock().unwrap();let db=db.as_mut().ok_or_else(failure)?;
            let tx=db.transaction().map_err(|_|failure())?;
            // Reserve one slot before insert; failure rolls back both deletion and insert.
            tx.execute("DELETE FROM audit WHERE seq NOT IN (SELECT seq FROM audit ORDER BY seq DESC LIMIT ?1)",[(this.config.max_records-1) as i64]).map_err(|_|failure())?;
            tx.execute("INSERT INTO audit(ts,source,operation,phase,success) VALUES(?1,?2,?3,?4,?5)",rusqlite::params![modorch_mqtt_api::unix_time_ms() as i64,source,operation,phase,success]).map_err(|_|failure())?;
            tx.commit().map_err(|_|failure())?;Ok(())
        })()))?.wait_finished().await?
    }
    pub fn close(&self) {
        self.tasks.lock().unwrap().take();
        self.connection.lock().unwrap().take();
    }
}
