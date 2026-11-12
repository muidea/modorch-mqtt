//! Development-host ownership: retain the same Application and Runtime until drained.
use modorch::{
    Application, ApplicationHandle, FrameworkError, FrameworkResult, ShutdownOptions,
    ShutdownReceipt,
};
use std::{future::Future, time::Duration};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
pub struct DrainBudget {
    pub timeout: Duration,
    pub retry: Duration,
}
impl Default for DrainBudget {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            retry: Duration::from_millis(50),
        }
    }
}

pub async fn drain(app: &ApplicationHandle, budget: DrainBudget) -> FrameworkResult<()> {
    loop {
        match app
            .shutdown_checked(ShutdownOptions {
                timeout: budget.timeout,
            })
            .await
        {
            Ok(ShutdownReceipt::GracefullyStopped) => return Ok(()),
            Ok(ShutdownReceipt::Forced { .. }) => {
                return Err(FrameworkError::InvalidConfig(
                    "Forced shutdown does not prove resource release".into(),
                ));
            }
            Err(error @ FrameworkError::StopIncomplete { .. }) => {
                eprintln!("{error}; retaining Application/Runtime and retrying");
                tokio::time::sleep(budget.retry).await;
            }
            Err(error) => return Err(error),
        }
    }
}

pub async fn start(app: Application, budget: DrainBudget) -> Result<ApplicationHandle> {
    match app.start().await {
        Ok(handle) => Ok(handle),
        Err(error) => {
            if error.cleanup.is_err() {
                if let Err(cleanup) = drain(&error.handle, budget).await {
                    eprintln!("startup cleanup retry failed: {cleanup}; preserving startup cause");
                }
            }
            Err(Box::new(error))
        }
    }
}

pub async fn wait_and_drain<F>(app: &ApplicationHandle, stop: F, budget: DrainBudget) -> Result<()>
where
    F: Future<Output = std::io::Result<()>>,
{
    let outcome: Result<()> = tokio::select! {
        result = stop => result.map_err(Into::into),
        result = app.wait() => result.map_err(Into::into),
    };
    let failure = app.health().failure;
    let cleanup = drain(app, budget).await;
    // Always drain first, retaining the primary run/signal error if both fail.
    if let Err(error) = &cleanup {
        eprintln!("checked shutdown failed: {error}");
    }
    outcome?;
    cleanup?;
    if let Some(error) = failure.or(app.health().failure) {
        return Err(Box::new(error));
    }
    Ok(())
}

#[cfg(unix)]
pub struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}
#[cfg(unix)]
impl Signals {
    pub fn install() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }
    pub async fn wait(&mut self) -> std::io::Result<()> {
        let received = tokio::select! {
            result = self.interrupt.recv() => result,
            result = self.terminate.recv() => result,
        };
        received.ok_or_else(|| std::io::Error::other("signal stream closed"))
    }
}

#[cfg(not(unix))]
pub struct Signals;
#[cfg(not(unix))]
impl Signals {
    pub fn install() -> std::io::Result<Self> {
        Ok(Self)
    }
    pub async fn wait(&mut self) -> std::io::Result<()> {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use modorch::{async_trait, prelude::*, *};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct TestModule {
        scheduler: Arc<std::sync::Mutex<Option<TaskScheduler>>>,
        release: Arc<tokio::sync::Notify>,
        finished: Arc<AtomicBool>,
        teardowns: Arc<AtomicUsize>,
        startup_failure: bool,
    }
    impl ModuleFactory for TestModule {
        fn descriptor(&self) -> ComponentDescriptor {
            ComponentDescriptor::new(ComponentRef::module("sample-host-test"))
        }
        fn create(&self, _: &ComponentConfig) -> FrameworkResult<Box<dyn Module>> {
            Ok(Box::new(self.clone()))
        }
    }
    #[async_trait]
    impl Lifecycle for TestModule {
        async fn start(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()> {
            *self.scheduler.lock().unwrap() = Some(ctx.tasks.clone());
            let release = self.release.clone();
            let finished = self.finished.clone();
            ctx.tasks.spawn(move |_| async move {
                release.notified().await;
                finished.store(true, Ordering::SeqCst);
                Ok(())
            })?;
            Ok(())
        }
        async fn ready(&mut self, _: &RuntimeContext) -> FrameworkResult<()> {
            if self.startup_failure {
                Err(FrameworkError::InvalidConfig(
                    "injected startup cause".into(),
                ))
            } else {
                Ok(())
            }
        }
        async fn teardown(&mut self, _: &ShutdownContext) -> FrameworkResult<()> {
            let attempt = self.teardowns.fetch_add(1, Ordering::SeqCst);
            if self.startup_failure && attempt == 0 {
                Err(FrameworkError::InvalidConfig(
                    "transient cleanup failure".into(),
                ))
            } else {
                Ok(())
            }
        }
    }
    impl Module for TestModule {
        fn descriptor(&self) -> ComponentDescriptor {
            ModuleFactory::descriptor(self)
        }
    }

    struct Fixture {
        application: Application,
        shared: TestModule,
    }
    fn fixture(startup_failure: bool) -> Fixture {
        let shared = TestModule {
            scheduler: Arc::new(std::sync::Mutex::new(None)),
            release: Arc::new(tokio::sync::Notify::new()),
            finished: Arc::new(AtomicBool::new(false)),
            teardowns: Arc::new(AtomicUsize::new(0)),
            startup_failure,
        };
        let mut builder = ApplicationBuilder::new("sample-host-test");
        builder.register_module(shared.clone()).unwrap();
        let mut selected = ComponentSelection::new();
        selected.enable_module("sample-host-test");
        Fixture {
            application: builder.selection(selected).build().unwrap(),
            shared,
        }
    }

    fn short_budget() -> DrainBudget {
        DrainBudget {
            timeout: Duration::from_millis(20),
            retry: Duration::from_millis(5),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn incomplete_drain_retains_same_application_and_real_task() -> Result<()> {
        let f = fixture(false);
        let app = start(f.application, short_budget()).await?;
        let id = app.id();
        let (cleanup, ()) = tokio::join!(drain(&app, short_budget()), async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while app.health().state != ApplicationState::StopIncomplete {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(app.id(), id);
            assert!(!f.shared.finished.load(Ordering::SeqCst));
            assert!(app.task_metrics().tasks > 0);
            f.shared.release.notify_one();
        });
        cleanup?;
        assert!(f.shared.finished.load(Ordering::SeqCst));
        assert_eq!(app.task_metrics().tasks, 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn running_failure_is_preserved_after_resources_drain() -> Result<()> {
        let f = fixture(false);
        let app = start(f.application, short_budget()).await?;
        let tasks = f.shared.scheduler.lock().unwrap().clone().unwrap();
        tasks
            .spawn(|_| async { Err(FrameworkError::InvalidConfig("injected run cause".into())) })?;
        f.shared.release.notify_one();
        let error = wait_and_drain(&app, std::future::pending(), short_budget())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected run cause"));
        assert!(f.shared.finished.load(Ordering::SeqCst));
        assert_eq!(app.task_metrics().tasks, 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stop_wait_error_still_drains_real_task() -> Result<()> {
        let f = fixture(false);
        let app = start(f.application, short_budget()).await?;
        let stop = async { Err(std::io::Error::other("injected signal wait cause")) };
        let (outcome, ()) = tokio::join!(wait_and_drain(&app, stop, short_budget()), async {
            tokio::time::sleep(Duration::from_millis(75)).await;
            f.shared.release.notify_one();
        });
        assert!(
            outcome
                .unwrap_err()
                .to_string()
                .contains("injected signal wait cause")
        );
        assert!(f.shared.finished.load(Ordering::SeqCst));
        assert_eq!(app.task_metrics().tasks, 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn startup_failure_retains_its_cause_through_cleanup_retry() {
        let f = fixture(true);
        let (started, ()) = tokio::join!(start(f.application, short_budget()), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            f.shared.release.notify_one();
        });
        assert!(
            started
                .err()
                .unwrap()
                .to_string()
                .contains("injected startup cause")
        );
        assert!(f.shared.finished.load(Ordering::SeqCst));
        assert_eq!(f.shared.teardowns.load(Ordering::SeqCst), 2);
    }
}
