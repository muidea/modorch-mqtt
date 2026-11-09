use super::*;
use argon2::{PasswordHasher, password_hash::SaltString};

fn config() -> PasswordAuthConfig {
    PasswordAuthConfig {
        users: vec![UserPolicy {
            username: "alice".into(),
            subject: "alice".into(),
            password_hash: Argon2::default()
                .hash_password(
                    b"password",
                    &SaltString::encode_b64(b"fixture-salt-1234").unwrap(),
                )
                .unwrap()
                .to_string(),
            client_id_prefixes: vec!["client-".into()],
            publish: vec![],
            subscribe: vec!["safe/#".into()],
        }],
        ..Default::default()
    }
}
fn authenticate() -> AuthRequest {
    AuthRequest::Authenticate(Authenticate {
        client_id: "client-a".into(),
        username: Some("alice".into()),
        password: Some(bytes::Bytes::from_static(b"password")),
        peer_ip: "127.0.0.1".parse().unwrap(),
    })
}
fn principal() -> Principal {
    Principal {
        key: PrincipalKey {
            domain: "mqtt-users".into(),
            subject: "alice".into(),
        },
        policy_version: 1,
        expires_at_ms: u64::MAX,
    }
}
fn request(hub: &ExternalEventHub, value: AuthRequest) -> RequestHandle<AuthResult> {
    hub.request(
        AUTH_TOPIC,
        Destination::Component(ComponentRef::module("mqtt-auth")),
        value,
        MessageOptions::default(),
    )
    .unwrap()
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_password_work_does_not_block_session_acl_or_new_policy_decisions() {
    let initial = config();
    let factory = PasswordAuthFactory::new(initial.clone()).unwrap();
    let slots = factory.password_slots.clone();
    let policy = factory.handle();
    // Emulate an occupied physical executor without depending on Argon2 speed.
    let occupied = slots.execution.clone().acquire_owned().await.unwrap();
    let mut builder = ApplicationBuilder::new("auth-lane-isolation");
    builder.register_module(factory).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("mqtt-auth");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let hub = app.event_hub().external();
    let pending = request(&hub, authenticate());
    until(|| slots.admission.available_permits() == 3).await;
    let attach = request(
        &hub,
        AuthRequest::SessionAttach(AuthorizeSessionAttach {
            principal: principal(),
            client_id: "client-a".into(),
            existing_owner: None,
            expected_version: None,
            clean_session: false,
            takeover: false,
        }),
    );
    let result = attach.wait(Duration::from_millis(300)).await;
    // Release before asserting so a pre-fix failure cannot strand the test host.
    if result.is_err() {
        drop(occupied);
        drop(pending);
        panic!("SessionAttach blocked behind password work: {result:?}");
    }
    assert!(matches!(
        result.unwrap().as_ref(),
        Ok(AuthDecision::Allowed)
    ));
    let mut replaced = initial;
    replaced.policy_version = 2;
    replaced.users[0].subscribe.clear();
    policy.replace(replaced).unwrap();
    let acl = request(
        &hub,
        AuthRequest::Subscribe(AuthorizeSubscribe {
            principal: principal(),
            filter: "safe/#".into(),
        }),
    );
    assert!(matches!(
        acl.wait(Duration::from_secs(1)).await.unwrap().as_ref(),
        Err(AuthError::Denied)
    ));
    drop(occupied);
    let reply = pending.wait(Duration::from_secs(10)).await.unwrap();
    assert!(matches!(reply.as_ref(), Ok(AuthDecision::Principal(p)) if p.policy_version == 1));
    until(|| slots.execution.available_permits() == 1 && slots.admission.available_permits() == 4)
        .await;
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

#[tokio::test]
async fn saturated_password_admission_fails_fast_while_acl_runs_and_running_timeout_retains_ownership()
 {
    let factory = PasswordAuthFactory::new(config()).unwrap();
    let slots = factory.password_slots.clone();
    let occupied = slots.execution.clone().acquire_owned().await.unwrap();
    let mut builder = ApplicationBuilder::new("password-admission");
    builder.register_module(factory).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("mqtt-auth");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let hub = app.event_hub().external();
    let mut pending = Vec::new();
    for expected in (0..4).rev() {
        pending.push(request(&hub, authenticate()));
        until(|| slots.admission.available_permits() == expected).await;
    }
    let overflow = request(&hub, authenticate())
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(overflow.as_ref(), Err(AuthError::Unavailable)));
    let acl = request(
        &hub,
        AuthRequest::Subscribe(AuthorizeSubscribe {
            principal: principal(),
            filter: "safe/#".into(),
        }),
    )
    .wait(Duration::from_secs(1))
    .await
    .unwrap();
    assert!(matches!(acl.as_ref(), Ok(AuthDecision::Allowed)));
    let timed_out = pending.pop().unwrap().wait(Duration::from_millis(10)).await;
    assert!(matches!(timed_out, Err(FrameworkError::Indeterminate(_))));
    assert_eq!(
        slots.admission.available_permits(),
        0,
        "caller timeout cannot release Running work"
    );
    drop(occupied);
    for operation in pending {
        assert!(matches!(
            operation
                .wait(Duration::from_secs(30))
                .await
                .unwrap()
                .as_ref(),
            Ok(AuthDecision::Principal(_))
        ));
    }
    until(|| slots.admission.available_permits() == 4 && slots.execution.available_permits() == 1)
        .await;
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

// The gate blocks an actual managed CPU closure after it has taken both permits.
// ReleaseOnDrop ensures an assertion failure cannot strand a blocking executor.
#[derive(Default)]
struct PhysicalState {
    entered: usize,
    active: usize,
    exited: usize,
    peak: usize,
    open: bool,
    cancel: Option<CancellationToken>,
}
pub(super) struct PasswordProbe {
    pub restart: bool,
    pub schedulers: Mutex<Vec<TaskScheduler>>,
    state: Mutex<PhysicalState>,
    gate: std::sync::Condvar,
}
impl PasswordProbe {
    fn new(restart: bool) -> Arc<Self> {
        Arc::new(Self {
            restart,
            schedulers: Mutex::new(vec![]),
            state: Mutex::new(PhysicalState::default()),
            gate: std::sync::Condvar::new(),
        })
    }
    pub fn enter(self: &Arc<Self>, cancel: CancellationToken) -> PhysicalGuard {
        let mut state = self.state.lock().unwrap();
        state.entered += 1;
        state.active += 1;
        state.peak = state.peak.max(state.active);
        state.cancel = Some(cancel);
        while !state.open {
            state = self.gate.wait(state).unwrap();
        }
        PhysicalGuard(self.clone())
    }
    fn release(&self) {
        self.state.lock().unwrap().open = true;
        self.gate.notify_all();
    }
}
pub(super) struct PhysicalGuard(Arc<PasswordProbe>);
impl Drop for PhysicalGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.active -= 1;
        state.exited += 1;
    }
}
struct ReleaseOnDrop(Arc<PasswordProbe>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
async fn probed_app(restart: bool) -> (ApplicationHandle, Arc<PasswordSlots>, Arc<PasswordProbe>) {
    let factory = PasswordAuthFactory::new(config()).unwrap();
    let slots = factory.password_slots.clone();
    let probe = PasswordProbe::new(restart);
    *slots.probe.lock().unwrap() = Some(probe.clone());
    let mut builder = ApplicationBuilder::new("physical-password-lifecycle");
    builder.register_module(factory).unwrap();
    let mut selection = ComponentSelection::new();
    selection.enable_module("mqtt-auth");
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    (app, slots, probe)
}
fn assert_not_authenticated(reply: FrameworkResult<Arc<AuthResult>>) {
    if let Ok(reply) = reply {
        assert!(!matches!(reply.as_ref(), Ok(AuthDecision::Principal(_))));
    }
}

#[tokio::test]
async fn physical_password_timeout_stop_incomplete_and_forced_shutdown_keep_permits_until_cpu_exit()
{
    for force in [false, true] {
        let (app, slots, probe) = probed_app(false).await;
        let _release_on_failure = ReleaseOnDrop(probe.clone());
        let hub = app.event_hub().external();
        let active = request(&hub, authenticate());
        until(|| probe.state.lock().unwrap().active == 1).await;
        let mut waiting = vec![];
        for expected in (0..3).rev() {
            waiting.push(request(&hub, authenticate()));
            until(|| slots.admission.available_permits() == expected).await;
        }
        let acl = request(
            &hub,
            AuthRequest::Subscribe(AuthorizeSubscribe {
                principal: principal(),
                filter: "safe/#".into(),
            }),
        )
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
        assert!(matches!(acl.as_ref(), Ok(AuthDecision::Allowed)));
        assert!(matches!(
            active.wait(Duration::from_millis(10)).await,
            Err(FrameworkError::Indeterminate(_))
        ));
        assert_eq!(slots.admission.available_permits(), 0);
        assert_eq!(slots.execution.available_permits(), 0);
        assert!(
            app.shutdown_checked(ShutdownOptions {
                timeout: Duration::from_millis(20)
            })
            .await
            .is_err()
        );
        assert_eq!(app.health().state, ApplicationState::StopIncomplete);
        until(|| {
            probe
                .state
                .lock()
                .unwrap()
                .cancel
                .as_ref()
                .unwrap()
                .is_cancelled()
        })
        .await;
        assert_eq!(probe.state.lock().unwrap().active, 1);
        assert!(app.health().pending.operations > 0);
        if force {
            let receipt = app.force_shutdown().await;
            assert!(
                matches!(receipt, ShutdownReceipt::Forced { pending } if pending.operations > 0)
            );
            assert_eq!(app.health().state, ApplicationState::Forced);
        }
        // Queued handlers may have been aborted by Forced; the physical job's
        // one admission/execution permit must still be owned until gate release.
        assert!(slots.admission.available_permits() <= 3);
        assert_eq!(slots.execution.available_permits(), 0);
        assert_eq!(probe.state.lock().unwrap().exited, 0);
        probe.release();
        for operation in waiting.drain(..) {
            assert_not_authenticated(operation.wait(Duration::from_secs(5)).await);
        }
        until(|| {
            slots.admission.available_permits() == 4
                && slots.execution.available_permits() == 1
                && probe.state.lock().unwrap().exited == 1
        })
        .await;
        until(|| app.health().pending.operations == 0).await;
        let receipt = app
            .shutdown_checked(ShutdownOptions::graceful())
            .await
            .unwrap();
        if force {
            assert!(
                matches!(receipt, ShutdownReceipt::Forced { pending } if pending.operations == 0)
            );
        } else {
            assert_eq!(receipt, ShutdownReceipt::GracefullyStopped);
        }
        assert_eq!(probe.state.lock().unwrap().peak, 1);
    }
}

#[tokio::test]
async fn supervised_auth_rebuild_waits_for_old_physical_password_and_fences_old_scheduler() {
    let (app, slots, probe) = probed_app(true).await;
    let _release_on_failure = ReleaseOnDrop(probe.clone());
    let hub = app.event_hub().external();
    let active = request(&hub, authenticate());
    until(|| probe.state.lock().unwrap().active == 1).await;
    let queued = request(&hub, authenticate());
    until(|| slots.admission.available_permits() == 2).await;
    let old = probe.schedulers.lock().unwrap()[0].clone();
    let fault = old
        .spawn(|_| async { panic!("controlled auth owner failure") })
        .unwrap();
    assert!(fault.wait_finished().await.is_err());
    until(|| {
        probe
            .state
            .lock()
            .unwrap()
            .cancel
            .as_ref()
            .unwrap()
            .is_cancelled()
    })
    .await;
    assert_eq!(app.health().state, ApplicationState::Recovering);
    assert_eq!(probe.schedulers.lock().unwrap().len(), 1);
    assert_eq!(probe.state.lock().unwrap().entered, 1);
    assert_eq!(slots.execution.available_permits(), 0);
    assert!(matches!(
        old.spawn(|_| async { Ok(()) }),
        Err(FrameworkError::Closed)
    ));
    probe.release();
    assert_not_authenticated(active.wait(Duration::from_secs(5)).await);
    // A request admitted before owner closure may drain nested CPU work under
    // its original operation lease. Recovery must wait for that actual exit.
    let drained = queued.wait(Duration::from_secs(5)).await;
    if let Ok(result) = drained {
        assert!(
            matches!(result.as_ref(), Ok(AuthDecision::Principal(p)) if p.policy_version == 1)
                || matches!(result.as_ref(), Err(AuthError::Unavailable))
        );
    }
    until(|| {
        app.health().ready
            && app.health().components[&ComponentRef::module("mqtt-auth")].generation == 1
    })
    .await;
    assert_eq!(probe.schedulers.lock().unwrap().len(), 2);
    let old_jobs = probe.state.lock().unwrap().entered;
    assert_eq!(probe.state.lock().unwrap().exited, old_jobs);
    assert!(matches!(
        request(&hub, authenticate())
            .wait(Duration::from_secs(5))
            .await
            .unwrap()
            .as_ref(),
        Ok(AuthDecision::Principal(_))
    ));
    until(|| slots.admission.available_permits() == 4 && slots.execution.available_permits() == 1)
        .await;
    {
        let state = probe.state.lock().unwrap();
        assert_eq!(
            (state.entered, state.exited, state.peak),
            (old_jobs + 1, old_jobs + 1, 1)
        );
    }
    assert!(matches!(
        old.spawn(|_| async { Ok(()) }),
        Err(FrameworkError::Closed)
    ));
    assert_eq!(
        app.shutdown_checked(ShutdownOptions::graceful())
            .await
            .unwrap(),
        ShutdownReceipt::GracefullyStopped
    );
}
