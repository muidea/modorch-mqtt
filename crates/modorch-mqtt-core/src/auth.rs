use crate::{codec::Connect, config::MqttBrokerConfig, connection::ConnectionLifetime};
use modorch::{
    CancellationToken, ComponentRef, Destination, EventHub, ExternalEventHub, MessageOptions,
};
use modorch_mqtt_api::*;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Notify, time::Instant};

enum Hub<'a> {
    External(&'a ExternalEventHub),
    Internal(&'a EventHub),
}

async fn call(
    hub: Hub<'_>,
    config: &MqttBrokerConfig,
    request: AuthRequest,
) -> Result<AuthDecision, MqttError> {
    let provider = config.auth.as_ref().ok_or(MqttError::NotAuthorized)?;
    let destination = Destination::Component(ComponentRef::module(&provider.component));
    let cost = match &request {
        AuthRequest::Authenticate(a) => {
            512 + a.client_id.len()
                + a.username.as_ref().map_or(0, String::len)
                + a.password.as_ref().map_or(0, |v| v.len())
        }
        AuthRequest::Subscribe(a) => 1024 + a.filter.len(),
        AuthRequest::Publish(a) => {
            1024 + a.topic.len() + a.internal_source.as_ref().map_or(0, String::len)
        }
        AuthRequest::SessionAttach(a) => 2048 + a.client_id.len(),
    };
    let options = MessageOptions {
        payload_bytes: Some(cost),
        ..Default::default()
    };
    let operation = match hub {
        Hub::External(hub) => {
            hub.request::<_, AuthResult>(AUTH_TOPIC, destination, request, options)
        }
        Hub::Internal(hub) => {
            hub.request::<_, AuthResult>(AUTH_TOPIC, destination, request, options)
        }
    }
    .map_err(|_| MqttError::NotAuthorized)?;
    operation
        .wait(provider.timeout)
        .await
        .map_err(|_| MqttError::NotAuthorized)?
        .as_ref()
        .clone()
        .map_err(|_| MqttError::NotAuthorized)
}

pub(crate) async fn attach(
    life: &ConnectionLifetime,
    hub: &ExternalEventHub,
    mut connect: Connect,
    cancel: &CancellationToken,
    deadline: Instant,
    peer_ip: std::net::IpAddr,
) -> Result<(String, u64, bool, Arc<Notify>, Principal), MqttError> {
    let config = life.state.lock().unwrap().config.clone();
    if connect.client_id.len() > config.max_client_id_bytes
        || (connect.client_id.is_empty() && !connect.clean && connect.v5.is_none())
    {
        return Err(MqttError::Protocol("invalid ClientID"));
    }
    let assigned = connect.client_id.is_empty();
    if assigned {
        connect.client_id = life.state.lock().unwrap().assign_client_id()?;
    }
    let authorize = async {
        let principal = if config.auth.is_some() {
            let request = Authenticate {
                client_id: connect.client_id.clone(),
                username: connect.username.take(),
                password: connect.password.take(),
                peer_ip,
            };
            match call(
                Hub::External(hub),
                &config,
                AuthRequest::Authenticate(request),
            )
            .await?
            {
                AuthDecision::Principal(principal)
                    if principal.is_valid() && !principal.key.domain.starts_with("$anonymous/") =>
                {
                    principal
                }
                _ => return Err(MqttError::NotAuthorized),
            }
        } else {
            if connect.username.is_some() || connect.password.is_some() {
                return Err(MqttError::NotAuthorized);
            }
            Principal {
                key: PrincipalKey {
                    domain: format!("$anonymous/{}", config.name),
                    subject: "anonymous".into(),
                },
                policy_version: 0,
                expires_at_ms: u64::MAX,
            }
        };
        let (view, subscriptions, _snapshot_charge) =
            life.state
                .lock()
                .unwrap()
                .auth_view(&principal, &connect.client_id, connect.clean)?;
        if assigned && view.existing_owner.is_some() {
            return Err(MqttError::SessionConflict);
        }
        if view
            .existing_owner
            .as_ref()
            .is_some_and(|owner| owner != &principal.key)
        {
            return Err(MqttError::NotAuthorized);
        }
        let mut will_grant = None;
        if config.auth.is_some() {
            if !matches!(
                call(
                    Hub::External(hub),
                    &config,
                    AuthRequest::SessionAttach(view.clone())
                )
                .await?,
                AuthDecision::Allowed
            ) {
                return Err(MqttError::NotAuthorized);
            }
            for filter in subscriptions {
                let filter = acl_filter(&filter, connect.v5.is_some());
                subscribe(hub, &config, &principal, filter).await?;
            }
            if let Some(will) = &connect.will {
                will_grant = publish(hub, &config, &principal, will, PublishKind::Will).await?;
            }
        }
        Ok((principal, view.expected_version, will_grant))
    };
    let (principal, expected_version, will_grant) = tokio::select! { biased;
        _ = cancel.cancelled() => return Err(MqttError::Closed),
        _ = tokio::time::sleep_until(deadline) => return Err(MqttError::NotAuthorized),
        result = authorize => result?,
    };
    // Waiting for the gate is cancel-safe; after acquisition even a failed
    // attach must commit deferred detaches. Never drop an in-progress commit.
    let transaction = tokio::select! { biased;
        _ = cancel.cancelled() => return Err(MqttError::Closed),
        _ = tokio::time::sleep_until(deadline) => return Err(MqttError::NotAuthorized),
        result = life.persistence.begin(&life.state) => result?,
    };
    let attached = life.state.lock().unwrap().attach(
        life.generation,
        &connect,
        cancel.clone(),
        &principal,
        expected_version,
        will_grant,
    );
    transaction
        .commit(crate::persistence::Hub::External(hub))
        .await?;
    let attached = attached?;
    Ok((attached.0, attached.1, attached.2, attached.3, principal))
}

pub(crate) async fn subscribe(
    hub: &ExternalEventHub,
    config: &MqttBrokerConfig,
    principal: &Principal,
    filter: String,
) -> Result<(), MqttError> {
    if !principal.is_valid() {
        return Err(MqttError::NotAuthorized);
    }
    if config.auth.is_none() {
        return Ok(());
    }
    match call(
        Hub::External(hub),
        config,
        AuthRequest::Subscribe(AuthorizeSubscribe {
            principal: principal.clone(),
            filter,
        }),
    )
    .await?
    {
        AuthDecision::Allowed if principal.is_valid() => Ok(()),
        _ => Err(MqttError::NotAuthorized),
    }
}

pub(crate) async fn publish(
    hub: &ExternalEventHub,
    config: &MqttBrokerConfig,
    principal: &Principal,
    message: &MqttMessage,
    kind: PublishKind,
) -> Result<Option<PublishGrant>, MqttError> {
    if !principal.is_valid() {
        return Err(MqttError::NotAuthorized);
    }
    if config.auth.is_none() {
        return Ok(None);
    }
    let context = PublishAuthContext {
        principal: Some(principal.clone()),
        internal_source: None,
        topic: message.topic.clone(),
        qos: message.qos,
        retain: message.retain,
        kind,
    };
    match call(
        Hub::External(hub),
        config,
        AuthRequest::Publish(context.clone()),
    )
    .await?
    {
        AuthDecision::Publish(grant)
            if grant.context == context
                && grant.policy_version == principal.policy_version
                && principal.is_valid() =>
        {
            Ok(Some(grant))
        }
        _ => Err(MqttError::NotAuthorized),
    }
}

pub(crate) async fn authorize_internal(
    hub: &EventHub,
    config: &MqttBrokerConfig,
    source: &ComponentRef,
    request: &MqttPublishRequest,
) -> Result<(), MqttError> {
    if config.auth.is_none() {
        return Ok(());
    }
    let context = PublishAuthContext {
        principal: None,
        internal_source: Some(source.to_string()),
        topic: request.topic.clone(),
        qos: request.qos,
        retain: request.retain,
        kind: PublishKind::InternalPublish,
    };
    match call(
        Hub::Internal(hub),
        config,
        AuthRequest::Publish(context.clone()),
    )
    .await?
    {
        AuthDecision::Publish(grant) if grant.context == context => Ok(()),
        _ => Err(MqttError::NotAuthorized),
    }
}

pub(crate) fn expiry(principal: &Principal) -> Duration {
    Duration::from_millis(
        principal
            .expires_at_ms
            .saturating_sub(unix_time_ms())
            .min(86400000),
    )
}

/// Convert the actual expiry, without turning long-lived identities into a
/// one-day session limit. The bounded `expiry` helper is for provider waits.
pub(crate) fn deadline(principal: &Principal) -> Option<Instant> {
    if principal.expires_at_ms == u64::MAX {
        return None;
    }
    Instant::now().checked_add(Duration::from_millis(
        principal.expires_at_ms.saturating_sub(unix_time_ms()),
    ))
}

pub(crate) fn acl_filter(filter: &str, v5: bool) -> String {
    #[cfg(feature = "mqtt5")]
    if v5 {
        if let Some((_, f)) = crate::mqtt5::shared_filter(filter) {
            return f.to_owned();
        }
    }
    let _ = v5;
    filter.to_owned()
}
