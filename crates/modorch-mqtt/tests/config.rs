#![cfg(feature = "toml")]
use modorch::{ApplicationBuilder, ComponentSelection, ShutdownOptions};
use modorch_mqtt::*;
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const ENABLED: &str = "[mqtt.core]\nenabled = true\nlistener = '127.0.0.1:0'\n[mqtt.security]\nanonymous_access = true\n";
fn parse(input: &str) -> Result<MqttTomlConfig, MqttError> {
    MqttTomlConfig::parse(input, "/host/config", None)
}

#[tokio::test]
async fn configuration_and_registration_never_enable_components_implicitly() {
    let mut builder = ApplicationBuilder::new("disabled-config");
    let disabled = parse("[mqtt.core]").unwrap();
    assert!(disabled.core_config().is_none());
    assert_eq!(disabled.module_ids().count(), 0);
    assert!(disabled.register(&mut builder).unwrap().is_none());
    let registered = parse(ENABLED)
        .unwrap()
        .register(&mut builder)
        .unwrap()
        .unwrap();
    assert_eq!(registered.module_ids().collect::<Vec<_>>(), ["mqtt-broker"]);
    assert!(registered.handle().local_addr().is_none());
    let app = builder.build().unwrap().start().await.unwrap();
    assert!(!registered.handle().status().active);
    assert!(registered.handle().local_addr().is_none());
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
}

#[tokio::test]
async fn enabled_toml_profile_uses_host_hub_and_real_tcp_qos1_then_checked_shutdown() {
    let input = include_str!("../../../examples/standalone-broker/mqtt.toml")
        .replace("127.0.0.1:1883", "127.0.0.1:0");
    let mut builder = ApplicationBuilder::new("toml-host");
    let registered = parse(&input)
        .unwrap()
        .register(&mut builder)
        .unwrap()
        .unwrap();
    assert_eq!(registered.handle().status().generation, 0);
    let mut selected = ComponentSelection::new();
    for id in registered.module_ids() {
        selected.enable_module(id);
    }
    let app = builder
        .selection(selected)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    let address = registered.handle().local_addr().unwrap();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[
            0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 1, b'x',
        ])
        .await
        .unwrap();
    let mut response = [0; 4];
    socket.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x20, 2, 0, 0]);
    // A retained QoS1 publish, independently encoded on the MQTT wire.
    socket
        .write_all(&[0x33, 9, 0, 4, b'c', b'f', b'g', b'1', 0, 7, b'v'])
        .await
        .unwrap();
    socket.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x40, 2, 0, 7]);
    assert_eq!(registered.handle().status().retained, 1);
    let status = app
        .event_hub()
        .external()
        .request::<_, BrokerStatus>(
            STATUS_TOPIC,
            modorch::Destination::Component(modorch::ComponentRef::module("mqtt-broker")),
            (),
            modorch::MessageOptions::default(),
        )
        .unwrap()
        .wait(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(status.retained, 1);
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    assert_eq!(registered.handle().status().connections, 0);
    assert!(TcpStream::connect(address).await.is_err());
}

#[test]
fn strict_schema_rejects_unknown_unsupported_ambiguous_and_unsafe_settings() {
    let core_cases = [
        "unknown_limit = 7",
        "protocols = ['5.0']",
        "protocols = []",
        "protocols = ['3.1.1', '3.1.1']",
        "connection_idle_ms = 1",
        "reliable_overflow_policy = 'drop_oldest'",
        "max_connections = -1",
        "max_connections = 0",
        "max_sessions = 1",
        "max_packet_bytes = 127",
        "max_client_id_bytes = 22",
        "max_inflight_per_client = 65536",
        "max_inbound_qos2_per_client = 65536",
        "handshake_timeout_ms = 0",
        "write_timeout_ms = 86400001",
        "connection_attempt_window_ms = 3600001",
        "memory_budget_bytes = 1024",
        "control_subscription_ids = [9, 9, 10]",
        "handshake_timeout_ms = 18446744073709551615",
        "max_connections = 99999999999999999999999999999999999",
    ];
    for setting in core_cases {
        let input = ENABLED.replace("[mqtt.security]", &format!("{setting}\n[mqtt.security]"));
        assert!(parse(&input).is_err(), "accepted {setting}");
    }
    for suffix in [
        "bad_key = true",
        "[unexpected]\na=1",
        "[mqtt.websocket]\nenabled = true\npath='invalid'",
        "[mqtt.bridge]\nenabled = true",
        "[mqtt.tls]\nprivate_key = 'unused'",
        "[mqtt.auth]\ncomponent = 'unused'",
        "[mqtt.auth]\nenabled=true",
        "[mqtt.auth]\nenabled=true\ncomponent='mqtt-broker'",
        "[mqtt.auth]\nenabled=true\ncomponent='auth'", // anonymous/Auth ambiguity
        "[mqtt.storage]\nenabled=true\nmode='memory'",
        "[mqtt.storage]\nmode='sqlite'",
        "[mqtt.storage]\ndisk_budget_bytes=1234",
        "[mqtt.storage]\naccess='not-allowed'",
        "[mqtt.admin]\nenabled=true",
        "[mqtt.admin]\ncontrol_subscription_id=4",
        "[mqtt.metrics]\ncomponent='unused'",
        "[mqtt.metrics]\nenabled=true\ncomponent='mqtt-broker'",
        "[mqtt.metrics]\nenabled=true\ncomponent='bad/name'",
    ] {
        assert!(
            parse(&format!("{ENABLED}{suffix}\n")).is_err(),
            "accepted {suffix}"
        );
    }
    for setting in [
        "allow_public_listener_without_tls = true",
        "reserved_internal_prefix = '$modorch/mqtt/'",
        "session_owner_policy = 'client_id'",
    ] {
        assert!(parse(&format!("{ENABLED}{setting}\n")).is_err());
    }
    for setting in [
        "inbound_mode = 'reliable'",
        "inbound_topic_mode = 'rewritten'",
        "control_prefix = 'custom/'",
        "allow_implicit_outbound = true",
        "typo = 1",
    ] {
        assert!(parse(&format!("{ENABLED}[mqtt.eventhub]\n{setting}\n")).is_err());
    }
    assert!(parse(&ENABLED.replace("127.0.0.1:0", "0.0.0.0:0")).is_err());
    assert!(parse("[mqtt.core]\nenabled=true").is_err());
    assert!(parse("[mqtt.core]\n[mqtt.metrics]\nenabled=true\ncomponent='metrics'").is_err());
}

#[test]
fn every_core_limit_and_millisecond_duration_is_mapped_and_defaults_match_rust() {
    let input = r#"
[mqtt.core]
enabled=true
name='configured'
listener='[::1]:1889'
control_subscription_ids=[11,12,13]
max_connections=3
max_connections_per_ip=2
max_connection_attempts_per_ip=17
max_tracked_peer_ips=19
max_sessions=23
max_packet_bytes=4096
max_client_id_bytes=37
handshake_timeout_ms=1234
write_timeout_ms=2345
connection_attempt_window_ms=3456
max_subscriptions_per_client=29
per_client_outbound_queue=31
max_inflight_per_client=7
max_offline_messages_per_client=41
max_pending_bytes_per_client=65537
max_inbound_qos2_per_client=43
max_inbound_qos2_bytes_per_client=65539
max_retained_topics=47
max_will_records=53
max_publish_records=59
memory_budget_bytes=33554432
[mqtt.security]
anonymous_access=true
"#;
    let c = parse(input).unwrap().into_core_config().unwrap();
    assert_eq!(c.name, "configured");
    assert_eq!(c.listener.to_string(), "[::1]:1889");
    assert_eq!(c.control_subscription_ids, [11, 12, 13]);
    assert_eq!(
        [
            c.max_connections,
            c.max_connections_per_ip,
            c.max_connection_attempts_per_ip,
            c.max_tracked_peer_ips,
            c.max_sessions
        ],
        [3, 2, 17, 19, 23]
    );
    assert_eq!(
        [
            c.max_packet_bytes,
            c.max_client_id_bytes,
            c.max_subscriptions_per_client,
            c.per_client_outbound_queue,
            c.max_inflight_per_client,
            c.max_offline_messages_per_client
        ],
        [4096, 37, 29, 31, 7, 41]
    );
    assert_eq!(
        [
            c.max_pending_bytes_per_client,
            c.max_inbound_qos2_per_client,
            c.max_inbound_qos2_bytes_per_client,
            c.max_retained_topics,
            c.max_will_records,
            c.max_publish_records,
            c.memory_budget_bytes
        ],
        [65537, 43, 65539, 47, 53, 59, 33554432]
    );
    assert_eq!(
        [
            c.handshake_timeout,
            c.write_timeout,
            c.connection_attempt_window
        ],
        [
            Duration::from_millis(1234),
            Duration::from_millis(2345),
            Duration::from_millis(3456)
        ]
    );
    let defaults = parse("[mqtt.core]\nenabled=true\n[mqtt.security]\nanonymous_access=true")
        .unwrap()
        .into_core_config()
        .unwrap();
    assert_eq!(
        format!("{defaults:?}"),
        format!("{:?}", MqttBrokerConfig::loopback_development())
    );
}

#[test]
fn parser_errors_debug_and_size_limit_do_not_echo_source_or_secrets() {
    let secret = "do-not-echo-private-material";
    for input in [
        format!("{ENABLED}password = '{secret}'"),
        format!("{ENABLED}[mqtt.auth]\nenabled=true\ncomponent = '{secret}"),
        format!("{ENABLED}[mqtt.metrics]\nenabled=true\ncomponent='{secret}/bad'"),
        format!("{ENABLED}{}", "#".repeat(MAX_MQTT_TOML_BYTES)),
        "[mqtt.core]\nname = '中文'\nunknown = '秘密'".into(),
    ] {
        let error = parse(&input).unwrap_err();
        assert!(!format!("{error:?} {error}").contains(secret));
        assert!(!format!("{error:?} {error}").contains("秘密"));
    }
    let mut boundary = ENABLED.to_string();
    boundary.push('#');
    boundary.extend(std::iter::repeat_n(
        'x',
        MAX_MQTT_TOML_BYTES - boundary.len(),
    ));
    assert!(parse(&boundary).is_ok());
    boundary.push('x');
    assert!(parse(&boundary).is_err());
    assert!(parse("").is_err());
    assert!(parse(&format!("{ENABLED}[mqtt.core]")).is_err());
}

const EXTERNAL: &str = r#"
[mqtt.core]
enabled=true
listener='127.0.0.1:0'
[mqtt.auth]
enabled=true
component='auth-ref'
timeout_ms=1234
[mqtt.storage]
enabled=true
mode='external'
component='store-ref'
max_snapshot_bytes=1048576
recovery_page_bytes=4096
timeout_ms=2345
recovery_timeout_ms=12345
[mqtt.admin]
enabled=true
component='admin-ref'
control_subscription_id=4
[mqtt.metrics]
enabled=true
component='metrics-ref'
"#;
#[test]
fn provider_references_require_host_capability_and_do_not_register_factories() {
    assert!(parse(EXTERNAL).is_err());
    assert!(MqttTomlConfig::parse(EXTERNAL, ".", Some(StoreAccessKey([0; 32]))).is_err());
    let plan = MqttTomlConfig::parse(EXTERNAL, ".", Some(StoreAccessKey([71; 32]))).unwrap();
    assert_eq!(
        plan.module_ids().collect::<Vec<_>>(),
        [
            "mqtt-broker",
            "auth-ref",
            "store-ref",
            "admin-ref",
            "metrics-ref"
        ]
    );
    let c = plan.core_config().unwrap();
    assert_eq!(
        c.auth.as_ref().unwrap().timeout,
        Duration::from_millis(1234)
    );
    let s = c.storage.as_ref().unwrap();
    assert_eq!(s.timeout, Duration::from_millis(2345));
    assert_eq!(s.recovery_timeout, Duration::from_millis(12345));
    assert_eq!(s.access.0, [71; 32]);
    assert_eq!(
        c.admin.as_ref().unwrap().allowed_sources,
        [modorch::ComponentRef::module("admin-ref")]
    );
    let mut builder = ApplicationBuilder::new("missing-provider");
    let mqtt = plan.register(&mut builder).unwrap().unwrap();
    let mut selection = ComponentSelection::new();
    for id in mqtt.module_ids() {
        selection.enable_module(id);
    }
    assert!(builder.selection(selection).build().is_err());
    assert!(mqtt.handle().local_addr().is_none());
    for input in [
        EXTERNAL.replace("store-ref", "auth-ref"),
        EXTERNAL.replace("control_subscription_id=4", "control_subscription_id=1"),
        EXTERNAL.replace("recovery_page_bytes=4096", "recovery_page_bytes=1048577"),
    ] {
        assert!(MqttTomlConfig::parse(&input, ".", Some(StoreAccessKey([71; 32]))).is_err());
    }
}

#[test]
fn tls_paths_use_explicit_base_without_reading_files_and_require_feature() {
    let input = format!(
        "{ENABLED}[mqtt.tls]\nenabled=true\ncertificate_chain='cert.pem'\nprivate_key='/absolute/key.pem'"
    );
    let result = parse(&input);
    if cfg!(feature = "tls") {
        let c = result.unwrap().into_core_config().unwrap();
        let tls = c.tls.unwrap();
        assert_eq!(tls.certificate_chain, Path::new("/host/config/cert.pem"));
        assert_eq!(tls.private_key, Path::new("/absolute/key.pem"));
    } else {
        assert!(matches!(result, Err(MqttError::Configuration(_))));
    }
    for settings in [
        "",
        "certificate_chain=''\nprivate_key='key'",
        "certificate_chain='cert'\nprivate_key=''",
        "certificate_chain='cert'",
    ] {
        assert!(parse(&format!("{ENABLED}[mqtt.tls]\nenabled=true\n{settings}")).is_err());
    }
}

#[cfg(all(
    feature = "auth",
    feature = "storage",
    feature = "admin",
    feature = "metrics",
    feature = "tls"
))]
#[tokio::test]
async fn explicit_host_factories_match_toml_refs_tls_and_prelisten_storage_recovery() {
    let path = std::env::temp_dir().join(format!("mqtt-toml-host-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../modorch-mqtt-core/tests/fixtures");
    let input = format!(
        "{EXTERNAL}[mqtt.tls]\nenabled=true\ncertificate_chain='localhost-cert.pem'\nprivate_key='localhost-key.pem'"
    );
    let access = StoreAccessKey([71; 32]);
    let plan = MqttTomlConfig::parse(&input, &fixtures, Some(access.clone())).unwrap();
    let mut builder = ApplicationBuilder::new("complete-toml-host");
    builder
        .register_module(
            PasswordAuthFactory::new(PasswordAuthConfig {
                name: "auth-ref".into(),
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
    builder
        .register_module(
            SqliteStorageFactory::new(SqliteStorageConfig {
                name: "store-ref".into(),
                broker: "mqtt-broker".into(),
                subscription_id: 3000,
                access,
                database: path.clone(),
                max_snapshot_bytes: 1048576,
                disk_budget_bytes: 12 * 1024 * 1024,
            })
            .unwrap(),
        )
        .unwrap();
    builder
        .register_module(
            AdminFactory::new(AdminConfig {
                name: "admin-ref".into(),
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
    let metrics = MetricsFactory::new(MetricsConfig {
        name: "metrics-ref".into(),
        interval: Duration::from_millis(100),
        timeout: Duration::from_millis(80),
        ..Default::default()
    })
    .unwrap();
    let observations = metrics.handle();
    builder.register_module(metrics).unwrap();
    let mqtt = plan.register(&mut builder).unwrap().unwrap();
    assert!(!path.exists());
    assert!(mqtt.handle().local_addr().is_none());
    let mut selection = ComponentSelection::new();
    for id in mqtt.module_ids() {
        selection.enable_module(id);
    }
    let app = builder
        .selection(selection)
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(path.exists());
    assert!(mqtt.handle().status().active);
    tokio::time::timeout(Duration::from_secs(2), async {
        while observations.snapshot().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    app.shutdown_checked(ShutdownOptions::graceful())
        .await
        .unwrap();
    drop((app, mqtt));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn optional_network_features_require_compilation_and_explicit_configuration() {
    let v5 = ENABLED.replace(
        "[mqtt.security]",
        "protocols=['3.1.1','5.0']\n[mqtt.security]",
    );
    let parsed = parse(&v5);
    assert_eq!(parsed.is_ok(), cfg!(feature = "mqtt5"));
    if let Ok(parsed) = parsed {
        assert!(parsed.core_config().unwrap().mqtt5);
    }
    let ws = format!("{ENABLED}[mqtt.websocket]\nenabled=true\npath='/mqtt'\n");
    let parsed = parse(&ws);
    assert_eq!(parsed.is_ok(), cfg!(feature = "websocket"));
    if let Ok(parsed) = parsed {
        assert_eq!(
            parsed
                .core_config()
                .unwrap()
                .websocket
                .as_ref()
                .unwrap()
                .path,
            "/mqtt"
        );
    }
    assert!(!parse(ENABLED).unwrap().core_config().unwrap().mqtt5);
    assert!(
        parse(ENABLED)
            .unwrap()
            .core_config()
            .unwrap()
            .websocket
            .is_none()
    );
    assert!(
        parse(&format!(
            "{ENABLED}[mqtt.websocket]\nenabled=false\npath='/mqtt'\n"
        ))
        .is_err()
    );
}
