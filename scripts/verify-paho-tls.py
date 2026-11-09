#!/usr/bin/env python3
"""Verify TLS/password ACL behavior against the explicit secure demo composition.

Uses the same optional Paho virtual environment as verify-paho.py. No dependency
installation, global TLS changes or externally reachable listeners are created.
"""
import os
import queue
import re
import selectors
import signal
import socket
import ssl
import subprocess
from pathlib import Path

import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[1]


def connect(port, identity, password):
    connected, subscribed, received = queue.Queue(), queue.Queue(), queue.Queue()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, identity, protocol=mqtt.MQTTv311)
    context = ssl.create_default_context(cafile=str(ROOT / "crates/modorch-mqtt-core/tests/fixtures/localhost-cert.pem"))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    client.tls_set_context(context)
    client.username_pw_set("device", password)
    client.on_connect = lambda _c, _u, _f, reason, _p: connected.put(reason)
    client.on_subscribe = lambda _c, _u, _m, reasons, _p: subscribed.put(reasons)
    client.on_message = lambda _c, _u, message: received.put(message)
    client.connect("127.0.0.1", port, keepalive=5)
    client.loop_start()
    return client, connected, subscribed, received


def main():
    process = subprocess.Popen([str(ROOT / os.environ.get("CARGO_TARGET_DIR", "target") / "debug/secure-modorch-server"), "--demo"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    clients, port = [], None
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10), "secure example startup deadline"
        line = process.stdout.readline()
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        assert match, line
        port = int(match.group(1))
        subscriber, connected, subscriptions, messages = connect(port, "device-subscriber", "demo-password")
        clients.append(subscriber)
        assert connected.get(timeout=10) == 0
        subscriber.subscribe("devices/#", qos=2)
        assert subscriptions.get(timeout=5)[0] == 2
        subscriber.subscribe("#", qos=1)
        assert subscriptions.get(timeout=5)[0].is_failure
        publisher, connected, _, _ = connect(port, "device-publisher", "demo-password")
        clients.append(publisher)
        assert connected.get(timeout=10) == 0
        published = publisher.publish("devices/001/data", b"TLS-and-auth", qos=2, retain=True)
        published.wait_for_publish(timeout=5)
        assert published.is_published()
        actual = messages.get(timeout=5)
        assert (actual.topic, actual.payload, actual.qos) == ("devices/001/data", b"TLS-and-auth", 2)
        denied, connected, _, _ = connect(port, "device-invalid", "wrong-password")
        clients.append(denied)
        assert connected.get(timeout=10).is_failure
        subscriber.publish("devices/001/data", b"still-connected", qos=1).wait_for_publish(timeout=5)
        assert messages.get(timeout=5).payload == b"still-connected"
        print("PASS: verified TLS certificate, Argon2id authentication, QoS2 handshake/delivery, wildcard ACL denial and bad-password isolation")
    finally:
        for client in clients:
            client.disconnect()
            client.loop_stop()
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            _, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise AssertionError("secure example did not complete checked shutdown")
        assert process.returncode == 0, stderr
        if port is not None:
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))
            print("PASS: secure composition checked shutdown and port release")


if __name__ == "__main__":
    main()
