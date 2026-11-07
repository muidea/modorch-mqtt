#!/usr/bin/env python3
"""Run an independent Paho 2.1.0 client against the built example process.

Install the optional test dependency in a virtual environment, build the example,
then run this script. This script does not install anything or modify global state.
"""
import queue
import re
import selectors
import signal
import socket
import subprocess
from pathlib import Path

import paho.mqtt.client as mqtt


ROOT = Path(__file__).resolve().parents[1]


def client(port, identity):
    connected = queue.Queue()
    subscribed = queue.Queue()
    received = queue.Queue()
    instance = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, identity, protocol=mqtt.MQTTv311)
    instance.on_connect = lambda _c, _u, _f, reason, _p: connected.put(reason)
    instance.on_subscribe = lambda _c, _u, _m, reasons, _p: subscribed.put(reasons)
    instance.on_message = lambda _c, _u, message: received.put(message)
    instance.connect("127.0.0.1", port, keepalive=2)
    instance.loop_start()
    assert connected.get(timeout=5) == 0
    return instance, subscribed, received


def main():
    process = subprocess.Popen(
        [str(ROOT / "target/debug/standalone-broker"), "127.0.0.1:0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    clients = []
    port = None
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), "broker startup deadline"
        line = process.stdout.readline()
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        assert match, line
        port = int(match.group(1))
        subscriber, subscribed, messages = client(port, "paho-subscriber")
        clients.append(subscriber)
        subscriber.subscribe("paho/+", qos=1)
        assert subscribed.get(timeout=5)[0] == 1
        publisher, _, _ = client(port, "paho-publisher")
        clients.append(publisher)
        for qos, payload in [(0, b"zero"), (1, b"one")]:
            published = publisher.publish("paho/test", payload, qos=qos, retain=qos == 1)
            published.wait_for_publish(timeout=5)
            assert published.is_published()
            message = messages.get(timeout=5)
            assert (message.topic, message.payload, message.qos, message.retain) == (
                "paho/test", payload, qos, False,
            )
        late, subscriptions, replay = client(port, "paho-late")
        clients.append(late)
        late.subscribe("paho/+", qos=1)
        subscriptions.get(timeout=5)
        retained = replay.get(timeout=5)
        assert (retained.payload, retained.retain) == (b"one", True)
        print("PASS: Paho QoS0/QoS1 live delivery, PUBACK and retained replay")
    finally:
        for instance in clients:
            instance.disconnect()
            instance.loop_stop()
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            _, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise AssertionError("broker did not complete checked shutdown")
        assert process.returncode == 0, stderr
        if port is not None:
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))
            print("PASS: SIGINT checked shutdown and port release")


if __name__ == "__main__":
    main()
