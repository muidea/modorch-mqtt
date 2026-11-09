#!/usr/bin/env python3
"""Independent Paho MQTT5 TCP/WebSocket verification; uses an existing isolated venv."""
import os
import queue
import re
import selectors
import signal
import subprocess
import tempfile
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.subscribeoptions import SubscribeOptions

ROOT = Path(__file__).resolve().parents[1]


def run(websocket):
    profile = (ROOT / "examples/standalone-broker/mqtt.toml").read_text()
    profile = profile.replace('protocols = ["3.1.1"]', 'protocols = ["3.1.1", "5.0"]')
    profile = profile.replace('127.0.0.1:1883', '127.0.0.1:0')
    if websocket:
        profile += '\n[mqtt.websocket]\nenabled=true\npath="/mqtt"\n'
    clients = []
    unsubscribed = queue.Queue()
    with tempfile.TemporaryDirectory(prefix="mqtt5-interop-") as directory:
        path = Path(directory) / "broker.toml"
        path.write_text(profile)
        process = subprocess.Popen(
            [str(ROOT / os.environ.get("CARGO_TARGET_DIR", "target") / "debug/standalone-broker"), "--config", str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                assert selector.select(timeout=5), "startup deadline"
            line = process.stdout.readline()
            match = re.search(r"127\.0\.0\.1:(\d+)", line)
            assert match, line
            port = int(match.group(1))

            def client(identity, *, will=False):
                ready, subscribed, received = queue.Queue(), queue.Queue(), queue.Queue()
                c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, identity,
                                protocol=mqtt.MQTTv5,
                                transport="websockets" if websocket else "tcp")
                if websocket:
                    c.ws_set_options(path="/mqtt")
                c.on_connect = lambda _c, _u, _f, reason, props: ready.put((reason, props))
                c.on_subscribe = lambda _c, _u, _id, reasons, _p: subscribed.put(reasons)
                c.on_message = lambda _c, _u, message: received.put(message)
                c.on_unsubscribe = lambda _c, _u, _id, reasons, _p: unsubscribed.put(reasons)
                if will:
                    c.will_set("paho5/will", b"nonzero-disconnect", qos=1)
                connect_props = Properties(PacketTypes.CONNECT)
                connect_props.SessionExpiryInterval = 30
                connect_props.ReceiveMaximum = 1
                c.connect("127.0.0.1", port, keepalive=2, properties=connect_props)
                clients.append(c)
                c.loop_start()
                reason, props = ready.get(timeout=5)
                assert reason == 0 and props.TopicAliasMaximum == 32
                return c, subscribed, received

            subscriber, subscriptions, messages = client("paho5-subscriber")
            props = Properties(PacketTypes.SUBSCRIBE)
            props.SubscriptionIdentifier = 19
            subscriber.subscribe("paho5/#", options=SubscribeOptions(qos=2, retainAsPublished=True),
                                 properties=props)
            assert subscriptions.get(timeout=5)[0] == 2
            publisher, _, _ = client("paho5-publisher")
            for qos in (0, 1, 2):
                props = Properties(PacketTypes.PUBLISH)
                props.UserProperty = [("kind", "interop")]
                props.ResponseTopic = "replies/one"
                props.CorrelationData = b"\x00\xff"
                props.MessageExpiryInterval = 30
                props.TopicAlias = 1
                sent = publisher.publish("paho5/value", bytes([qos]), qos=qos,
                                         retain=True, properties=props)
                sent.wait_for_publish(timeout=5)
                assert sent.is_published()
                message = messages.get(timeout=5)
                assert message.payload == bytes([qos]) and message.retain
                assert message.properties.SubscriptionIdentifier == [19]
                assert message.properties.UserProperty == [("kind", "interop")]
                assert message.properties.CorrelationData == b"\x00\xff"
                assert message.properties.ResponseTopic == "replies/one"
            owner, _, _ = client("paho5-will", will=True)
            owner.disconnect(reasoncode=ReasonCode(PacketTypes.DISCONNECT, "Unspecified error"))
            will = messages.get(timeout=5)
            assert will.topic == "paho5/will" and will.payload == b"nonzero-disconnect"
            subscriber.unsubscribe(["paho5/#", "paho5/#"])
            assert unsubscribed.get(timeout=5) == [0, 0x11]
            print(f"PASS: Paho MQTT5 {'WebSocket' if websocket else 'TCP'} QoS0/1/2, properties, alias, subscription identifier, nonzero DISCONNECT Will and sequential duplicate UNSUBSCRIBE")
        finally:
            for c in clients:
                c.disconnect()
                c.loop_stop()
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                _, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise AssertionError("checked shutdown deadline")
            assert process.returncode == 0, stderr


if __name__ == "__main__":
    run(False)
    run(True)
