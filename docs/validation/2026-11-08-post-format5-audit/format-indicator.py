#!/usr/bin/env python3
"""Independent raw-wire check; indicator zero omission is permitted by MQTT5 section 3.3.2.3.2."""
import hashlib
from pathlib import Path
import re
import selectors
import signal
import socket
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def varint(value):
    result = bytearray()
    while True:
        digit, value = value % 128, value // 128
        result.append(digit | (128 if value else 0))
        if not value:
            return bytes(result)


def string(value):
    data = value.encode()
    return len(data).to_bytes(2, "big") + data


def frame(header, body):
    return bytes([header]) + varint(len(body)) + body


def exact(client, count):
    result = b""
    while len(result) < count:
        data = client.recv(count - len(result))
        if not data:
            raise EOFError()
        result += data
    return result


def recv(client):
    header = exact(client, 1)[0]
    size, shift = 0, 0
    while True:
        digit = exact(client, 1)[0]
        size |= (digit & 127) << shift
        if not digit & 128:
            break
        shift += 7
    return header, exact(client, size)


def connect(port, identity, clean=True, expiry=0):
    client = socket.create_connection(("127.0.0.1", port), timeout=2)
    properties = b"\x11" + expiry.to_bytes(4, "big")
    body = string("MQTT") + bytes([5, 2 if clean else 0, 0, 0])
    body += varint(len(properties)) + properties + string(identity)
    client.sendall(frame(0x10, body))
    header, ack = recv(client)
    assert header == 0x20 and ack[1] == 0, (header, ack)
    return client, bool(ack[0])


def subscribe(client, topic):
    client.sendall(frame(0x82, b"\x00\x01\x00" + string(topic) + b"\x01"))
    assert recv(client) == (0x90, b"\x00\x01\x00\x01")


def publish(client, topic, count=0):
    properties = (b"\x26" + string("k") + string("v")) * count
    client.sendall(frame(0x32, string(topic) + b"\x00\x09" + varint(len(properties)) + properties + b"payload"))
    return recv(client)


def run(port):
    clients = []
    try:
        sink, _ = connect(port, "format-sink")
        clients.append(sink)
        sink.sendall(frame(0x82, b"\x00\x01\x00" + string("audit/format") + b"\x00"))
        assert recv(sink) == (0x90, b"\x00\x01\x00\x00")
        writer, _ = connect(port, "format-source")
        clients.append(writer)
        for indicator in [None, 1, 0]:
            props = b"" if indicator is None else bytes([1, indicator])
            body = string("audit/format") + b"\x00\x09" + varint(len(props)) + props + b"payload"
            writer.sendall(frame(0x32, body))
            assert recv(writer) == (0x40, b"\x00\x09")
            header, body = recv(sink)
            assert header == 0x30
            topic_bytes = int.from_bytes(body[:2], "big")
            offset = 2 + topic_bytes
            size = body[offset]
            outbound = body[offset + 1:offset + 1 + size]
            assert body[offset + 1 + size:] == b"payload"
            if indicator == 0:
                assert outbound == b"", outbound
                print("RULED OUT: indicator=0 is omitted; MQTT5 section 3.3.2.3.2 explicitly permits this equivalent representation")
            else:
                assert outbound == props, (indicator, outbound)
                print(f"CONTROL: indicator={indicator}, outgoing properties={outbound.hex()}")
        sink.sendall(b"\xc0\x00")
        assert recv(sink) == (0xd0, b"")
    finally:
        for client in clients:
            client.close()


def main():
    profile = (ROOT / "examples/standalone-broker/mqtt.toml").read_text()
    profile = profile.replace('protocols = ["3.1.1"]', 'protocols = ["3.1.1", "5.0"]')
    profile = profile.replace("127.0.0.1:1883", "127.0.0.1:0")
    with tempfile.TemporaryDirectory(prefix="mqtt-remaining-audit-") as directory:
        config = Path(directory) / "mqtt.toml"
        config.write_text(profile)
        process = subprocess.Popen(
            [str(ROOT / "target/debug/standalone-broker"), "--config", str(config)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                assert selector.select(timeout=5), "startup deadline"
            line = process.stdout.readline()
            match = re.search(r"127\.0\.0\.1:(\d+)", line)
            assert match, line
            print("Binary sha256:", hashlib.sha256((ROOT / "target/debug/standalone-broker").read_bytes()).hexdigest())
            run(int(match[1]))
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                _, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise
            assert process.returncode == 0, stderr
            print("Broker checked shutdown: exit 0")


if __name__ == "__main__":
    main()
