#!/usr/bin/env python3
"""Independent raw-wire audit probes; exit zero means defects were reproduced."""
import argparse
import hashlib
import json
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
        old, _ = connect(port, "active-zero")
        clients.append(old)
        subscribe(old, "audit/takeover")
        replacement, present = connect(port, "active-zero", clean=False, expiry=30)
        clients.append(replacement)
        writer, _ = connect(port, "writer")
        clients.append(writer)
        assert publish(writer, "audit/takeover") == (0x40, b"\x00\x09")
        replacement.sendall(b"\xc0\x00")
        next_frame = recv(replacement)
        assert not present and next_frame == (0xd0, b""), (present, next_frame)
        print("REPRODUCED R-01: active SessionExpiry=0, CleanStart=false takeover returns SessionPresent=0 and loses subscription")

        control, _ = connect(port, "control")
        clients.append(control)
        assert publish(control, "audit/properties", 128) == (0x40, b"\x00\x09")
        expanded, _ = connect(port, "expanded")
        clients.append(expanded)
        try:
            publish(expanded, "audit/properties", 130)
        except EOFError:
            print("REPRODUCED R-03 boundary: 128 User Properties accepted; 130 (after Bridge adds two) closes publisher without PUBACK")
        else:
            raise AssertionError("expected property expansion rejection")
    finally:
        for client in clients:
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-historical-source-check", action="store_true",
                        help="Use after audit documentation changes; still verify historical evidence")
    args = parser.parse_args()
    for filename in ("source-hashes.json", "evidence-hashes.json"):
        if filename == "source-hashes.json" and args.skip_historical_source_check:
            print("Historical source check explicitly skipped (audit documentation changed)")
            continue
        manifest = ROOT / "docs/validation/2026-11-08-functional" / filename
        files = json.loads(manifest.read_text())["files"]
        bad = [path for path, digest in files.items()
               if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest]
        assert not bad, bad
        print(f"Previous {filename}: {len(files)} hashes match")
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
