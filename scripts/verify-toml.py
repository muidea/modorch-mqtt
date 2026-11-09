#!/usr/bin/env python3
"""Exercise the TOML CLI using only local subprocesses and standard-library MQTT IO."""
from pathlib import Path
import os
import re
import selectors
import signal
import socket
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / os.environ.get("CARGO_TARGET_DIR", "target") / "debug/standalone-broker"
PROFILE = ROOT / "examples/standalone-broker/mqtt.toml"
SECRET = "must-not-appear-in-config-diagnostic"


def frame(header, body):
    assert len(body) < 128
    return bytes([header, len(body)]) + body


def string(value):
    data = value.encode()
    return len(data).to_bytes(2, "big") + data


def read_exact(peer, length):
    data = bytearray()
    while len(data) < length:
        chunk = peer.recv(length - len(data))
        assert chunk, "unexpected MQTT EOF"
        data.extend(chunk)
    return bytes(data)


def receive(peer):
    header, length = read_exact(peer, 2)
    assert length < 128
    return header, read_exact(peer, length)


def main():
    with tempfile.TemporaryDirectory(prefix="mqtt-toml-") as directory:
        config = Path(directory) / "mqtt.toml"
        valid = PROFILE.read_text().replace("127.0.0.1:1883", "127.0.0.1:0")
        cases = [
            ("[mqtt.core]\nenabled=false\n", True),
            (valid, True),
            (valid.replace("[mqtt.security]", "unsupported_limit=1\n[mqtt.security]"), False),
            (valid.replace('protocols = ["3.1.1"]', 'protocols = ["5.0"]'), False),
            (valid.replace("127.0.0.1:0", "0.0.0.0:0"), False),
            (valid.replace("anonymous_access = true", "anonymous_access = false"), False),
            (valid + f"\n[credentials]\npassword='{SECRET}'\n", False),
            (valid + f"\n[bad_section]\nsecret='{SECRET}", False),
            ("#" * 65537, False),
            (b"[mqtt.core]\n\xff", False),
        ]
        for content, success in cases:
            config.write_bytes(content if isinstance(content, bytes) else content.encode())
            result = subprocess.run([str(BINARY), "--config", str(config), "--once"],
                                    capture_output=True, timeout=10)
            output = (result.stdout + result.stderr).decode(errors="replace")
            assert (result.returncode == 0) == success, output
            assert SECRET not in output
            if not success:
                assert "listening on" not in output
        config.write_text(valid)
        for extra in [["--benchmark"], ["127.0.0.1:0"], ["--config", str(config)], ["--unknown"]]:
            result = subprocess.run([str(BINARY), "--config", str(config), *extra, "--once"],
                                    capture_output=True, timeout=10)
            assert result.returncode != 0 and b"listening on" not in result.stdout
        print("PASS: TOML enabled/disabled CLI, strict rejection, byte bound, UTF-8, redacted errors and unambiguous flags")
        process = subprocess.Popen([str(BINARY), "--config", str(config)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        port = None
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                assert selector.select(timeout=10), "TOML host startup deadline"
            line = process.stdout.readline()
            match = re.search(r"127\.0\.0\.1:(\d+)", line)
            assert match, line
            port = int(match.group(1))
            with socket.create_connection(("127.0.0.1", port), timeout=5) as peer:
                peer.settimeout(5)
                peer.sendall(frame(0x10, string("MQTT") + bytes([4, 2, 0, 0]) + string("toml-client")))
                assert receive(peer) == (0x20, b"\x00\x00")
                peer.sendall(frame(0x35, string("config/test") + b"\x00\x09binary\x00value"))
                assert receive(peer) == (0x50, b"\x00\x09")
                peer.sendall(frame(0x62, b"\x00\x09"))
                assert receive(peer) == (0x70, b"\x00\x09")
                peer.sendall(frame(0x82, b"\x00\x02" + string("config/+") + b"\x02"))
                assert receive(peer) == (0x90, b"\x00\x02\x02")
                header, message = receive(peer)
                assert header == 0x35
                assert message[:13] == string("config/test")
                packet_id = message[13:15]
                assert message[15:] == b"binary\x00value"
                peer.sendall(frame(0x50, packet_id))
                assert receive(peer) == (0x62, packet_id)
                peer.sendall(frame(0x70, packet_id))
                peer.sendall(b"\xc0\x00")
                assert receive(peer) == (0xd0, b"")
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=10)
            assert process.returncode == 0, stderr
            assert "MQTT checked shutdown complete" in stdout
            with socket.socket() as probe:
                probe.settimeout(1)
                assert probe.connect_ex(("127.0.0.1", port)) != 0
            print("PASS: TOML host QoS2 retained replay, complete bidirectional exchange, SIGINT checked shutdown and port release")
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)


if __name__ == "__main__":
    main()
