#!/usr/bin/env python3
"""Exercise real process crashes and SQLite recovery using Python's standard library.

Build durable-broker first. Every broker and database belongs to this temporary
verification run; no dependencies are installed and no user database is opened.
"""
import os
import selectors
import signal
import socket
import struct
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def field(value):
    data = value.encode() if isinstance(value, str) else value
    return struct.pack("!H", len(data)) + data


def frame(header, body=b""):
    length, digits = len(body), bytearray()
    while True:
        digit, length = length % 128, length // 128
        digits.append(digit | (128 if length else 0))
        if not length:
            return bytes([header]) + digits + body


class Client:
    def __init__(self, port, identity, clean=False, will=None):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=5)
        flags = (2 if clean else 0) | (0x2c if will else 0)
        body = field("MQTT") + bytes([4, flags, 0, 0]) + field(identity)
        if will:
            body += field(will[0]) + field(will[1])
        self.send(0x10, body)
        header, body = self.receive()
        assert header == 0x20 and body[1] == 0, (header, body)
        self.present = body[0] == 1

    def exact(self, size):
        data = bytearray()
        while len(data) < size:
            more = self.socket.recv(size - len(data))
            assert more, "broker closed socket"
            data.extend(more)
        return bytes(data)

    def send(self, header, body=b""):
        self.socket.sendall(frame(header, body))

    def receive(self):
        header, length = self.exact(1)[0], 0
        for shift in (0, 7, 14, 21):
            digit = self.exact(1)[0]
            length |= (digit & 127) << shift
            if not digit & 128:
                return header, self.exact(length)
        raise AssertionError("invalid Remaining Length")

    def subscribe(self, topic, qos=1):
        self.send(0x82, b"\x00\x01" + field(topic) + bytes([qos]))
        assert self.receive() == (0x90, b"\x00\x01" + bytes([qos]))

    def publish(self, topic, payload, retain=False):
        self.send(0x32 | retain, field(topic) + b"\x00\x02" + payload)
        assert self.receive() == (0x40, b"\x00\x02")

    def message(self):
        header, body = self.receive()
        assert header >> 4 == 3 and (header >> 1 & 3) in (1, 2)
        length = struct.unpack("!H", body[:2])[0]
        return header, body[2:2 + length].decode(), body[2 + length:4 + length], body[4 + length:]

    def ack(self, identifier):
        self.send(0x40, identifier)
        # A PINGRESP after the ACK confirms that its durable state transition finished.
        self.send(0xc0)
        assert self.receive() == (0xd0, b"")

    def close(self):
        self.socket.close()

    def start_qos2(self, duplicate=False):
        self.send(0x3c if duplicate else 0x34, field("qos2/data") + b"\x00\x09" + b"exactly-once")
        assert self.receive() == (0x50, b"\x00\x09")


class Broker:
    def __init__(self, database):
        self.process = subprocess.Popen([str(ROOT / os.environ.get("CARGO_TARGET_DIR", "target") / "debug/durable-broker"), str(database), "127.0.0.1:0"],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                assert selector.select(timeout=15), "recovery/listener startup timed out"
            line = self.process.stdout.readline()
            self.port = int(line.strip().rsplit(":", 1)[1])
        except BaseException:
            self.process.kill()
            _, errors = self.process.communicate(timeout=10)
            print(errors)
            raise

    def crash(self):
        self.process.kill()
        _, errors = self.process.communicate(timeout=10)
        assert self.process.returncode == -signal.SIGKILL, errors

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                _, errors = self.process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.communicate()
                raise AssertionError("durable broker checked shutdown timed out")
            assert self.process.returncode == 0, errors
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", self.port))


def main():
    clients, broker = [], None
    with tempfile.TemporaryDirectory(prefix="mqtt-storage-interop-") as directory:
        database = Path(directory) / "broker.sqlite"
        try:
            broker = Broker(database)
            subscriber = Client(broker.port, "persistent-a")
            publisher = Client(broker.port, "clean-publisher", clean=True)
            clients += [subscriber, publisher]
            subscriber.subscribe("persist/#")
            publisher.publish("persist/first", b"unacknowledged", retain=True)
            first = subscriber.message()
            qsub = Client(broker.port, "qos2-reader")
            qpub = Client(broker.port, "qos2-writer")
            clients += [qsub, qpub]
            qsub.subscribe("qos2/#", qos=2)
            qpub.start_qos2()
            qfirst = qsub.message()
            assert qfirst[0] == 0x34
            broker.crash()
            for client in clients:
                client.close()
            clients.clear()

            broker = Broker(database)
            publisher = Client(broker.port, "new-publisher", clean=True)
            clients.append(publisher)
            publisher.publish("persist/offline", b"routed-before-reconnect")
            subscriber = Client(broker.port, "persistent-a")
            clients.append(subscriber)
            assert subscriber.present
            replay = subscriber.message()
            assert replay[0] & 8 and replay[1:] == first[1:]
            offline = subscriber.message()
            assert offline[1] == "persist/offline" and offline[3] == b"routed-before-reconnect"
            # Drain both PUBLISH packets before reading the subsequent PINGRESPs.
            subscriber.ack(replay[2])
            subscriber.ack(offline[2])
            retained = Client(broker.port, "retained-reader", clean=True)
            clients.append(retained)
            retained.subscribe("persist/first")
            stored = retained.message()
            assert stored[0] & 1 and stored[3] == b"unacknowledged"
            retained.ack(stored[2])
            will = Client(broker.port, "clean-will", clean=True, will=("persist/will", b"crash-will"))
            clients.append(will)
            qpub = Client(broker.port, "qos2-writer")
            qsub = Client(broker.port, "qos2-reader")
            clients += [qpub, qsub]
            assert qpub.present and qsub.present
            qpub.start_qos2(duplicate=True)
            qreplay = qsub.message()
            assert qreplay[0] == 0x3c and qreplay[1:] == qfirst[1:]
            qsub.send(0x50, qreplay[2])
            assert qsub.receive() == (0x62, qreplay[2])
            broker.crash()
            for client in clients:
                client.close()
            clients.clear()

            broker = Broker(database)
            subscriber = Client(broker.port, "persistent-a")
            clients.append(subscriber)
            assert subscriber.present
            triggered = subscriber.message()
            assert triggered[1] == "persist/will" and triggered[3] == b"crash-will"
            subscriber.ack(triggered[2])
            qpub = Client(broker.port, "qos2-writer")
            qsub = Client(broker.port, "qos2-reader")
            clients += [qpub, qsub]
            assert qpub.present and qsub.present
            assert qsub.receive() == (0x62, qfirst[2])
            for _ in range(2):
                qpub.send(0x62, b"\x00\x09")
                assert qpub.receive() == (0x70, b"\x00\x09")
            qsub.send(0x70, qfirst[2])
            qsub.send(0xc0)
            assert qsub.receive() == (0xd0, b"")  # no second application delivery
            print("PASS: SIGKILL preserved QoS2 receiver deduplication, sender Packet ID/DUP and PUBREL phase without rerouting")
            print("PASS: SIGKILL restart restored offline subscription before reconnect, retained value, Packet ID/DUP and armed clean-session Will")
            for client in clients:
                client.close()
            clients.clear()
            broker.stop()
            print("PASS: durable composition checked shutdown and port release")
            broker = None
        finally:
            for client in clients:
                client.close()
            if broker is not None and broker.process.poll() is None:
                broker.stop()


if __name__ == "__main__":
    main()
