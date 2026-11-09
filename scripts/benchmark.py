#!/usr/bin/env python3
"""Record an MQTT loopback baseline with asyncio TCP clients, without external dependencies.

Each client subscribes to its own Topic and measures PUB->SUB latency. Defaults:
1/10/100/500 clients, QoS0/1, 20 messages/client and 128-byte payloads. Optional
TLS/Auth and TLS/Auth/SQLite compositions retain the demo 32-client IP limit.
QoS2 completes both independent exchanges before the next message. This includes
client event-loop scheduling and network/queue overhead; it is not a production SLO.
"""
import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import signal
import ssl
import tempfile
import struct
import subprocess
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(value):
    encoded = value.encode()
    return struct.pack("!H", len(encoded)) + encoded


def frame(header, body):
    length, digits = len(body), bytearray()
    while True:
        digit, length = length % 128, length // 128
        digits.append(digit | (128 if length else 0))
        if not length:
            return bytes([header]) + digits + body


async def receive(reader):
    header = (await reader.readexactly(1))[0]
    length = 0
    for shift in (0, 7, 14, 21):
        digit = (await reader.readexactly(1))[0]
        length |= (digit & 127) << shift
        if not digit & 128:
            return header, await reader.readexactly(length)
    raise AssertionError("invalid remaining length")


def rss_kib(pid):
    status = Path(f"/proc/{pid}/status")
    if not status.exists():
        return None
    for line in status.read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return None


async def scenario(binary, count, qos, messages, payload_bytes, composition="tcp", database=None):
    secure = composition != "tcp"
    command = [str(binary), "--demo"] if secure else [str(binary), "127.0.0.1:0", "--benchmark"]
    if composition == "tls-auth-storage":
        command.extend(["--database", str(database)])
    context = None
    if secure:
        context = ssl.create_default_context(cafile=str(ROOT / "crates/modorch-mqtt-core/tests/fixtures/localhost-cert.pem"))
        context.minimum_version = ssl.TLSVersion.TLSv1_2
    process = await asyncio.create_subprocess_exec(*command,
                                                 stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    sockets, latencies, samples = [], [], []
    measured_result = None
    try:
        line = (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        port = int(line.rsplit(":", 1)[1])
        config = (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        semaphore = asyncio.Semaphore(4 if secure else 50)

        async def connect(index):
            async with semaphore:
                reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context)
                sockets.append((reader, writer))
                identity = f"device-bench-{index}" if secure else f"bench-{index}"
                credentials = text("device") + text("demo-password") if secure else b""
                flags = 0xC2 if secure else 2
                writer.write(frame(0x10, text("MQTT") + bytes([4, flags, 0, 0]) + text(identity) + credentials))
                await writer.drain()
                assert await receive(reader) == (0x20, b"\x00\x00")
                topic = text(f"devices/bench/{index}" if secure else f"bench/{index}")
                writer.write(frame(0x82, b"\x00\x01" + topic + bytes([qos])))
                await writer.drain()
                assert await receive(reader) == (0x90, b"\x00\x01" + bytes([qos]))
                return reader, writer, topic

        connection_started = time.perf_counter()
        connections = [asyncio.create_task(connect(i)) for i in range(count)]
        try:
            connected = await asyncio.wait_for(asyncio.gather(*connections), 30)
        except BaseException:
            for task in connections:
                task.cancel()
            await asyncio.gather(*connections, return_exceptions=True)
            raise
        connection_seconds = time.perf_counter() - connection_started
        peak = rss_kib(process.pid)
        collecting = True

        async def sample_memory():
            while collecting:
                value = rss_kib(process.pid)
                if value is not None:
                    samples.append(value)
                await asyncio.sleep(0.01)

        async def run(client):
            reader, writer, topic = client
            for index in range(messages):
                sent = time.perf_counter_ns()
                payload = struct.pack("!Q", sent) + bytes(payload_bytes - 8)
                identifier = struct.pack("!H", index % 65535 + 1) if qos else b""
                writer.write(frame(0x30 | qos << 1, topic + identifier + payload))
                await writer.drain()
                acknowledged, delivered = not qos, False
                downstream_id = None
                receive_complete = False
                while not (acknowledged and delivered):
                    header, body = await receive(reader)
                    if header == 0x40 and qos == 1:
                        assert body == identifier
                        acknowledged = True
                    elif header == 0x50 and qos == 2:
                        assert body == identifier
                        writer.write(frame(0x62, identifier))
                        await writer.drain()
                    elif header == 0x70 and qos == 2:
                        assert body == identifier
                        acknowledged = True
                    elif header == 0x62 and qos == 2:
                        assert body == downstream_id and receive_complete
                        writer.write(frame(0x70, body))
                        await writer.drain()
                        delivered = True
                    else:
                        assert header == 0x30 | qos << 1
                        length = struct.unpack("!H", body[:2])[0]
                        assert body[:2 + length] == topic
                        offset = 2 + length
                        if qos:
                            downstream_id = body[offset:offset + 2]
                            assert len(downstream_id) == 2 and downstream_id != b"\x00\x00"
                            writer.write(frame(0x50 if qos == 2 else 0x40, downstream_id))
                            await writer.drain()
                            offset += 2
                        assert body[offset:] == payload
                        assert not receive_complete, "duplicate downstream PUBLISH in fault-free measurement"
                        latencies.append((time.perf_counter_ns() - sent) / 1e6)
                        receive_complete = True
                        delivered = qos != 2

        sampler = asyncio.create_task(sample_memory())
        started = time.perf_counter()
        runners = [asyncio.create_task(run(client)) for client in connected]
        try:
            await asyncio.wait_for(asyncio.gather(*runners), 120)
        finally:
            for task in runners:
                task.cancel()
            await asyncio.gather(*runners, return_exceptions=True)
            elapsed = time.perf_counter() - started
            collecting = False
            await sampler
        latencies.sort()
        percentile = lambda percent: latencies[max(0, math.ceil(len(latencies) * percent) - 1)]
        assert len(latencies) == count * messages
        measured_result = {"composition": composition, "clients": count, "qos": qos, "payload_bytes": payload_bytes, "messages_per_client": messages,
                "connection_seconds": connection_seconds,
                "messages": len(latencies), "elapsed_seconds": elapsed, "messages_per_second": len(latencies) / elapsed,
                "latency_ms": {"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)},
                "peak_sampled_broker_rss_kib": max(samples + ([peak] if peak is not None else []), default=None),
                "broker_config": config,
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "database_bytes_before_shutdown": database.stat().st_size if database and database.exists() else None,
                "rss_samples_kib": samples, "raw_latency_ms_sorted": latencies}
        return measured_result
    finally:
        async def close_socket(writer):
            try:
                writer.write(b"\xe0\x00")
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), 2)
            except (ConnectionError, OSError, asyncio.TimeoutError):
                writer.transport.abort()

        await asyncio.gather(*(close_socket(writer) for _, writer in sockets))
        shutdown_started = time.perf_counter()
        if process.returncode is None:
            process.send_signal(signal.SIGINT)
        try:
            output, errors = await asyncio.wait_for(process.communicate(), 10)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            raise AssertionError("benchmark broker failed checked shutdown")
        assert process.returncode == 0, errors.decode()
        assert b"checked shutdown complete" in output, output.decode()
        if measured_result is not None:
            measured_result["checked_shutdown_seconds"] = time.perf_counter() - shutdown_started
        # A standalone record ensures shutdown data survives even if a later case fails.
        print(json.dumps({"composition": composition, "clients": count, "qos": qos,
                          "checked_shutdown_seconds": time.perf_counter() - shutdown_started,
                          "shutdown_scope": "after client exchanges and socket cleanup; not loaded shutdown"}), flush=True)


async def main(args):
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    modorch = next(p for p in lock["package"] if p["name"] == "modorch")
    cpu = next((s.split(":", 1)[1].strip() for s in Path("/proc/cpuinfo").read_text().splitlines()
                if s.startswith("model name")), platform.processor())
    report = {"environment": {"platform": platform.platform(), "cpu": cpu, "logical_cpus": os.cpu_count(), "load_average_before_run": os.getloadavg(),
                              "physical_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                              "rustc": subprocess.check_output(["rustc", "-Vv"], text=True), "modorch_source": modorch["source"],
                              "mqtt_version": "3.1.1", "profile": args.profile, "compositions": args.compositions,
                              "source_base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
                              "benchmark_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                              "standalone_source_sha256": hashlib.sha256((ROOT / "examples/standalone-broker/src/main.rs").read_bytes()).hexdigest()},
              "method": "Self-subscription PUB->SUB including client scheduling/network/queue overhead; one outstanding publication per client; RSS sampled every 10 ms; startup/subscriptions excluded from timed throughput.",
              "results": []}
    for composition in args.compositions:
        binary = ROOT / os.environ.get("CARGO_TARGET_DIR", "target") / args.profile / ("standalone-broker" if composition == "tcp" else "secure-modorch-server")
        for count in args.clients:
            for qos in args.qos:
                with tempfile.TemporaryDirectory(prefix="mqtt-benchmark-") as directory:
                    database = Path(directory) / "broker.sqlite" if composition == "tls-auth-storage" else None
                    result = await scenario(binary, count, qos, args.messages, args.payload_bytes, composition, database)
                report["results"].append(result)
                print(json.dumps({k: v for k, v in result.items() if k not in {"rss_samples_kib", "raw_latency_ms_sorted"}}, ensure_ascii=False), flush=True)
                args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 10, 100, 500])
    parser.add_argument("--qos", type=int, nargs="+", choices=[0, 1, 2], default=[0, 1])
    parser.add_argument("--compositions", nargs="+", choices=["tcp", "tls-auth", "tls-auth-storage"], default=["tcp"])
    parser.add_argument("--messages", type=int, default=20)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--profile", choices=["debug", "release"], default="release")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if any(n < 1 or n > 500 for n in arguments.clients) or arguments.messages < 1 or not 8 <= arguments.payload_bytes <= 4000:
        parser.error("clients must be 1..500, messages positive, payload 8..4000 bytes")
    if any(mode != "tcp" for mode in arguments.compositions) and max(arguments.clients) > 32:
        parser.error("secure demo keeps its 32 connections/IP admission limit; select --clients 1 10 32")
    asyncio.run(main(arguments))
