#!/usr/bin/env python3
"""Measure sustained TLS/Auth/SQLite QoS2 traffic, unacked slow consumer,
loaded checked shutdown and durable resume. Local baseline unless explicit SLOs
are supplied; raw samples/latencies and ack boundaries are retained for review.
"""
import argparse
import asyncio
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import signal
import ssl
import struct
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), ROOT / 'scripts' / f'{name}.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


WIRE = module('benchmark')
SOURCES = module('check-release')


async def recv(reader):
    return await asyncio.wait_for(WIRE.receive(reader), 10)


async def send(writer, header, body=b''):
    writer.write(WIRE.frame(header, body))
    await asyncio.wait_for(writer.drain(), 10)


async def connect(port, context, identity, clean=True, trace=None):
    trace = trace if trace is not None else {}
    started = time.monotonic()
    writer = None
    trace.update(status='Running', stage='tcp_tls')
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection('127.0.0.1', port, ssl=context), 10)
        trace['tcp_tls_ms'] = (time.monotonic()-started)*1000
        trace['stage'] = 'mqtt_connack'
        mqtt_started = time.monotonic()
        body = WIRE.text('MQTT') + bytes([4, 0xc2 if clean else 0xc0, 0, 0])
        body += WIRE.text('device-soak-' + identity) + WIRE.text('device') + WIRE.text('demo-password')
        await send(writer, 0x10, body)
        header, body = await recv(reader)
        assert header == 0x20 and body[1:] == b'\x00', (header, body.hex())
        trace.update(status='Passed', mqtt_connack_ms=(time.monotonic()-mqtt_started)*1000)
        return reader, writer, bool(body[0])
    except BaseException as error:
        trace.update(status='Failed', error=type(error).__name__)
        if writer is not None:
            writer.transport.abort()
        raise
    finally:
        trace['total_ms'] = (time.monotonic()-started)*1000


async def subscribe(reader, writer, topic, qos):
    await send(writer, 0x82, b'\x00\x01' + WIRE.text(topic) + bytes([qos]))
    assert await recv(reader) == (0x90, b'\x00\x01' + bytes([qos]))


async def publish(reader, writer, topic, identifier, payload):
    pkid = struct.pack('!H', identifier)
    await send(writer, 0x34, WIRE.text(topic) + pkid + payload)
    assert await recv(reader) == (0x50, pkid)
    await send(writer, 0x62, pkid)
    assert await recv(reader) == (0x70, pkid)


def publication(header, body, topic):
    assert header >> 4 == 3 and header & 6 in (2, 4), (header, body.hex())
    prefix = WIRE.text(topic)
    assert body.startswith(prefix)
    return body[len(prefix):len(prefix)+2], body[len(prefix)+2:]


def recovered_publication(header, pkid, payload, sequence, payload_bytes, observed):
    """Only actually observed, unacknowledged frames require retransmission identity."""
    identifier = int.from_bytes(pkid, 'big')
    if header >> 4 != 3 or header & 6 != 2 or len(pkid) != 2 or not identifier:
        raise ValueError(f'invalid recovered QoS1 publication: sequence={sequence}')
    if payload != struct.pack('!Q', sequence) + bytes(payload_bytes-8):
        raise ValueError(f'recovered payload/order mismatch: sequence={sequence}')
    previous = observed.get(sequence)
    if previous is not None:
        if not header & 8:
            raise ValueError(f'observed unacknowledged delivery requires DUP=1: sequence={sequence}')
        if identifier != previous['packet_id'] or payload.hex() != previous['payload_hex']:
            raise ValueError(f'observed delivery changed original Packet ID/payload: sequence={sequence}')
    return dict(sequence=sequence, dup=bool(header & 8), packet_id=identifier,
                observed_before_restart=previous is not None)


def assess(report, args):
    metrics = {
        'peak_rss_kib': (report['peak_rss_kib'], args.max_rss_kib, '<='),
        'rss_growth_kib': (report['rss_growth_kib'], args.max_rss_growth_kib, '<='),
        'p99_ms': (report['p99_ms'], args.max_p99_ms, '<='),
        'loaded_shutdown_seconds': (report['loaded_shutdown_seconds'], args.max_shutdown_seconds, '<='),
        'recovery_to_resumed_connack_seconds': (report['recovery_to_resumed_connack_seconds'], args.max_recovery_seconds, '<='),
        'messages_per_second': (report['messages_per_second'], args.min_messages_per_second, '>='),
    }
    checks = {key: dict(measured=value, threshold=threshold, comparison=op,
                        status='NotRun' if threshold is None else
                        ('Passed' if (value <= threshold if op == '<=' else value >= threshold) else 'Failed'))
              for key, (value, threshold, op) in metrics.items()}
    states = {v['status'] for v in checks.values()}
    return dict(status='Failed' if 'Failed' in states else ('NotRun' if 'NotRun' in states else 'Passed'), checks=checks,
                scope='Explicit local thresholds only; production workload representativeness requires independent review.')


async def scenario(args, report):
    binary = ROOT / os.environ.get('CARGO_TARGET_DIR', 'target') / args.profile / 'secure-modorch-server'
    database = args.output.parent / (args.output.stem + '.sqlite')
    if database.exists():
        raise ValueError('refusing to reuse an existing database')
    context = ssl.create_default_context(cafile=str(ROOT / 'crates/modorch-mqtt-core/tests/fixtures/localhost-cert.pem'))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    command = [str(binary), '--demo', '--database', str(database)]
    report['command'] = command
    report['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
    sockets, tasks, processes = [], [], []
    running = True
    active = set()
    counters = dict(started=0, upstream_complete=0, downstream_complete=0, completed=0, interrupted=0)
    samples, latencies = [], []
    report.update(counters=counters, rss_samples=samples, raw_latency_ms=latencies)

    async def launch(label):
        started = time.monotonic()
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        processes.append(process)
        line = (await asyncio.wait_for(process.stdout.readline(), 30)).decode().strip()
        config = (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        report[label] = dict(listen_line=line, config=config, spawn_to_listen_seconds=time.monotonic()-started)
        return process, int(line.rsplit(':', 1)[1])

    async def client(port, name, clean=True):
        report['stage'] = 'connect:' + name
        trace = dict(client=name)
        report.setdefault('connection_attempts', []).append(trace)
        reader, writer, present = await connect(port, context, name, clean, trace)
        sockets.append(writer)
        return reader, writer, present

    async def collect(process):
        while process.returncode is None:
            samples.append(dict(seconds=time.monotonic()-started, rss_kib=WIRE.rss_kib(process.pid),
                                active=len(active), **counters))
            await asyncio.sleep(.1)

    async def worker(index, reader, writer, topic):
        sequence = 0
        while running:
            sequence += 1
            pkid = struct.pack('!H', sequence % 65535 + 1)
            payload = struct.pack('!QQ', index, sequence) + bytes(args.payload_bytes-16)
            before = time.monotonic()
            counters['started'] += 1
            active.add(index)
            try:
                await send(writer, 0x34, WIRE.text(topic) + pkid + payload)
                upstream, downstream, seen = False, False, False
                while not (upstream and downstream):
                    header, body = await recv(reader)
                    if header == 0x50:
                        assert body == pkid
                        await send(writer, 0x62, pkid)
                    elif header == 0x70:
                        assert body == pkid and not upstream
                        upstream = True
                        counters['upstream_complete'] += 1
                    elif header == 0x62:
                        assert seen and body == downstream_id
                        await send(writer, 0x70, body)
                        downstream = True
                        counters['downstream_complete'] += 1
                    else:
                        assert header == 0x34 and not seen
                        downstream_id, actual = publication(header, body, topic)
                        assert actual == payload
                        seen = True
                        await send(writer, 0x50, downstream_id)
                counters['completed'] += 1
                latencies.append((time.monotonic()-before)*1000)
            except (ConnectionError, OSError, asyncio.IncompleteReadError):
                if running:
                    raise
                counters['interrupted'] += 1
                return
            finally:
                active.discard(index)
            await asyncio.sleep(1 / args.rate_per_client)

    try:
        process, port = await launch('initial_start')
        slow_topic = 'devices/soak/slow'
        slow_r, slow_w, present = await client(port, 'slow', False)
        assert not present
        await subscribe(slow_r, slow_w, slow_topic, 1)
        seed_r, seed_w, _ = await client(port, 'seed')
        for sequence in range(args.slow_messages):
            report['stage'] = f'seed:{sequence}'
            await publish(seed_r, seed_w, slow_topic, sequence+1, struct.pack('!Q', sequence)+bytes(args.payload_bytes-8))
        # Observe one delivery without ACK, then leave this socket open and unread.
        header, body = await recv(slow_r)
        pkid, payload = publication(header, body, slow_topic)
        assert header & 6 == 2 and int.from_bytes(payload[:8], 'big') == 0
        observed = {0: dict(packet_id=int.from_bytes(pkid, 'big'), payload_hex=payload.hex())}
        assert observed[0]['packet_id'] and payload == bytes(args.payload_bytes)
        report['slow_consumer'] = dict(accepted=args.slow_messages, acknowledged=0,
                                      observed_before_restart=observed,
                                      mode='open socket, no PUBACK, no further reads')
        # Bound setup concurrency independently from steady-state client count.
        # Keep every failed attempt rather than silently retrying the burst.
        admission = asyncio.Semaphore(args.connect_concurrency)

        async def configured(index):
            async with admission:
                reader, writer, _ = await client(port, str(index))
                topic = f'devices/soak/{index}'
                await subscribe(reader, writer, topic, 2)
                return index, reader, writer, topic

        outcomes = await asyncio.gather(*(configured(i) for i in range(args.clients)), return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        connected = outcomes
        started = time.monotonic()
        sampler = asyncio.create_task(collect(process))
        tasks.append(sampler)
        runners = [asyncio.create_task(worker(*row)) for row in connected]
        tasks.extend(runners)
        until = started + args.duration
        while time.monotonic() < until:
            for runner in runners:
                if runner.done():
                    await runner
                    raise AssertionError('load worker stopped before shutdown')
            await asyncio.sleep(min(.1, max(0, until-time.monotonic())))
        # Signal only while an exchange is active; clients remain connected.
        async def wait_active():
            while not active:
                await asyncio.sleep(.001)
        await asyncio.wait_for(wait_active(), 10)
        report['signal_boundary'] = dict(active=len(active), sockets=len(sockets), latency_samples=len(latencies), **counters)
        running = False
        shutdown_started = time.monotonic()
        process.send_signal(signal.SIGINT)
        output, errors = await asyncio.wait_for(process.communicate(), 30)
        report['loaded_shutdown_seconds'] = time.monotonic()-shutdown_started
        report['shutdown_stdout'] = output.decode()
        report['shutdown_stderr'] = errors.decode()
        assert process.returncode == 0 and b'checked shutdown complete' in output
        await asyncio.gather(*runners)
        await sampler
        elapsed = shutdown_started - started
        report['measured_duration_seconds'] = elapsed
        assert counters['completed'] > 0 and samples and latencies
        report['messages_per_second'] = report['signal_boundary']['completed'] / elapsed
        ordered = sorted(latencies[:report['signal_boundary']['latency_samples']])
        assert ordered, 'no completed latency samples before the shutdown signal'
        report['p99_ms'] = ordered[max(0, math.ceil(len(ordered)*.99)-1)]
        rss = [s['rss_kib'] for s in samples if s['rss_kib'] is not None]
        assert rss, 'RSS measurement requires Linux /proc'
        report['peak_rss_kib'] = max(rss)
        report['rss_growth_kib'] = rss[-1]-rss[0]
        for writer in sockets:
            writer.transport.abort()
        restart_started = time.monotonic()
        recovered, port = await launch('restart')
        reader, writer, present = await client(port, 'slow', False)
        report['recovery_to_resumed_connack_seconds'] = time.monotonic()-restart_started
        assert present, 'persistent slow-consumer session was lost'
        restored = []
        report['recovered_slow_messages'] = restored
        for sequence in range(args.slow_messages):
            header, body = await recv(reader)
            pkid, payload = publication(header, body, slow_topic)
            restored.append(recovered_publication(header, pkid, payload, sequence, args.payload_bytes, observed))
            await send(writer, 0x40, pkid)
        barrier_r, barrier_w, _ = await client(port, 'recovery-barrier')
        await publish(barrier_r, barrier_w, slow_topic, 1, b'recovery-barrier')
        header, body = await recv(reader)
        pkid, payload = publication(header, body, slow_topic)
        assert payload == b'recovery-barrier', 'unexpected duplicate before reliable FIFO barrier'
        await send(writer, 0x40, pkid)
        report['recovery_fifo_barrier'] = 'Passed'
        await send(writer, 0xc0)
        assert await recv(reader) == (0xd0, b'')
        report['database_bytes'] = database.stat().st_size
        recovered.send_signal(signal.SIGINT)
        output, errors = await asyncio.wait_for(recovered.communicate(), 30)
        assert recovered.returncode == 0 and b'checked shutdown complete' in output, errors.decode()
        report['functional_status'] = 'Passed'
        report['slo'] = assess(report, args)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for writer in sockets:
            writer.transport.abort()
        for index, process in enumerate(processes):
            if process.returncode is None:
                process.kill()
                output, errors = await process.communicate()
                report[f'forced_cleanup_{index}'] = dict(stdout=output.decode(), stderr=errors.decode(), exit_code=process.returncode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration', type=float, default=300)
    parser.add_argument('--clients', type=int, default=8)
    parser.add_argument('--connect-concurrency', type=int, default=1,
                        help='bounded simultaneous client setup (default: sequential)')
    parser.add_argument('--slow-messages', type=int, default=64)
    parser.add_argument('--rate-per-client', type=float, default=10)
    parser.add_argument('--payload-bytes', type=int, default=128)
    parser.add_argument('--profile', choices=['debug', 'release'], default='release')
    parser.add_argument('--output', type=Path, required=True)
    for key in ['max-rss-kib', 'max-rss-growth-kib', 'max-p99-ms', 'max-shutdown-seconds', 'max-recovery-seconds', 'min-messages-per-second']:
        parser.add_argument('--' + key, type=float)
    args = parser.parse_args()
    if (not math.isfinite(args.duration) or args.duration <= 0 or not 1 <= args.clients <= 30
            or not 1 <= args.connect_concurrency <= 30 or not 1 <= args.slow_messages <= 128 or not 16 <= args.payload_bytes <= 4000
            or not math.isfinite(args.rate_per_client) or args.rate_per_client <= 0):
        parser.error('positive finite duration/rate, clients/concurrency 1..30 (secure demo 32/IP), slow messages 1..128, payload 16..4000 required')
    for key, value in vars(args).items():
        if key.startswith(('min_', 'max_')) and value is not None and (not math.isfinite(value) or value < 0):
            parser.error('SLO thresholds must be finite and nonnegative')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x'):
        pass
    report = dict(functional_status='Failed', scope='Linux local sustained baseline, no production SLO approval',
                  environment=dict(platform=platform.platform(), logical_cpus=os.cpu_count(), load_average=os.getloadavg(),
                                   cgroup_cpu_max=Path('/sys/fs/cgroup/cpu.max').read_text().strip()
                                   if Path('/sys/fs/cgroup/cpu.max').is_file() else None),
                  parameters={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  source_inputs=SOURCES.source_inputs(ROOT))
    report['source_digest'] = SOURCES.object_digest(report['source_inputs'])
    code = 0
    try:
        asyncio.run(scenario(args, report))
        code = int(report['slo']['status'] == 'Failed')
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
        report['traceback'] = traceback.format_exc()
        code = 1
    finally:
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({k: v for k, v in report.items() if k in {'functional_status', 'error', 'slo', 'messages_per_second', 'p99_ms', 'peak_rss_kib', 'rss_growth_kib', 'loaded_shutdown_seconds', 'recovery_to_resumed_connack_seconds'}}, ensure_ascii=False))
    return code


if __name__ == '__main__':
    sys.exit(main())
