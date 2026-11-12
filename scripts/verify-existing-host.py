#!/usr/bin/env python3
"""Verify only owned development-host processes: MQTT, SIGINT/SIGTERM and drain."""
import argparse
import os
from pathlib import Path
import queue
import re
import signal
import socket
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def exact(stream, count):
    result = b''
    while len(result) < count:
        part = stream.recv(count - len(result))
        if not part:
            raise EOFError('development host closed the MQTT stream')
        result += part
    return result


def verify(binary):
    for signum in (signal.SIGINT, signal.SIGTERM):
        process = subprocess.Popen([str(binary), '--listen', '127.0.0.1:0'],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        lines, output = queue.Queue(), []
        def collect():
            for line in process.stdout:
                output.append(line)
                lines.put(line)
            lines.put(None)
        reader = threading.Thread(target=collect)
        reader.start()
        address = None
        try:
            deadline = time.monotonic() + 15
            while address is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('development host Ready timeout')
                line = lines.get(timeout=remaining)
                if line is None:
                    raise RuntimeError('development host exited before Ready')
                match = re.fullmatch(r'MQTT subset listening on (127\.0\.0\.1):(\d+)\n', line)
                if match:
                    address = (match[1], int(match[2]))
            with socket.create_connection(address, timeout=5) as stream:
                stream.settimeout(5)
                stream.sendall(b'\x10\x0d\x00\x04MQTT\x04\x02\x00\x1e\x00\x01x')
                assert exact(stream, 4) == b'\x20\x02\x00\x00'
                stream.sendall(b'\xc0\x00')
                assert exact(stream, 2) == b'\xd0\x00'
                stream.sendall(b'\xe0\x00')
            process.send_signal(signum)
            assert process.wait(timeout=40) == 0, ''.join(output)
            reader.join(timeout=5)
            assert not reader.is_alive()
            assert any('checked shutdown: GracefullyStopped' in line for line in output)
            with socket.socket() as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(address)
            print('Development host MQTT, ' + signal.Signals(signum).name + ', checked drain and port release: Passed')
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            reader.join(timeout=5)
            process.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / os.environ.get('CARGO_TARGET_DIR', 'target') / 'debug/existing-modorch-server')
    args = parser.parse_args()
    verify(args.binary.resolve())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
