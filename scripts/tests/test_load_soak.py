"""SLO evaluation cannot turn an unconfigured baseline into production approval."""
import argparse
import importlib.util
from pathlib import Path
import unittest
import struct

spec = importlib.util.spec_from_file_location('load_soak', Path(__file__).parents[1] / 'load-soak.py')
load = importlib.util.module_from_spec(spec)
spec.loader.exec_module(load)


class LoadThresholdTests(unittest.TestCase):
    def setUp(self):
        self.report = dict(peak_rss_kib=1000, rss_growth_kib=100, p99_ms=20,
                           loaded_shutdown_seconds=.5, recovery_to_resumed_connack_seconds=1,
                           messages_per_second=100)
        self.args = argparse.Namespace(max_rss_kib=None, max_rss_growth_kib=None, max_p99_ms=None,
                                       max_shutdown_seconds=None, max_recovery_seconds=None,
                                       min_messages_per_second=None)

    def test_unconfigured_and_partially_configured_slos_are_not_approved(self):
        self.assertEqual(load.assess(self.report, self.args)['status'], 'NotRun')
        self.args.max_rss_kib = 2000
        self.assertEqual(load.assess(self.report, self.args)['status'], 'NotRun')
        self.args.max_p99_ms = 10
        self.assertEqual(load.assess(self.report, self.args)['status'], 'Failed')

    def test_threshold_equality_passes_and_each_real_violation_fails(self):
        self.args = argparse.Namespace(max_rss_kib=1000, max_rss_growth_kib=100, max_p99_ms=20,
                                       max_shutdown_seconds=.5, max_recovery_seconds=1,
                                       min_messages_per_second=100)
        self.assertEqual(load.assess(self.report, self.args)['status'], 'Passed')
        for key in self.report:
            with self.subTest(key=key):
                changed = dict(self.report)
                changed[key] += -1 if key == 'messages_per_second' else 1
                self.assertEqual(load.assess(changed, self.args)['status'], 'Failed')


class RecoveryIdentityTests(unittest.TestCase):
    def setUp(self):
        self.payload = struct.pack('!Q', 0) + bytes(120)
        self.observed = {0: dict(packet_id=7, payload_hex=self.payload.hex())}

    def test_observed_unacked_delivery_requires_dup_original_id_and_payload(self):
        valid = load.recovered_publication(0x3a, b'\x00\x07', self.payload, 0, 128, self.observed)
        self.assertTrue(valid['observed_before_restart'])
        for header, pkid, payload in [(0x32, b'\x00\x07', self.payload),
                                      (0x3a, b'\x00\x08', self.payload),
                                      (0x3a, b'\x00\x00', self.payload),
                                      (0x3a, b'\x00\x07', self.payload[:-1]+b'x')]:
            with self.subTest(header=header, pkid=pkid, payload=payload):
                with self.assertRaises(ValueError):
                    load.recovered_publication(header, pkid, payload, 0, 128, self.observed)

    def test_unobserved_messages_do_not_assume_the_broker_had_sent_them(self):
        payload = struct.pack('!Q', 1) + bytes(120)
        for header in [0x32, 0x3a]:
            row = load.recovered_publication(header, b'\x00\x08', payload, 1, 128, self.observed)
            self.assertFalse(row['observed_before_restart'])
        with self.assertRaises(ValueError):
            load.recovered_publication(0x34, b'\x00\x08', payload, 1, 128, self.observed)


if __name__ == '__main__':
    unittest.main()
