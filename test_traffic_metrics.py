"""Binary storage and dashboard response checks, without Docker or Kafka."""

import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from traffic_metrics import HEADER, RECORD, pack_sample, read_recent


class TrafficMetricsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'metrics.bin'

    def record(self, index):
        sample = {'expected': index + 3, 'attempted': index + 2, 'acked': index + 1, 'errors': 0}
        return pack_sample(sample, 1792857600.123456 + index)

    def test_round_trip_preserves_timestamp_and_unsigned_counts(self):
        sample = {'expected': 4294967295, 'attempted': 2147483648, 'acked': 123, 'errors': 4}
        timestamp = 1792857600.123456
        self.path.write_bytes(HEADER + pack_sample(sample, timestamp))
        response = read_recent(self.path)
        self.assertEqual(RECORD.size, 24)
        self.assertEqual(RECORD.unpack(response[len(HEADER):]), (timestamp, *sample.values()))

    def test_only_latest_complete_samples_are_returned(self):
        records = [self.record(i) for i in range(1005)]
        incomplete = self.record(1005)
        self.path.write_bytes(HEADER + b''.join(records) + incomplete[:10])
        self.assertEqual(read_recent(self.path), HEADER + b''.join(records[-900:]))
        with self.path.open('ab') as metrics_file:
            metrics_file.write(incomplete[10:])
        self.assertEqual(read_recent(self.path, 2), HEADER + records[-1] + incomplete)

    def test_missing_empty_and_incomplete_header_return_empty_history(self):
        self.assertEqual(read_recent(self.path), HEADER)
        for contents in (b'', HEADER[:3], HEADER):
            with self.subTest(contents=contents):
                self.path.write_bytes(contents)
                self.assertEqual(read_recent(self.path), HEADER)

    def test_unsupported_format_is_rejected(self):
        for header in (b'TRFM\x02\x00\x00\x00', b'{"expect'):
            with self.subTest(header=header):
                self.path.write_bytes(header)
                with self.assertRaisesRegex(ValueError, 'Unsupported'):
                    read_recent(self.path)

    def test_small_history_zero_limit_and_invalid_limit(self):
        self.path.write_bytes(HEADER + self.record(0) + self.record(1))
        self.assertEqual(read_recent(self.path), self.path.read_bytes())
        self.assertEqual(read_recent(self.path, 0), HEADER)
        with self.assertRaises(ValueError):
            read_recent(self.path, -1)

    def test_reader_does_not_scan_old_history(self):
        class TrackingFile(io.BytesIO):
            def __init__(self, data):
                super().__init__(data)
                self.read_sizes = []

            def read(self, count=-1):
                self.read_sizes.append(count)
                return super().read(count)

        metrics_file = TrackingFile(HEADER + self.record(0) * 10000)
        with patch('traffic_metrics.open', return_value=metrics_file, create=True):
            self.assertEqual(len(read_recent(self.path)), len(HEADER) + 900 * RECORD.size)
        self.assertEqual(metrics_file.read_sizes, [len(HEADER), 900 * RECORD.size])

    def test_dashboard_serves_the_same_binary_tail(self):
        import app

        self.path.write_bytes(HEADER + self.record(0))
        with patch.object(app, 'METRICS_FILE', self.path):
            with app.app.test_client() as client:
                response = client.get('/api/traffic')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'application/octet-stream')
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(response.data, self.path.read_bytes())


if __name__ == '__main__':
    unittest.main()
