"""Fixed-width traffic samples shared by the simulator and dashboard."""

import os
import struct

# The header is "traffic metrics v1" + 3 reserved bytes.
HEADER = b'TRFM\x01\x00\x00\x00'
# The record is little-endian: UTC epoch seconds (float64), then four unsigned 32-bit counts.
RECORD = struct.Struct('<dIIII')


def pack_sample(sample, wall_time):
    return RECORD.pack(
        wall_time, sample['expected'], sample['attempted'], sample['acked'], sample['errors']
    )


def read_recent(path, limit=900):
    """Read only the requested tail, excluding a record still being written."""
    if limit < 0:
        raise ValueError('Sample limit must be nonnegative')
    try:
        with open(path, 'rb', buffering=0) as metrics_file:
            header = metrics_file.read(len(HEADER))
            if len(header) < len(HEADER):
                # Silently return an empty file as there are no metrics available yet.
                # The file is only partially written.
                return HEADER
            if header != HEADER:
                raise ValueError('Unsupported traffic metrics format')
            end = metrics_file.seek(0, os.SEEK_END)
            complete_records = max(0, (end - len(HEADER)) // RECORD.size)
            count = min(limit, complete_records)
            metrics_file.seek(len(HEADER) + (complete_records - count) * RECORD.size)
            data = metrics_file.read(count * RECORD.size)
            # A new simulation can truncate the file during a dashboard refresh.
            return HEADER + data[:len(data) // RECORD.size * RECORD.size]
    except FileNotFoundError:
        # Silently return an empty file as there are no metrics available yet.
        return HEADER
