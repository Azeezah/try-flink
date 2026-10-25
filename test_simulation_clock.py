"""Timing regressions without sleeping or connecting to Kafka."""

import unittest
from unittest.mock import patch

from simulation_clock import SimulationClock


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.oversleep = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds + self.oversleep


class SimulationClockTests(unittest.TestCase):
    def setUp(self):
        self.time = FakeTime()
        self.monotonic = patch('simulation_clock.time.monotonic', self.time.monotonic)
        self.sleep = patch('simulation_clock.time.sleep', self.time.sleep)
        self.monotonic.start()
        self.sleep.start()
        self.addCleanup(self.monotonic.stop)
        self.addCleanup(self.sleep.stop)

    def test_oversleep_and_send_work_do_not_accumulate(self):
        self.time.oversleep = 0.002
        clock = SimulationClock(lambda: (False, 120))
        for second in range(121):
            clock.wait_until(second)
            # Work done sending each event must count toward the next deadline.
            self.time.now += 0.001
        self.assertGreaterEqual(self.time.now, 1.0)
        self.assertLessEqual(self.time.now, 1.003001)

    def test_same_timestamp_events_do_not_shift_the_next_deadline(self):
        clock = SimulationClock(lambda: (False, 1))
        for _ in range(10):
            clock.wait_until(0)
            self.time.now += 0.01
        clock.wait_until(1)
        self.assertAlmostEqual(self.time.now, 1.0)

    def test_overdue_events_catch_up_without_dropping_them(self):
        clock = SimulationClock(lambda: (False, 1))
        self.time.now = 5.0
        for second in range(6):
            clock.wait_until(second)
            self.assertEqual(self.time.now, 5.0)
        clock.wait_until(6)
        self.assertAlmostEqual(self.time.now, 6.0)

    def test_pause_preserves_progress_through_a_gap(self):
        def controls():
            return 0.4 - 1e-9 <= self.time.now < 2.4 - 1e-9, 1

        clock = SimulationClock(controls)
        clock.wait_until(1)
        # 0.4 active seconds, 2 paused seconds, then 0.6 active seconds.
        self.assertAlmostEqual(self.time.now, 3.0)

    def test_initial_pause_applies_to_the_first_event(self):
        clock = SimulationClock(lambda: (self.time.now < 2 - 1e-9, 120))
        clock.wait_until(0)
        self.assertAlmostEqual(self.time.now, 2.0)
        clock.wait_until(120)
        self.assertAlmostEqual(self.time.now, 3.0)

    def test_speed_change_applies_only_to_remaining_virtual_time(self):
        for initial, final, expected in ((2, 1, 8), (1, 2, 6)):
            with self.subTest(initial=initial, final=final):
                self.time.now = 0.0

                def controls():
                    return False, initial if self.time.now < 2 - 1e-9 else final

                clock = SimulationClock(controls)
                self.assertIsNone(clock.wait_until(seconds_since_start=10))
                self.assertAlmostEqual(self.time.now, expected)


if __name__ == '__main__':
    unittest.main()
