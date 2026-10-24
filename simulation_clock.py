"""Keep CSV replay on a continuous timeline, including live pause and speed controls."""

import time


class SimulationClock:
    POLL_INTERVAL = 0.1

    def __init__(self, read_control):
        self._read_control = read_control
        self._paused, self._speed = read_control()
        self._last_wall_time = time.monotonic()
        self._virtual_seconds = 0.0

    def wait_until(self, virtual_seconds):
        """Wait for an offset from the first CSV timestamp; return the active speed."""
        while True:
            paused, speed = self._read_control()
            now = time.monotonic()

            # Charge elapsed time to the previous control state. Processing and
            # late wakeups advance this clock too, so later waits can be shorter.
            if not self._paused:
                self._virtual_seconds += (now - self._last_wall_time) * self._speed
            self._last_wall_time = now
            self._paused, self._speed = paused, speed

            remaining = virtual_seconds - self._virtual_seconds
            if not paused and remaining <= 0:
                return speed

            # Pause freezes virtual time. Resume and speed changes take effect
            # when observed, without rescaling time that has already passed.
            delay = self.POLL_INTERVAL if paused else min(remaining / speed, self.POLL_INTERVAL)
            time.sleep(delay)
