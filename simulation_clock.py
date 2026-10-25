"""A continuous simulation clock with live pause and speed controls."""

import time


class SimulationClock:
    POLL_INTERVAL = 0.1

    def __init__(self, read_control):
        self._read_control = read_control
        self._paused, self._speed = read_control()
        self._last_wall_time = time.monotonic()
        self._seconds_since_start = 0.0

    def wait_until(self, seconds_since_start):
        while True:
            paused, speed = self._read_control()
            now = time.monotonic()

            # Charge elapsed time to the previous control state.
            if not self._paused:
                self._seconds_since_start += (now - self._last_wall_time) * self._speed
            self._last_wall_time = now
            self._paused, self._speed = paused, speed

            remaining = seconds_since_start - self._seconds_since_start
            if not paused and remaining <= 0:
                return

            # When paused, loop with a fixed delay.  
            # When not paused, also loop with a fixed delay (so we can pick up config updates quickly).  
            # Unless there's not much time left, in which case we sleep for the remaining time.
            delay = self.POLL_INTERVAL if paused else min(remaining / speed, self.POLL_INTERVAL)
            time.sleep(delay)
