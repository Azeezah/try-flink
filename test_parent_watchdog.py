"""Exercise parent death using real processes, without Kafka or Docker."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parent


class ParentWatchdogTests(unittest.TestCase):
    def test_simulator_exits_when_parent_is_killed(self):
        child_code = (
            "import os, time; from parent_watchdog import watch_parent; "
            "watch_parent(); print(os.getpid(), flush=True); time.sleep(60)"
        )
        parent_code = (
            "import os, subprocess, sys, time; "
            f"subprocess.Popen([sys.executable, '-c', {child_code!r}], "
            "env={**os.environ, 'SIM_PARENT_PID': str(os.getpid())}); "
            "time.sleep(60)"
        )
        parent = subprocess.Popen(
            [sys.executable, "-c", parent_code], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        child_pid = None
        try:
            child_pid = int(parent.stdout.readline().strip())
            parent.kill()  # SIGKILL bypasses all Flask/Python cleanup handlers.
            # The child inherits these pipes: EOF means it also exited.
            parent.communicate(timeout=5)
            self.assertEqual(parent.returncode, -signal.SIGKILL)
        finally:
            if parent.poll() is None:
                parent.kill()
            if child_pid is not None:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            parent.communicate(timeout=5)

    def test_manual_simulator_does_not_start_watchdog(self):
        result = subprocess.run(
            [sys.executable, "-c",
             "import threading; from parent_watchdog import watch_parent; "
             "watch_parent(); assert threading.active_count() == 1"],
            cwd=ROOT, env={k: v for k, v in os.environ.items() if k != "SIM_PARENT_PID"},
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
