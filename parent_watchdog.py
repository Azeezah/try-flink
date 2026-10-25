"""Stop a panel-launched simulator when its Flask parent disappears."""

import os
import threading
import time


def watch_parent():
    parent_pid = os.environ.get("SIM_PARENT_PID")
    if parent_pid is None:
        return  # Manually launched simulations have an independent lifetime.
    parent_pid = int(parent_pid)

    def monitor():
        while os.getppid() == parent_pid:
            time.sleep(0.25)
        # Exit even if the main thread is blocked in Kafka or paused.
        os._exit(1)

    threading.Thread(target=monitor, name="parent-watchdog", daemon=True).start()
