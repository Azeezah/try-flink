import csv
import json
import os
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from kafka import KafkaProducer

"""
Design:
The point of this script is to simulate the traffic at a rate that mimicks production load and timing.

The fake event data is already generated with timestamps, so this code simply forwards it at a rate to correspond with those timestamps.

It is scaled down though because we're replaying a full 24 hours of traffic in just a few minutes.  Additionally, the servers are running locally in docker on a single laptop, so the traffic is scaled to match.
"""

# Connect to our local Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:19092'],
    api_version=(2, 0, 2),    
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'youtube-clicks'
CSV_FILE = 'youtube-clicks.csv'
CONTROL_FILE = os.environ.get('SIM_CONTROL_FILE', '.simulation-control.json')
METRICS_FILE = os.environ.get('SIM_METRICS_FILE', '.traffic-metrics.jsonl')
DEFAULT_SPEED = float(os.environ.get('SIM_SPEED', '120'))

metrics_lock = threading.Lock()
metrics = {'expected': 0, 'attempted': 0, 'acked': 0, 'errors': 0}
metrics_stop = threading.Event()


def record_metric(name):
    with metrics_lock:
        metrics[name] += 1


def set_expected_rate(value):
    with metrics_lock:
        metrics['expected'] = value


def on_delivery(_metadata):
    record_metric('acked')


def on_delivery_error(_error):
    record_metric('errors')


def write_metrics():
    """Write one aggregate sample per second so the dashboard stays cheap."""
    with open(METRICS_FILE, 'w') as metrics_file:
        while not metrics_stop.wait(1):
            with metrics_lock:
                sample = dict(metrics)
                metrics.update(attempted=0, acked=0, errors=0)
            sample['wall_time'] = datetime.now(timezone.utc).isoformat()
            metrics_file.write(json.dumps(sample) + '\n')
            metrics_file.flush()


def read_control():
    """Returns a tuple: (paused, speed)"""
    try:
        with open(CONTROL_FILE, 'r') as control_file:
            value = json.load(control_file)
        return bool(value.get('paused', False)), max(float(value.get('speed', DEFAULT_SPEED)), 0.01)
    except (FileNotFoundError, json.JSONDecodeError, TypeError, ValueError):
        return False, DEFAULT_SPEED

print("Starting throttled real-time stream simulation...", flush=True)
metrics_writer = threading.Thread(target=write_metrics, daemon=True)
metrics_writer.start()

with open(CSV_FILE, 'r') as source_file:
    source_reader = csv.reader(source_file)
    next(source_reader)
    expected_events_per_second = Counter(row[1] for row in source_reader)

with open(CSV_FILE, 'r') as f:
    reader = csv.reader(f)
    header = next(reader) # Skip header
    
    first_row = next(reader)
    # Parse the very first event's virtual time
    last_virtual_time = datetime.strptime(first_row[1], "%Y-%m-%d %H:%M:%S")
    
    # Send the first event
    first_future = producer.send(TOPIC, {"videoid": first_row[0], "timestamp": first_row[1]})
    first_future.add_callback(on_delivery)
    first_future.add_errback(on_delivery_error)
    record_metric('attempted')
    set_expected_rate(expected_events_per_second[first_row[1]] * DEFAULT_SPEED)
    
    last_real_time = time.time()
    
    for row in reader:
        vid, ts_str = row[0], row[1]
        current_virtual_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        virtual_time_diff = (current_virtual_time - last_virtual_time).total_seconds()
        
        # Pause and speed are read between events, so the UI can change them live.
        paused, speed = read_control()
        while paused:
            time.sleep(0.1)
            paused, speed = read_control()

        # Recheck the target delay during the wait so speed changes apply live.
        while True:
            paused, speed = read_control()
            if paused:
                time.sleep(0.1)
                last_real_time = time.time()
                continue
            elapsed_real_time = time.time() - last_real_time
            sleep_duration = (virtual_time_diff / speed) - elapsed_real_time
            if sleep_duration <= 0:
                break
            time.sleep(min(sleep_duration, 0.1))
            
        # Emit the event to Kafka
        future = producer.send(TOPIC, {"videoid": vid, "timestamp": ts_str})
        future.add_callback(on_delivery)
        future.add_errback(on_delivery_error)
        record_metric('attempted')
        set_expected_rate(expected_events_per_second[ts_str] * speed)
        
        # Keep track of clocks for the next iteration
        last_virtual_time = current_virtual_time
        last_real_time = time.time()

producer.flush()
metrics_stop.set()
metrics_writer.join(timeout=2)
print("Stream simulation complete.", flush=True)

