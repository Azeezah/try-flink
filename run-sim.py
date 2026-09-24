import csv
import json
import time
from datetime import datetime
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

print("Starting throttled real-time stream simulation...")

with open(CSV_FILE, 'r') as f:
    reader = csv.reader(f)
    header = next(reader) # Skip header
    
    first_row = next(reader)
    # Parse the very first event's virtual time
    last_virtual_time = datetime.strptime(first_row[1], "%Y-%m-%d %H:%M:%S")
    
    # Send the first event
    producer.send(TOPIC, {"videoid": first_row[0], "timestamp": first_row[1]})
    
    last_real_time = time.time()
    
    for row in reader:
        vid, ts_str = row[0], row[1]
        current_virtual_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        
        # Calculate how many virtual seconds passed between this event and the last
        virtual_time_diff = (current_virtual_time - last_virtual_time).total_seconds()
        
        # --- TIME SCALING FACTOR ---
        # 1.0 = Exact real-time matching (takes 24 hours to finish file)
        # 60.0 = 1 minute of virtual time happens in 1 second of real time (24 hours finishes in 24 mins)
        TIME_SCALE = 120.0 
        
        target_real_delay = virtual_time_diff / TIME_SCALE
        
        # Calculate how much time we actually need to sleep
        elapsed_real_time = time.time() - last_real_time
        sleep_duration = target_real_delay - elapsed_real_time
        
        if sleep_duration > 0:
            time.sleep(sleep_duration)
            
        # Emit the event to Kafka
        producer.send(TOPIC, {"videoid": vid, "timestamp": ts_str})
        
        # Keep track of clocks for the next iteration
        last_virtual_time = current_virtual_time
        last_real_time = time.time()

producer.flush()
print("Stream simulation complete.")

