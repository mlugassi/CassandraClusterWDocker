import uuid
import time
import random
import argparse
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# --- Parse command-line argument ---
parser = argparse.ArgumentParser(description="Trace update and select for a specific bird.")
parser.add_argument('--bird_id', type=str, required=True, help='UUID of the bird to trace')
args = parser.parse_args()

try:
    bird_id = uuid.UUID(args.bird_id)
except ValueError:
    print("❌ Invalid bird UUID format.")
    exit(1)

print(f"Selected bird ID: {bird_id}")

# --- Connect to Cassandra ---
cluster = Cluster(['cassandra-1', 'cassandra-2'])
session = cluster.connect('bird_tracking')

# --- UPDATE with trace ---
print("\nRunning update with trace...")
update_stmt = SimpleStatement("""
    UPDATE bird_locations 
    SET latitude = %s, longitude = %s 
    WHERE bird_id = %s AND timestamp = toTimestamp(now())
""")
update_result = session.execute(update_stmt, [random.uniform(-90, 90), random.uniform(-180, 180), bird_id], trace=True)
update_trace = update_result.get_query_trace()

print("\nUPDATE TRACE EVENTS:")
if update_trace:
    for event in update_trace.events:
        print(f"{event.source} | {event.source_elapsed} µs | {event.description}")
else:
    print("No update trace available.")

# --- Delay between queries ---
time.sleep(2)

# --- SELECT with trace ---
print("\nRunning select with trace...")
select_stmt = SimpleStatement("""
    SELECT * FROM bird_locations WHERE bird_id = %s
""")
select_result = session.execute(select_stmt, [bird_id], trace=True)
select_trace = select_result.get_query_trace()

print("\nSELECT TRACE EVENTS:")
if select_trace:
    for event in select_trace.events:
        print(f"{event.source} | {event.source_elapsed} µs | {event.description}")
else:
    print("No select trace available.")

cluster.shutdown()
