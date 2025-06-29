import uuid
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import random
import time

# Connect to Cassandra
cluster = Cluster(['cassandra-1', 'cassandra-2'])
session = cluster.connect('bird_tracking')

bird_id = uuid.UUID("e7f4af4f4fd54646810d3a5bd7abc81e") 
print(f"Selected bird ID: {bird_id}")

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

# המתן מעט כדי להפריד את השאילתות
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