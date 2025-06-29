from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime, timezone
import time
import uuid
import os

print("Starting Bird Client...")

cluster = Cluster(['cassandra-1', 'cassandra-2'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS bird_tracking
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
""")
session.set_keyspace('bird_tracking')

session.execute("""
    CREATE TABLE IF NOT EXISTS bird_locations (
        bird_id UUID,
        species TEXT,
        timestamp TIMESTAMP,
        latitude DOUBLE,
        longitude DOUBLE,
        PRIMARY KEY ((bird_id), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
""")

bird_ids_path = "bird_ids.txt"
if os.path.exists(bird_ids_path):
    print("Loading bird IDs from file...")
    with open(bird_ids_path, "r") as f:
        bird_ids = [uuid.UUID(line.strip()) for line in f.readlines()]
else:
    print("Generating new bird IDs...")
    bird_ids = [uuid.uuid4() for _ in range(10)]
    with open(bird_ids_path, "w") as f:
        for bid in bird_ids:
            f.write(str(bid) + "\n")

insert_stmt = session.prepare("""
    INSERT INTO bird_locations (bird_id, species, timestamp, latitude, longitude)
    VALUES (?, ?, ?, ?, ?)
""")

now = datetime.now(timezone.utc)
species = "swallow"

for bird_id in bird_ids:
    lat, lon = 32.0, 35.0
    ts = now
    result = session.execute(insert_stmt, (bird_id, species, ts, lat, lon), trace=True)
    trace = result.get_query_trace()
    print(f"Insert trace for bird {bird_id}")
    print(f"Coordinator: {trace.coordinator}")
    for event in trace.events:
        print(f"{event.source_elapsed} µs | {event.description} | from {event.source}")

for i in range(20):
    for bird_id in bird_ids:
        lat, lon = 32.0 + i * 0.01, 35.0 + i * 0.01
        ts = datetime.now(timezone.utc)
        session.execute(insert_stmt, (bird_id, species, ts, lat, lon), trace=(i == 0))
    time.sleep(0.01)

print("Bird Client finished.")
