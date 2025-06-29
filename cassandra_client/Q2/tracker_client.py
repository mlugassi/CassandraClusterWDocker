from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from uuid import UUID
from datetime import datetime, timezone
from time import sleep
import os

print("Starting Tracker Client...")

bird_ids_path = "bird_ids.txt"
bird_ids = []

if os.path.exists(bird_ids_path):
    print("Loading bird IDs from file...")
    with open(bird_ids_path, "r") as f:
        bird_ids = [UUID(line.strip()) for line in f.readlines()]
else:
    print("No bird_ids.txt found, using fallback IDs.")
    bird_ids = [
        UUID("f7dc0d88-c8fc-49af-8c2f-9e852cf9e18d"),
        UUID("6e6442ea-8705-4271-8a9f-e4077bc43fd2")
    ]

cluster = Cluster(['cassandra-1', 'cassandra-2'])
session = cluster.connect('bird_tracking')

log_path = "tracker_log.txt"
log_file = open(log_path, "a")

while True:
    now = datetime.now(timezone.utc)
    print(f"Querying birds at {now.isoformat()}")

    for bird_id in bird_ids:
        stmt = SimpleStatement("""
            SELECT timestamp, latitude, longitude
            FROM bird_locations
            WHERE bird_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        try:
            result = session.execute(stmt, (bird_id,), trace=True)
            row = result.one()

            if row:
                print(f"Bird {bird_id} => {row.timestamp} | {row.latitude}, {row.longitude}")
                log_file.write(f"[{now}] Bird {bird_id}: {row.latitude}, {row.longitude}\n")
            else:
                print(f"Bird {bird_id} - No data found")
                log_file.write(f"[{now}] Bird {bird_id}: No data\n")

            trace = result.get_query_trace()
            print(f"Select trace for bird {bird_id}")
            print(f"Coordinator: {trace.coordinator}")
            for event in trace.events:
                print(f"{event.source_elapsed} µs | {event.description} | from {event.source}")

        except Exception as e:
            print(f"Error querying bird {bird_id}: {e}")

    log_file.flush()
    sleep(10)
