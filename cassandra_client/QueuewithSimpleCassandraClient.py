import threading
import queue
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Initialize Cassandra connection
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('your_keyspace')

# Create a thread-safe queue
command_queue = queue.Queue()

def worker():
    while True:
        cql_query = command_queue.get()
        if cql_query is None:
            break  # Exit signal
        try:
            statement = SimpleStatement(cql_query)
            session.execute(statement)
        except Exception as e:
            # Handle exceptions (e.g., log or retry)
            print(f"Error executing query: {e}")
        finally:
            command_queue.task_done()

# Start worker thread
thread = threading.Thread(target=worker)
thread.start()

# Enqueue commands
command_queue.put("INSERT INTO users (id, name) VALUES (1, 'Alice')")
command_queue.put("UPDATE users SET name = 'Bob' WHERE id = 1")
command_queue.put("DELETE FROM users WHERE id = 1")

# Wait for all tasks to complete
command_queue.join()

# Stop the worker
command_queue.put(None)
thread.join()

# Close Cassandra connection
session.shutdown()
cluster.shutdown()
