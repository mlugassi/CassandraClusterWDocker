import threading
import queue
import time

def worker(q, worker_id):
    while True:
        task = q.get()
        if task is None:
            break
        print(f"Worker {worker_id} processing task {task}")
        time.sleep(1)  # Simulate task processing time
        q.task_done()

task_queue = queue.Queue()
tasks = [f"Task-{i}" for i in range(5)]
for task in tasks:
    task_queue.put(task)

# Start multiple worker threads
num_workers = 3
threads = []
for i in range(num_workers):
    t = threading.Thread(target=worker, args=(task_queue, i))
    t.start()
    threads.append(t)

# Wait for all tasks to be processed
task_queue.join()

# Stop workers
for _ in range(num_workers):
    task_queue.put(None)
for t in threads:
    t.join()
