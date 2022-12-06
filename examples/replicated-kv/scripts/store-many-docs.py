import threading
import time

import requests


def upload():
    session = requests.session()
    for i in range(100_000):
        r = session.post(f"http://127.0.0.1:8000/my-keyspace/{i}", data=b'{"name": "tod"}')
        r.raise_for_status()


start = time.perf_counter()
handles = []
# Let's add a million docs to the store
# This is sort of the worst case scenario as we're not making use of the bulk apis.
# We're using 10 threads here to give us a bit of concurrency.
for _ in range(10):
    t = threading.Thread(target=upload)
    t.start()

    handles.append(t)

for handle in handles:
    handle.join()

total_time = time.perf_counter() - start
print(f"Took {total_time:.2f} seconds!")
