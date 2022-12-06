import time
import random

import requests



start = time.perf_counter()
session = requests.session()
for i in range(100_000):
    if (i / 1_000).is_integer():
        print(f"Added {i} docs")

    r = session.post(f"http://127.0.0.1:8000/my-keyspace/{random.randint(1, 12345678791231)}", data=b'{"name": "tod"}')
    r.raise_for_status()

total_time = time.perf_counter() - start
print(f"Took {total_time:.2f} seconds!")
