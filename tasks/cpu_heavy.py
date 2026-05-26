import hashlib
import time

start = time.time()

payload = b"distributed-task-orchestrator-capstone" * 8192
digest = payload

rounds = 18_000_000

for _ in range(rounds):
    digest = hashlib.sha256(digest).digest()

duration = time.time() - start

print(f"CPU heavy benchmark completed in {duration:.2f} seconds")