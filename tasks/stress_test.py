import time

start = time.time()
total = 0

while time.time() - start < 5:
    for i in range(100000):
        total += i

print(f"Stress test completed after 5 seconds. Checksum: {total}")