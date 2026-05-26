import math
import time

start = time.time()

result = 0

for i in range(1, 5_000_000):
    result += math.sqrt(i)

end = time.time()

print(f"CPU benchmark completed in {end - start:.2f} seconds")
