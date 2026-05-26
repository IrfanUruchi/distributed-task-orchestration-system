numbers = list(range(1, 500000))
result = sum(x * 2 for x in numbers)

print(f"Data processing task completed. Processed {len(numbers)} items. Result: {result}")