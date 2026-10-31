import argparse
import statistics
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description="Benchmark the synthetic lookup API.")
parser.add_argument("--url", default="http://127.0.0.1:8000/search?account_id=TEST100001")
parser.add_argument("--requests", type=int, default=500)
args = parser.parse_args()

samples = []
failures = 0
for _ in range(args.requests):
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(args.url, timeout=10) as response:
            response.read()
            if response.status != 200:
                failures += 1
            else:
                samples.append((time.perf_counter() - start) * 1000)
    except (urllib.error.URLError, TimeoutError):
        failures += 1

if samples:
    ordered = sorted(samples)
    p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
    print(f"Successful: {len(samples)}")
    print(f"Failed: {failures}")
    print(f"Mean: {statistics.mean(samples):.3f} ms")
    print(f"Median: {statistics.median(samples):.3f} ms")
    print(f"P95: {p95:.3f} ms")
    print(f"Min: {min(samples):.3f} ms")
    print(f"Max: {max(samples):.3f} ms")
else:
    print("No successful requests.")
    print(f"Failed: {failures}")
