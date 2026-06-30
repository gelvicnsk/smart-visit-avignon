from prometheus_client import Counter, Histogram

# Counter name kept identical to the original for Prometheus query compatibility.
# prometheus_client exposes it as smartvisit_requests_total_total in /metrics output.
REQUEST_COUNTER = Counter(
    "smartvisit_requests_total",
    "Total number of HTTP requests processed by the API",
)

REQUEST_DURATION = Histogram(
    "smartvisit_request_duration_seconds",
    "HTTP request processing duration in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
