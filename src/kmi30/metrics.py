from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry(auto_describe=True)

POLLS = Counter("kmi30_polls_total", "PSX polls by result", ["result"], registry=REGISTRY)
POLL_LATENCY = Histogram("kmi30_poll_seconds", "PSX poll latency", registry=REGISTRY,
                         buckets=(0.25, 0.5, 1, 2, 5, 10, 20))
VALUE = Gauge("kmi30_index_value", "Latest KMI-30 value", registry=REGISTRY)
CHANGE_PCT = Gauge("kmi30_change_pct", "Change vs previous close, percent", registry=REGISTRY)
TICK_AGE = Gauge("kmi30_last_tick_age_seconds", "Seconds since the newest tick", registry=REGISTRY)
SIGMA = Gauge("kmi30_sigma_1m", "EWMA one-minute volatility (log return)", registry=REGISTRY)
ALERTS = Counter("kmi30_alerts_total", "Alerts raised", ["detector", "severity"], registry=REGISTRY)
SLACK = Counter("kmi30_slack_deliveries_total", "Slack deliveries by result", ["result"], registry=REGISTRY)
MARKET_OPEN = Gauge("kmi30_market_open", "1 while a PSX session is open", registry=REGISTRY)
