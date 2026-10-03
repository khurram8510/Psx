import math
import random

from kmi30.models import Bar

from .conftest import pkt

START = int(pkt(2026, 9, 14, 10, 0).timestamp())  # Monday, after warm-up


def bars_from_closes(closes, start=START):
    return [Bar(start + 60 * i, c, c, c, c) for i, c in enumerate(closes)]


def random_walk(n, sigma=0.0006, seed=1, start=260_000.0, drift=0.0):
    rng = random.Random(seed)
    out, v = [], start
    for _ in range(n):
        v *= math.exp(drift + rng.gauss(0, sigma))
        out.append(v)
    return out
