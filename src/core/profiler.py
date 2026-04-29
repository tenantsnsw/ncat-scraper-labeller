import os
import time
import tracemalloc
from contextlib import contextmanager

import psutil
from loguru import logger


@contextmanager
def profile_resources(name: str):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024**2
    cpu_before = process.cpu_times()
    t0 = time.monotonic()
    tracemalloc.start()
    yield
    elapsed = time.monotonic() - t0
    mem_after = process.memory_info().rss / 1024**2
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    cpu_after = process.cpu_times()
    cpu_secs = (
        (cpu_after.user - cpu_before.user)
        + (cpu_after.system - cpu_before.system)
    )
    logger.bind(script="profiler").info(
        f"{name} | RSS {mem_before:.0f}→{mem_after:.0f} MB "
        f"(+{mem_after - mem_before:.0f}) | peak alloc {peak / 1024**2:.0f} MB"
        f" | {elapsed:.0f}s elapsed | CPU {cpu_secs:.0f}s"
    )
