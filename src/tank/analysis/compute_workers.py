"""Full Market Truth computations in dedicated worker processes (02 §8, 03 §3.8).

compute_market_truth is seconds to minutes of pure Python. In a thread of the service
process it shared one GIL with the event loop, every other worker thread and every other
full computation, so a RUN_MARKET_TRUTH waited behind reaction checkpoints and the whole
service stalled while it ran. Each lane is one spawned worker process: ASSESSMENT runs
RUN_MARKET_TRUTH and keeps each token's baseline cache between assessments, REACTION runs
reaction checkpoints, so an assessment never queues behind a checkpoint. The service
releases a token's baselines in the worker when the token goes idle (03 §5).

The snapshot is pickled in slices in a thread, so the event loop keeps running while it
is serialized. A worker that dies fails only the call it was running; the next call
starts a new worker.
"""

import asyncio
import multiprocessing
import pickle
import time
from concurrent.futures import Executor, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

ASSESSMENT = "ASSESSMENT"
REACTION = "REACTION"
LANES = (ASSESSMENT, REACTION)
SLICE_ROWS = 5000

_executors: dict[str, Executor] = {}
_baselines: dict[str, dict] = {}
_used: dict[tuple[str, str], float] = {}
_active: dict[tuple[str, str], int] = {}


def start_executor(lane: str) -> Executor:
    """One spawned worker process for a lane (a fork would copy the service's threads)."""
    return ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))


def executor(lane: str) -> Executor:
    if lane not in LANES:
        raise ValueError(f"Unknown Market Truth compute lane: {lane}")
    current = _executors.get(lane)
    if current is None:
        current = _executors[lane] = start_executor(lane)
    return current


def pack(snapshot: dict) -> list:
    """The snapshot as pickled parts; long lists are cut into slices of SLICE_ROWS rows."""
    parts: list[tuple[str, bytes | list[bytes]]] = []
    for key, value in snapshot.items():
        if isinstance(value, list) and len(value) > SLICE_ROWS:
            parts.append(
                (
                    key,
                    [
                        pickle.dumps(value[start : start + SLICE_ROWS], protocol=pickle.HIGHEST_PROTOCOL)
                        for start in range(0, len(value), SLICE_ROWS)
                    ],
                )
            )
        else:
            parts.append((key, pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)))
    return parts


def unpack(parts: list) -> dict:
    snapshot = {}
    for key, value in parts:
        if isinstance(value, list):
            rows = []
            for part in value:
                rows.extend(pickle.loads(part))
            snapshot[key] = rows
        else:
            snapshot[key] = pickle.loads(value)
    return snapshot


def _compute(parts: list, config: dict, baseline_key: str | None):
    from tank.analysis import truth

    snapshot = unpack(parts)
    if baseline_key is None:
        return truth.compute_market_truth(snapshot, config=config)
    return truth.compute_market_truth(
        snapshot, config=config, baseline_cache=_baselines.setdefault(baseline_key, {})
    )


def _release(baseline_keys: list[str]) -> None:
    for key in baseline_keys:
        _baselines.pop(key, None)


def release_idle_baselines(lane: str, idle_s: float) -> None:
    """Drop, in the lane's worker after its current computation, the baseline caches whose
    last computation ended more than idle_s ago and that no computation is using. Last use
    is kept per process, so it survives a service generation replaced on SIGHUP. A lane
    without a worker, or whose worker died, holds none to drop."""
    now = time.monotonic()
    idle = [
        key
        for (owner, key), used in list(_used.items())
        if owner == lane and now - used > idle_s and not _active.get((owner, key))
    ]
    for key in idle:
        del _used[(lane, key)]
    pool = _executors.get(lane)
    if not idle or pool is None:
        return
    try:
        pool.submit(_release, idle)
    except BrokenProcessPool:
        if _executors.get(lane) is pool:
            del _executors[lane]
            pool.shutdown(wait=False, cancel_futures=True)


async def compute_market_truth(lane: str, snapshot: dict, *, config: dict, baseline_key: str | None = None):
    """compute_market_truth(snapshot) in the lane's worker process. baseline_key names the
    baseline cache kept in that worker between calls (03 §5.7 history reuse); None uses a
    fresh cache, as compute_market_truth does without one."""
    use = (lane, baseline_key) if baseline_key is not None else None
    if use is not None:
        _active[use] = _active.get(use, 0) + 1
        _used[use] = time.monotonic()
    try:
        parts = await asyncio.to_thread(pack, snapshot)
        pool = executor(lane)
        try:
            return await asyncio.wrap_future(pool.submit(_compute, parts, config, baseline_key))
        except BrokenProcessPool:
            if _executors.get(lane) is pool:
                del _executors[lane]
                pool.shutdown(wait=False, cancel_futures=True)
            raise
    finally:
        if use is not None:
            _used[use] = time.monotonic()
            _active[use] -= 1
            if not _active[use]:
                del _active[use]
