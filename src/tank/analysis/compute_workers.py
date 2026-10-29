"""Full Market Truth computations in dedicated worker processes (02 §8, 03 §3.8).

compute_market_truth is seconds to minutes of pure Python. In a thread of the service
process it shared one GIL with the event loop, every other worker thread and every other
full computation, so a RUN_MARKET_TRUTH waited behind reaction checkpoints and the whole
service stalled while it ran. Each lane is one spawned worker process: ASSESSMENT runs
RUN_MARKET_TRUTH and keeps each token's baseline cache between assessments, REACTION runs
reaction checkpoints, so an assessment never queues behind a checkpoint.

The snapshot is pickled in slices in a thread, so the event loop keeps running while it
is serialized. A worker that dies fails only the call it was running; the next call
starts a new worker.
"""

import asyncio
import multiprocessing
import pickle
from concurrent.futures import Executor, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

ASSESSMENT = "ASSESSMENT"
REACTION = "REACTION"
LANES = (ASSESSMENT, REACTION)
SLICE_ROWS = 5000

_executors: dict[str, Executor] = {}
_baselines: dict[str, dict] = {}


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


async def compute_market_truth(lane: str, snapshot: dict, *, config: dict, baseline_key: str | None = None):
    """compute_market_truth(snapshot) in the lane's worker process. baseline_key names the
    baseline cache kept in that worker between calls (03 §5.7 history reuse); None uses a
    fresh cache, as compute_market_truth does without one."""
    parts = await asyncio.to_thread(pack, snapshot)
    pool = executor(lane)
    try:
        return await asyncio.wrap_future(pool.submit(_compute, parts, config, baseline_key))
    except BrokenProcessPool:
        if _executors.get(lane) is pool:
            del _executors[lane]
            pool.shutdown(wait=False, cancel_futures=True)
        raise
