"""Market reads and their Market Truth computations in worker processes (02 §8, 03 §3.8).

A condition read, an assessment and a reaction checkpoint are each a market read of up to
90 days followed by a computation: seconds to minutes of Python that needs nothing of the
service's event loop. In the service process they shared one GIL with the job workers,
agent sessions, sources and delivery, so the whole service ran on one core however many
the host has, and the event loop stalled for tens of seconds while a read hashed its
inputs. Each lane is a fixed set of spawned worker processes with their own PostgreSQL
pool, ClickHouse client and MarketService caches. The service process sends a request,
awaits the result and keeps every write.

A token always goes to the same worker of its lane, so its 90-day caches exist once per
lane and stay warm. A worker serves one request at a time: a computation never starves
another request's ClickHouse socket. A cancelled request is cancelled in its worker and
holds the worker until it has stopped there, so the caches it filled survive and the next
request does not run beside it. A worker that dies fails the request it was serving; the
next request starts a new one.
"""

import asyncio
import logging
import multiprocessing
import os
import pickle
import threading
import time
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from typing import Any

log = logging.getLogger(__name__)

CONDITION = "CONDITION"
ASSESSMENT = "ASSESSMENT"
REACTION = "REACTION"
LANE_WORKERS = {CONDITION: 4, ASSESSMENT: 2, REACTION: 1}
WORKER_CONNECTIONS = 4
UPKEEP_S = 60


class MarketWorkerLost(RuntimeError):
    """The worker process ended, or its pool closed, before it answered the request."""


class MarketReader:
    """What a worker process serves: the market reads and computations of the service,
    over this process's own MarketTruthService, MarketService caches and baseline caches."""

    def __init__(self, truth, name):
        self.truth, self.market, self.settings, self.name = truth, truth.market, truth.settings, name
        self.baselines = {}
        self.upkeep = []

    async def condition_values(self, event_id, token_id, cutoff, requirements):
        return await self.truth.condition_values(event_id, token_id, cutoff, requirements)

    async def assessment(self, event_id, token_id, cutoff, capture_waits):
        """An assessment's Market Truth without its identity; the service process stores it.

        capture_waits are the bounded capture waits the service process recorded for the
        token, which the snapshot reports."""
        from tank.analysis.truth import compute_market_truth

        started = time.monotonic()
        self.market.record_capture_waits(capture_waits)
        snapshot, knobs = await self.truth.assessment_inputs(event_id, token_id, cutoff)
        read = time.monotonic()
        held = self.baselines.setdefault(token_id, {"cache": {}})
        try:
            return compute_market_truth(snapshot, config=knobs, baseline_cache=held["cache"])
        finally:
            held["used"] = time.monotonic()
            log.info(
                "assessment %s %s: read %.1fs, compute %.1fs",
                event_id,
                token_id,
                read - started,
                held["used"] - read,
            )

    async def reaction(self, event_id, token_id, end_stamp, anchor):
        from tank.analysis.truth import compute_market_truth, reaction_inputs

        started = time.monotonic()
        snapshot, knobs = await reaction_inputs(
            self.market, self.settings, event_id, token_id, end_stamp, anchor
        )
        read = time.monotonic()
        try:
            return compute_market_truth(snapshot, config=knobs)
        finally:
            log.info(
                "reaction %s %s: read %.1fs, compute %.1fs",
                event_id,
                token_id,
                read - started,
                time.monotonic() - read,
            )

    async def release_idle_baselines(self):
        """A token without an assessment for ASSESSMENT_BASELINE_IDLE_S gives its baselines
        back: their entries are keyed by history heads that move every minute."""
        from tank.analysis.truth import ASSESSMENT_BASELINE_IDLE_S

        while True:
            await asyncio.sleep(UPKEEP_S)
            now = time.monotonic()
            for token_id, held in list(self.baselines.items()):
                if now - held.get("used", now) > ASSESSMENT_BASELINE_IDLE_S:
                    del self.baselines[token_id]


async def open_reader(settings, name):
    """This worker process's own stores and reader, over the service generation's settings."""
    from tank import clickhouse_market
    from tank.analysis.truth import MarketTruthService
    from tank.logging import configure_logging
    from tank.market.service import MarketService
    from tank.storage import PostgresRepository

    configure_logging(settings)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter(f"%(asctime)s %(levelname)s %(name)s [{name}] %(message)s"))
    repo = PostgresRepository(settings.database_url, connections=WORKER_CONNECTIONS)
    clickhouse_market.install(repo, settings)
    await repo.open()
    reader = MarketReader(MarketTruthService(repo, settings, MarketService(repo, settings)), name)
    reader.upkeep = [
        asyncio.create_task(reader.market.follow_day_counts()),
        asyncio.create_task(reader.release_idle_baselines()),
    ]
    return reader


def _portable(exc: BaseException, name: str) -> BaseException:
    """exc as the service process can raise it: itself when it survives pickling, so its
    class names the job's failure and its retry_after still schedules the retry."""
    if isinstance(exc, asyncio.CancelledError):
        return MarketWorkerLost(f"{name} stopped the request")
    try:
        pickle.loads(pickle.dumps(exc))
    except Exception:
        return RuntimeError(f"{type(exc).__name__} in {name}")
    return exc


def _outcome(number: int, task: asyncio.Task, name: str) -> bytes:
    failure = asyncio.CancelledError() if task.cancelled() else task.exception()
    if failure is None:
        reply: tuple[int, bool, Any] = (number, True, task.result())
    else:
        if not task.cancelled():
            log.error("%s request failed", name, exc_info=failure)
        reply = (number, False, _portable(failure, name))
    try:
        return pickle.dumps(reply, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as exc:
        return pickle.dumps((number, False, RuntimeError(f"{type(exc).__name__} in {name}")))


async def _serve(requests, replies, opener, settings, name):
    loop = asyncio.get_running_loop()
    inbox = asyncio.Queue()

    def receive():
        while True:
            try:
                request = requests.recv()
            except (EOFError, OSError):
                os._exit(0)
            loop.call_soon_threadsafe(inbox.put_nowait, request)

    threading.Thread(target=receive, name="market-worker-requests", daemon=True).start()
    served = await opener(settings, name)
    serving = {}

    def answer(number, task):
        del serving[number]
        replies.send_bytes(_outcome(number, task, name))

    while True:
        number, method, arguments = await inbox.get()
        if method is None:
            if number in serving:
                serving[number].cancel()
            continue
        task = serving[number] = loop.create_task(getattr(served, method)(*arguments))
        task.add_done_callback(lambda task, number=number: answer(number, task))


def serve(requests, replies, opener, settings, name):
    """A worker process: the requests of the service process, until it closes the pipe or
    dies. opener(settings, name) gives the object whose coroutine methods are served."""
    asyncio.run(_serve(requests, replies, opener, settings, name))


class _Worker:
    """The service process's end of one worker process."""

    def __init__(self, settings, name: str, opener: Callable):
        self.settings, self.name, self.opener = settings, name, opener
        self.turn = asyncio.Lock()
        self.process: Any = None
        self.requests: Any = None
        self.waiting: dict[int, asyncio.Future] = {}
        self.sent = 0
        self.closed = False

    def start(self):
        """A spawned process (a fork would copy the service's threads and its caches)."""
        context = multiprocessing.get_context("spawn")
        requests, send_requests = context.Pipe(duplex=False)
        receive_replies, replies = context.Pipe(duplex=False)
        process = context.Process(
            target=serve,
            args=(requests, replies, self.opener, self.settings, self.name),
            name=self.name,
            daemon=True,
        )
        process.start()
        requests.close()
        replies.close()
        self.process, self.requests = process, send_requests
        threading.Thread(
            target=self._receive,
            args=(asyncio.get_running_loop(), process, receive_replies),
            name=self.name + "-replies",
            daemon=True,
        ).start()
        log.info("%s started as process %s", self.name, process.pid)

    def _receive(self, loop, process, replies):
        """Replies are unpickled in this thread; their pipe ends when the process does."""
        while True:
            try:
                reply = pickle.loads(replies.recv_bytes())
            except Exception:
                reply = None
            try:
                loop.call_soon_threadsafe(self._deliver, process, reply)
            except RuntimeError:
                reply = None
            if reply is None:
                replies.close()
                process.kill()
                process.join()
                if not self.closed:
                    log.warning("%s ended with exit code %s", self.name, process.exitcode)
                return

    def _deliver(self, process, reply):
        if reply is None:
            if self.process is process:
                self._forget()
            return
        number, done, value = reply
        future = self.waiting.pop(number, None)
        if future is not None and not future.done():
            future.set_result((done, value))

    def _forget(self):
        requests, self.process, self.requests = self.requests, None, None
        if requests is not None:
            requests.close()
        waiting, self.waiting = self.waiting, {}
        for future in waiting.values():
            if not future.done():
                future.set_exception(MarketWorkerLost(f"{self.name} ended before it answered"))

    async def call(self, method: str, *arguments):
        await self.turn.acquire()
        release = True
        try:
            if self.closed:
                raise MarketWorkerLost(f"{self.name} is closed")
            if self.process is None:
                self.start()
            self.sent += 1
            number = self.sent
            answer = self.waiting[number] = asyncio.get_running_loop().create_future()
            try:
                self.requests.send((number, method, arguments))
            except OSError as exc:
                self.waiting.pop(number, None)
                raise MarketWorkerLost(f"{self.name} ended before the request") from exc
            try:
                done, value = await asyncio.shield(answer)
            except asyncio.CancelledError:
                if not answer.done() and self.requests is not None:
                    with suppress(OSError):
                        self.requests.send((number, None, ()))

                    def stopped(future):
                        future.cancelled() or future.exception()
                        self.turn.release()

                    answer.add_done_callback(stopped)
                    release = False
                raise
        finally:
            if release:
                self.turn.release()
        if done:
            return value
        raise value

    def close(self):
        self.closed = True
        process = self.process
        self._forget()
        if process is not None:
            process.kill()


class MarketWorkers:
    """The worker processes of one service generation, started when first used."""

    def __init__(self, settings, *, opener: Callable = open_reader, lanes: dict[str, int] | None = None):
        self._workers = {
            lane: [_Worker(settings, f"market-worker-{lane.lower()}-{n}", opener) for n in range(count)]
            for lane, count in (lanes or LANE_WORKERS).items()
        }
        self._homes: dict[str, dict[str, int]] = {lane: {} for lane in self._workers}

    def worker(self, lane: str, token_id: str) -> _Worker:
        """The token's worker in the lane: the one with the fewest tokens when the token
        is first seen, and the same one for as long as the generation lives."""
        homes = self._homes[lane]
        if token_id not in homes:
            held = Counter(homes.values())
            homes[token_id] = min(range(len(self._workers[lane])), key=lambda n: (held[n], n))
        return self._workers[lane][homes[token_id]]

    async def condition_values(self, event_id, token_id, cutoff, requirements):
        return await self.worker(CONDITION, token_id).call(
            "condition_values", event_id, token_id, cutoff, requirements
        )

    async def assessment(self, event_id, token_id, cutoff, capture_waits):
        return await self.worker(ASSESSMENT, token_id).call(
            "assessment", event_id, token_id, cutoff, capture_waits
        )

    async def reaction(self, event_id, token_id, end_stamp, anchor):
        return await self.worker(REACTION, token_id).call("reaction", event_id, token_id, end_stamp, anchor)

    def close(self):
        for workers in self._workers.values():
            for worker in workers:
                worker.close()
