"""The single Tank asyncio process and its durable worker pool."""
import asyncio
import logging
from tank.delivery import DeliveryService
from tank.scheduler import Scheduler
from tank.util import parse_time, utcnow

log = logging.getLogger(__name__)
TIMEOUTS = {'FILTER_ITEM': 10, 'ROUTE_ITEM': 20, 'AGENT_MESSAGE': 10, 'ESCALATE_TOKENS': 10, 'RUN_MARKET_TRUTH': 90, 'DELIVER': 10, 'EVAL_CONDITIONS': 20, 'MILESTONE_CHECK': 90, 'REACTION_CAPTURE': 120, 'SCOUT_REVIEW': 300}

class Runtime:
    def __init__(self, repo, settings, *, sources=None, market=None, truth=None, pipeline=None, sessions=None, event_tools=None, delivery=None, scheduler=None):
        self.repo, self.settings = repo, settings
        if sources is None:
            from tank.sources.service import SourceService
            sources = SourceService(repo, settings)
        if market is None:
            from tank.market.service import MarketService
            market = MarketService(repo, settings)
        if truth is None:
            from tank.analysis.truth import MarketTruthService
            truth = MarketTruthService(repo, settings, market)
        if pipeline is None:
            from tank.agents.pipeline import Pipeline
            pipeline = Pipeline(repo, settings)
        if event_tools is None:
            from tank.analysis.decisions import DecisionEngine
            from tank.event_tools import EventToolService
            event_tools = EventToolService(repo, settings, truth, DecisionEngine(settings))
        if sessions is None:
            from tank.agents.sessions import SessionService
            sessions = SessionService(repo, settings, event_tools.dispatch)
        self.sources, self.market, self.truth = sources, market, truth
        self.pipeline, self.sessions, self.event_tools = pipeline, sessions, event_tools
        self.delivery = delivery or DeliveryService(repo, settings)
        self.scheduler = scheduler or Scheduler(repo, settings, market, truth)
        self.handlers = {'FILTER_ITEM': pipeline.filter_item, 'ROUTE_ITEM': pipeline.route_item,
                         'AGENT_MESSAGE': sessions.send_message, 'ESCALATE_TOKENS': self.escalate,
                         'RUN_MARKET_TRUTH': self.assess, 'DELIVER': self.delivery.deliver,
                         'EVAL_CONDITIONS': self.scheduler.eval_conditions,
                         'MILESTONE_CHECK': self.scheduler.milestone_check,
                         'REACTION_CAPTURE': self.scheduler.reaction_capture,
                         'SCOUT_REVIEW': self.scout_review}

    def validate(self):
        if self.settings.workers != 8:
            raise ValueError('Tank production requires exactly eight workers')
        for name in ('sources', 'registry', 'semantics', 'dependency_map', 'thresholds'):
            self.settings.load_config(name)
        required = ('database_url', 'openma_url', 'openma_model', 'telegram_token', 'telegram_alerts_chat', 'telegram_ops_chat')
        missing = [name for name in required if not getattr(self.settings, name, None)]
        if missing:
            raise ValueError('Missing required service settings: ' + ', '.join(missing))
        thresholds = self.settings.load_config('thresholds')
        if thresholds.get('filter_cutoff') is None or thresholds.get('routing_cutoff') is None:
            raise ValueError('Filter and routing cutoffs require calibration before live service startup')

    async def escalate(self, payload):
        tokens = payload.get('tokens') or ([payload['token_id']] if payload.get('token_id') else None)
        if not tokens:
            raise ValueError('ESCALATE_TOKENS requires tokens')
        await self.market.escalate(payload['event_id'], tokens)

    async def assess(self, payload):
        return await self.truth.assess(payload['event_id'], payload['token_id'], payload.get('cutoff'))

    async def scout_review(self, payload):
        if not hasattr(self.sessions, 'scout_review'):
            raise RuntimeError('SessionService.scout_review is required for SCOUT_REVIEW jobs')
        return await self.sessions.scout_review(payload)

    async def worker(self, stop, number=0):
        while not stop.is_set():
            try:
                job = await self.repo.claim_job()
            except Exception:
                log.exception('Worker %s failed to claim a job', number)
                await self.pause(stop, 1)
                continue
            if job is None:
                await self.pause(stop, .25)
                continue
            try:
                if job.get('expires_at') and parse_time(job['expires_at']) <= utcnow():
                    # claim_job atomically expires queued jobs; this also protects custom repositories.
                    await self.repo.put('ops.jobs', job['job_id'], {**job, 'status': 'EXPIRED'})
                    continue
                kind = job['type']
                if kind not in self.handlers:
                    raise ValueError(f'Unregistered durable job kind: {kind}')
                async with asyncio.timeout(TIMEOUTS[kind]):
                    await self.handlers[kind](job['payload'])
                await self.repo.finish_job(job['job_id'])
            except asyncio.CancelledError:
                # Remains RUNNING and is recovered on the next process start.
                raise
            except Exception as exc:
                log.error('Job %s (%s) failed: %s', job['job_id'], job['type'], type(exc).__name__)
                try:
                    await self.repo.fail_job(job['job_id'], type(exc).__name__, retry_after=getattr(exc, 'retry_after', min(60, 2 ** job.get('attempts', 1))))
                except Exception:
                    log.exception('Could not persist job failure: %s', job['job_id'])
                    await self.pause(stop, 1)

    @staticmethod
    async def pause(stop, seconds):
        try:
            await asyncio.wait_for(stop.wait(), seconds)
        except TimeoutError:
            pass

    async def supervise(self, name, service, stop):
        while not stop.is_set():
            try:
                await service.run(stop)
                if not stop.is_set():
                    raise RuntimeError('Long-lived service returned unexpectedly')
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('%s loop failed; restarting', name)
                await self.pause(stop, 1)

    async def run(self, stop=None):
        stop = stop or asyncio.Event()
        self.validate()
        await self.repo.recover_jobs()
        tasks = [asyncio.create_task(self.supervise(name, service, stop), name=name) for name, service in [('sources', self.sources), ('market', self.market), ('sessions', self.sessions), ('scheduler', self.scheduler)]]
        tasks += [asyncio.create_task(self.worker(stop, n), name=f'worker-{n}') for n in range(8)]
        try:
            await stop.wait()
        finally:
            stop.set()
            # In-flight jobs get a bounded shutdown window, then remain recoverable.
            done, pending = await asyncio.wait(tasks, timeout=10)
            for task in pending:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for service in (self.sources, self.market, self.sessions, self.pipeline, self.delivery, self.event_tools):
                if hasattr(service, 'close'):
                    try:
                        await service.close()
                    except Exception:
                        log.exception('Resource close failed')
