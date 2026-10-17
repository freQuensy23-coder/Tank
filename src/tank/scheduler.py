"""Durable timer creation, watch evaluation and reaction checkpoints."""
import asyncio
import logging
from datetime import timedelta
from decimal import Decimal
from tank.util import iso, new_id, parse_time, utcnow

log = logging.getLogger(__name__)
CHECKPOINTS = {'PRE_24H': -86400, 'PRE_4H': -14400, 'PRE_1H': -3600, 'POST_15M': 900, 'POST_1H': 3600, 'POST_4H': 14400, 'POST_24H': 86400, 'POST_72H': 259200}

def field(record, path):
    value = record
    for part in path.split('.'):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value

def compare(actual, op, target):
    if actual is None:
        return False
    if op in ('>', '>=', '<', '<='):
        a, b = Decimal(str(actual)), Decimal(str(target))
        return {'>': a > b, '>=': a >= b, '<': a < b, '<=': a <= b}[op]
    if op == '==':
        return actual == target
    if op == '!=':
        return actual != target
    if op == 'in':
        return actual in target
    raise ValueError(f'Unsupported watch operator: {op}')

class Scheduler:
    def __init__(self, repo, settings, market, truth):
        self.repo, self.settings, self.market, self.truth = repo, settings, market, truth

    async def run(self, stop):
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:
                log.exception('Scheduler tick failed')
            try:
                await asyncio.wait_for(stop.wait(), timeout=30)
            except TimeoutError:
                pass

    async def tick(self, now=None):
        now = now or utcnow()
        bucket = iso(now.replace(second=0, microsecond=0))
        await self.repo.enqueue('EVAL_CONDITIONS', {'cutoff': bucket}, bucket, priority=2, expires_at=iso(now+timedelta(minutes=5)), max_attempts=2)
        for event in await self.repo.list('evt.events', limit=100000):
            milestone = event.get('next_milestone')
            if milestone and milestone.get('due_at') and parse_time(milestone['due_at']) <= now:
                await self.repo.enqueue('MILESTONE_CHECK', {'event_id': event['event_id'], 'milestone': milestone}, f"{event['event_id']}:{milestone['due_at']}", priority=1, expires_at=milestone.get('expires_at') or iso(now+timedelta(days=1)))
            assessments = await self.repo.list('evt.ems_assessments', filters={'event_id': event['event_id']}, limit=10000)
            if not assessments:
                continue
            assessment = max(assessments, key=lambda a: a.get('version', 0))
            if assessment.get('class') not in ('MAJOR', 'STRUCTURAL'):
                continue
            anchor = self.anchor(event)
            edges = await self.repo.list('evt.beneficiary_edges', filters={'event_id': event['event_id']}, limit=10000)
            tokens = {'BTC', 'ETH'} | {e['token_id'] for e in edges if e.get('token_id')}
            for token in tokens:
                for label, offset in CHECKPOINTS.items():
                    due = parse_time(anchor['ts']) + timedelta(seconds=max(0, offset))
                    await self.repo.enqueue('REACTION_CAPTURE', {'event_id': event['event_id'], 'token_id': token, 'checkpoint': label, 'anchor': anchor, 'cutoff': iso(parse_time(anchor['ts'])+timedelta(seconds=offset)), 'retro': due < now}, f"{event['event_id']}:{token}:{label}", priority=2, run_after=iso(due), expires_at=iso(max(due, now)+timedelta(hours=6)), max_attempts=5)
        for call in await self.repo.list('ana.calls', filters={'status': 'OPEN'}, limit=100000):
            if parse_time(call['opened_at']) + timedelta(days=7) <= now:
                async with self.repo.transaction():
                    await self.repo.put('ana.calls', call['call_id'], {**call, 'status': 'CLOSED', 'closed_by': 'EXPIRED', 'closed_at': iso(now)})
                    await self.message('CALL_EXPIRED', call['event_id'], call['call_id'], call=call)
        await self.demote(now)
        if now.hour >= 8:
            await self.heartbeat(now)
        # Production storage supplies efficient partition/file retention and backup.
        if hasattr(self.repo, 'maintenance'):
            await self.repo.maintenance(self.settings, now)

    @staticmethod
    def anchor(event):
        if event.get('event_time_prec') == 'EXACT' and event.get('state_verified') and event.get('event_time'):
            return {'type': 'T0', 'ts': event['event_time']}
        if event.get('public_at'):
            return {'type': 'T1', 'ts': event['public_at']}
        return {'type': 'KNOWN_TO_US_AT', 'ts': event['known_to_us_at']}

    async def message(self, kind, event_id, key, **extra):
        await self.repo.enqueue('AGENT_MESSAGE', {'kind': kind, 'event_id': event_id, **extra}, f'{event_id}:{kind}:{key}', priority=1, expires_at=iso(utcnow()+timedelta(days=1)))

    async def eval_conditions(self, payload):
        cutoff = payload.get('cutoff') or iso()
        for cond in await self.repo.list('ana.watch_conditions', filters={'status': 'ARMED'}, limit=100000):
            async with self.repo.lock('condition:' + cond['cond_id']):
                cond = await self.repo.get('ana.watch_conditions', cond['cond_id'])
                if cond['status'] != 'ARMED':
                    continue
                if parse_time(cond['expires_at']) <= parse_time(cutoff):
                    await self.repo.put('ana.watch_conditions', cond['cond_id'], {**cond, 'status': 'EXPIRED'})
                    continue
                expr = cond['expr']
                if 'milestone' in expr:
                    continue
                metric = expr['metric']
                if metric.startswith('event.'):
                    event = await self.repo.get('evt.events', cond['event_id'])
                    value = event.get('event_state') if metric == 'event.state' else event.get('contradiction_fatal')
                else:
                    truth = await self.truth.assess(cond['event_id'], cond['token_id'], cutoff)
                    value = field(truth, metric)
                    if value is None:
                        value = field(truth.get('detail', {}), metric)
                window = expr.get('window', '1m')
                seconds = int(window[:-1]) * {'m': 60, 'h': 3600, 'd': 86400}[window[-1]]
                window_id = int(parse_time(cutoff).timestamp()) // seconds
                if cond.get('last_window') == window_id:
                    continue
                matched = compare(value, expr['op'], expr['value'])
                consecutive = (cond.get('consecutive', 0) if cond.get('last_window') == window_id-1 else 0)+1 if matched else 0
                updated = {**cond, 'consecutive': consecutive, 'last_window': window_id, 'last_value': value, 'last_evaluated_at': cutoff}
                if consecutive >= expr.get('persist', {}).get('count', 1):
                    updated.update(status='FIRED', fired_at=cutoff)
                    async with self.repo.transaction():
                        await self.repo.put('ana.watch_conditions', cond['cond_id'], updated)
                        await self.message('CONDITION_FIRED', cond['event_id'], cond['cond_id'], condition=updated, token_id=cond['token_id'])
                else:
                    await self.repo.put('ana.watch_conditions', cond['cond_id'], updated)

    async def milestone_check(self, payload):
        # Resolution requires official-source evidence, so ask the event session to verify it.
        await self.message('MILESTONE', payload['event_id'], str(payload['milestone']), milestone=payload['milestone'], instruction='Verify outcome against the scheduled official source; retain a quote. Mark a missed window explicitly.')

    async def reaction_capture(self, payload):
        key = f"{payload['event_id']}:{payload['token_id']}:{payload['checkpoint']}:1"
        if await self.repo.get('out.reaction_checkpoints', key):
            return
        truth = await self.truth.assess(payload['event_id'], payload['token_id'], payload['cutoff'])
        record = {'rwp_id': f"{payload['event_id']}:{payload['token_id']}", **payload, 'captured_at': iso(), 'revision': 1, 'market_truth_id': truth.get('mta_id'), 'market_truth': truth, 'config_version': self.settings.config_version, 'code_version': self.settings.code_version}
        await self.repo.put('out.reaction_checkpoints', key, record, immutable=True)

    async def demote(self, now):
        hot = await self.repo.list('mkt.hot_tokens', limit=100000)
        for row in hot:
            calls = await self.repo.list('ana.calls', filters={'token_id': row['token_id'], 'status': 'OPEN'}, limit=100000)
            armed = await self.repo.list('ana.watch_conditions', filters={'token_id': row['token_id'], 'status': 'ARMED'}, limit=100000)
            checkpoints = await self.repo.list('out.reaction_checkpoints', filters={'event_id': row['event_id'], 'token_id': row['token_id'], 'checkpoint': 'POST_72H'})
            if checkpoints and not calls and not armed:
                await self.market.demote(row['token_id'], row['event_id'])
        hot = await self.repo.list('mkt.hot_tokens', limit=100000)
        tokens = {r['token_id'] for r in hot}
        for row in sorted(hot, key=lambda r: (r.get('priority', 0), r['since'])):
            if len(tokens) <= 15:
                break
            if row['token_id'] in tokens:
                await self.market.demote(row['token_id'])
                tokens.remove(row['token_id'])

    async def heartbeat(self, now):
        notice_id = f'heartbeat_{now.date()}'
        if await self.repo.get('out.ops_notices', notice_id):
            return
        sent = await self.repo.list('out.deliveries', filters={'status': 'SENT'}, limit=100000)
        hot = await self.repo.list('mkt.hot_tokens', limit=100000)
        jobs = await self.repo.list('ops.jobs', filters={'status': 'QUEUED'}, limit=100000)
        usage = await self.repo.list('ops.llm_usage', limit=100000)
        count = sum(1 for r in sent if r.get('alert_id') and parse_time(r['at']) > now-timedelta(days=1))
        lag = max((max(0, (now-parse_time(j['run_after'])).total_seconds()) for j in jobs), default=0)
        spend = sum(Decimal(str(r.get('cost_usd', 0))) for r in usage if parse_time(r['at']).date() == now.date())
        async with self.repo.transaction():
            await self.repo.put('out.ops_notices', notice_id, {'notice_id': notice_id, 'kind': 'HEARTBEAT', 'text': f'Tank 24h: alerts={count}; hot tokens={len({r["token_id"] for r in hot})}; queue lag={lag:.0f}s; LLM spending=${spend}', 'created_at': iso(now), 'created_by': 'scheduler'}, immutable=True)
            await self.repo.enqueue('DELIVER', {'notice_id': notice_id, 'channel': 'telegram:ops'}, notice_id, priority=0, expires_at=iso(now+timedelta(days=1)), max_attempts=5)

    async def maintenance(self, now):
        import os
        import shutil
        from pathlib import Path
        from urllib.parse import urlsplit, unquote
        backup_dir = getattr(self.settings, 'backup_dir', None)
        if backup_dir is None:
            raise ValueError('TANK_BACKUP_DIR must point to the operator external backup disk')
        if now.hour < getattr(self.settings, 'backup_hour_utc', 2):
            return
        target = Path(backup_dir)
        if not target.is_dir():
            raise ValueError('Configured external backup directory is unavailable')
        final = target / f'tank-{now.date()}'
        if (final / 'COMPLETE').exists():
            return
        await self.repo.prune_market(iso(now-timedelta(days=getattr(self.settings, 'market_retention_days', 90))))
        staging = target / f'.tank-{now.date()}.partial'
        staging.mkdir(exist_ok=True)
        connection = urlsplit(self.settings.database_url)
        env = os.environ.copy()
        for key, value in {'PGHOST': connection.hostname, 'PGPORT': connection.port, 'PGUSER': unquote(connection.username or ''), 'PGPASSWORD': unquote(connection.password or ''), 'PGDATABASE': unquote(connection.path.lstrip('/'))}.items():
            if value:
                env[key] = str(value)
        proc = await asyncio.create_subprocess_exec('pg_dump', '--format=custom', '--file', str(staging/'tank.dump'), env=env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, stderr = await proc.communicate()
        if proc.returncode:
            raise RuntimeError(f'Nightly pg_dump failed with status {proc.returncode}')
        bundles = self.settings.data_dir / 'bundles'
        if bundles.exists():
            await asyncio.to_thread(shutil.copytree, bundles, staging/'bundles', dirs_exist_ok=True)
        (staging/'COMPLETE').write_text(iso(now)+'\n')
        if final.exists():
            raise RuntimeError('Incomplete backup destination already exists')
        staging.rename(final)
