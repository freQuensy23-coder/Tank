"""Immutable Telegram delivery ledger and delivered-call activation."""
import html
from datetime import timedelta
import httpx
from tank.util import iso, new_id, utcnow

class DeliveryError(RuntimeError):
    def __init__(self, message, retry_after=1):
        super().__init__(message)
        self.retry_after = retry_after

class DeliveryService:
    def __init__(self, repo, settings, *, http=None):
        self.repo, self.settings = repo, settings
        self.http = http or httpx.AsyncClient(timeout=8)
        self.owns_http = http is None

    async def close(self):
        if self.owns_http:
            await self.http.aclose()

    async def deliver(self, payload):
        subject = payload.get('notice_id') or f"{payload['alert_id']}:{payload['version']}"
        channel = payload.get('channel', 'telegram:ops' if payload.get('notice_id') else 'telegram:alerts')
        async with self.repo.lock('deliver:' + subject + ':' + channel):
            filters = {'channel': channel}
            filters.update({'notice_id': payload['notice_id']} if payload.get('notice_id') else
                           {'alert_id': payload['alert_id'], 'version': payload['version']})
            ledger = await self.repo.list('out.deliveries', filters=filters, limit=10000)
            if any(row['status'] == 'SENT' for row in ledger):
                return
            attempt = max((r['attempt'] for r in ledger), default=0) + 1
            if attempt > 5:
                raise DeliveryError('Delivery exhausted five attempts')
            notice = bool(payload.get('notice_id'))
            if notice:
                record = await self.repo.get('out.ops_notices', payload['notice_id'])
            else:
                rows = await self.repo.list('out.alerts', filters={'alert_id': payload['alert_id'], 'version': payload['version']})
                record = rows[0] if rows else None
            if record is None:
                raise ValueError('Delivery subject does not exist')
            chat = self.settings.telegram_ops_chat if notice else self.settings.telegram_alerts_chat
            if not self.settings.telegram_token or not chat:
                raise ValueError('Telegram token and destination must be configured')
            body = {'chat_id': chat, 'text': html.escape(record['text'] if notice else record['card_text']), 'parse_mode': 'HTML'}
            if not notice:
                prior = await self.repo.list('out.alerts', filters={'event_id': record['event_id']}, limit=10000)
                ids = {r['alert_id'] for r in prior}
                sent = await self.repo.list('out.deliveries', filters={'channel': channel, 'status': 'SENT'}, limit=100000)
                sent = sorted((r for r in sent if r.get('alert_id') in ids), key=lambda r: r['at'])
                if sent:
                    body['reply_parameters'] = {'message_id': int(sent[0]['message_ref'])}
            transition = {**filters, 'attempt': attempt, 'at': iso(), 'status': 'PENDING'}
            await self.repo.put('out.deliveries', new_id('delivery'), transition, immutable=True)
            try:
                response = await self.http.post(f'https://api.telegram.org/bot{self.settings.telegram_token}/sendMessage', json=body)
                data = response.json()
                if response.status_code != 200 or not data.get('ok'):
                    delay = max(2 ** (attempt - 1), data.get('parameters', {}).get('retry_after', 0))
                    raise DeliveryError(f'Telegram API rejected delivery (HTTP {response.status_code})', delay)
                message_ref = str(data['result']['message_id'])
            except Exception as exc:
                await self.repo.put('out.deliveries', new_id('delivery'), {**transition, 'at': iso(), 'status': 'FAILED', 'error': type(exc).__name__}, immutable=True)
                if attempt == 5 and not notice:
                    notice_id = 'delivery_failed_' + subject.replace(':', '_')
                    async with self.repo.transaction():
                        await self.repo.put('out.ops_notices', notice_id, {'notice_id': notice_id, 'kind': 'DELIVERY_FAILED', 'subject_id': payload['alert_id'], 'text': f'Delivery failed: {subject}', 'created_at': iso(), 'created_by': 'delivery'}, immutable=True)
                        await self.repo.enqueue('DELIVER', {'notice_id': notice_id, 'channel': 'telegram:ops'}, notice_id, priority=0, max_attempts=5, expires_at=iso(utcnow()+timedelta(days=1)))
                raise DeliveryError('Telegram delivery failed', getattr(exc, 'retry_after', 2 ** (attempt - 1))) from None
            async with self.repo.transaction():
                await self.repo.put('out.deliveries', new_id('delivery'), {**transition, 'at': iso(), 'status': 'SENT', 'message_ref': message_ref}, immutable=True)
                if not notice:
                    await self._activate_calls(record)

    async def _activate_calls(self, alert):
        for dec_id in alert.get('decisions', []):
            decision = await self.repo.get('ana.decisions', dec_id)
            if not decision:
                raise ValueError('Delivered card references missing decision')
            calls = await self.repo.list('ana.calls', filters={'event_id': alert['event_id'], 'token_id': decision['token_id'], 'status': 'OPEN'})
            if decision['action'] == 'START' and not calls:
                entry = decision.get('entry_ref_price')
                invalidation = decision.get('invalidation_price')
                if entry is None or invalidation is None:
                    raise ValueError('START decision must freeze entry_ref_price and invalidation_price before delivery')
                call_id = new_id('call')
                await self.repo.put('ana.calls', call_id, {'call_id': call_id, 'event_id': alert['event_id'], 'token_id': decision['token_id'], 'opened_by_alert': alert['alert_id'], 'opened_by_version': alert['version'], 'opened_at': iso(), 'entry_ref_price': entry, 'invalidation_price': invalidation, 'status': 'OPEN'})
            elif decision['action'] == 'EXIT':
                for call in calls:
                    await self.repo.put('ana.calls', call['call_id'], {**call, 'status': 'CLOSED', 'closed_by': 'EXIT', 'closed_at': iso()})
