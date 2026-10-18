"""V0-A source and BASE market capture before V0-B Jev calibration.

PRD §10 separates source/market acceptance from event calibration. This service
never starts the event workers, model gateway, sessions, scheduler or delivery.
"""

import asyncio
import logging

from tank.bootstrap import sync_registry

log = logging.getLogger(__name__)


class CaptureService:
    def __init__(self, repo, settings, *, sources=None, market=None):
        if not getattr(settings, "capture_only", False):
            raise ValueError("CaptureService requires capture_only settings")
        if getattr(settings, "replay", False):
            raise ValueError("Live capture cannot run with replay settings")
        self.repo, self.settings = repo, settings
        if sources is None:
            from tank.sources import SourceService

            sources = SourceService(repo, settings)
        if market is None:
            from tank.market.service import MarketService

            market = MarketService(repo, settings)
        self.sources, self.market = sources, market

    @staticmethod
    async def _pause(stop, seconds):
        try:
            await asyncio.wait_for(stop.wait(), seconds)
        except TimeoutError:
            pass

    async def _supervise(self, name, service, stop):
        while not stop.is_set():
            try:
                await service.run(stop)
                if not stop.is_set():
                    raise RuntimeError("Capture service returned unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s capture failed; retrying", name)
                await self._pause(stop, 1)

    async def _partitions(self, stop):
        await self._pause(stop, 3600)
        while not stop.is_set():
            try:
                await self.repo.maintain_partitions()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Capture partition maintenance failed; retrying")
                await self._pause(stop, 1)
                continue
            await self._pause(stop, 3600)

    async def run(self, stop):
        tasks = []
        try:
            await sync_registry(self.repo, self.settings)
            # Partition creation must not depend on model jobs or backup success.
            if hasattr(self.repo, "maintain_partitions"):
                await self.repo.maintain_partitions()
            tasks = [
                asyncio.create_task(self._supervise(name, service, stop), name="capture-" + name)
                for name, service in (("sources", self.sources), ("market", self.market))
            ]
            if hasattr(self.repo, "maintain_partitions"):
                tasks.append(asyncio.create_task(self._partitions(stop), name="capture-partitions"))
            await stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.close()

    async def close(self):
        for service in (self.sources, self.market):
            if hasattr(service, "close"):
                try:
                    await service.close()
                except Exception:
                    log.exception("Capture resource close failed")
