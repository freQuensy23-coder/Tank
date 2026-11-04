import asyncio
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tank.analysis.truth import MarketTruthService
    from tank.analysis.truth_native_local import ReplayNativeRows


async def _finish_cleanup[_Result](
    operation: Awaitable[_Result], *, failure: BaseException | None = None
) -> _Result:
    task = asyncio.ensure_future(operation)
    cancellation = None
    try:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as error:
                if task.cancelled():
                    raise
                if cancellation is None:
                    cancellation = error
        result = task.result()
        if cancellation is not None:
            raise cancellation
        return result
    except BaseException as error:
        if failure is not None:
            raise failure from error
        raise


@dataclass(slots=True, repr=False)
class _ReplayNativeState:
    service: "MarketTruthService"
    repo: Any
    settings: Any
    provider: "ReplayNativeRows"
    token: object
    lifecycle: str = "active"


def native_provider(service: "MarketTruthService") -> Any:
    state = getattr(service, "_replay_native_state", None)
    if state is None:
        return getattr(service.repo, "clickhouse", None)
    if (
        not isinstance(state, _ReplayNativeState)
        or state.service is not service
        or state.repo is not service.repo
        or state.settings is not service.settings
        or not service.settings.replay
        or state.lifecycle != "active"
        or state.token is None
    ):
        raise RuntimeError("Invalid replay native ownership")
    return state.provider


@asynccontextmanager
async def bind_replay_native(service: "MarketTruthService") -> AsyncIterator[None]:
    from tank.analysis.truth import MarketTruthService

    if not isinstance(service, MarketTruthService) or not service.settings.replay:
        raise ValueError("Replay native binding requires the real replay truth service")
    previous_present = "_replay_native_state" in vars(service)
    previous = getattr(service, "_replay_native_state", None)
    if previous is not None:
        raise RuntimeError("Replay native ownership already exists")

    from tank.analysis.truth_native_local import ReplayNativeRows

    state = _ReplayNativeState(service, service.repo, service.settings, ReplayNativeRows(), object())
    service._replay_native_state = state
    failure = None
    try:
        yield
    except BaseException as error:
        failure = error
        raise
    finally:
        state.lifecycle = "closing"
        try:
            await _finish_cleanup(state.provider.close())
        except BaseException as error:
            if failure is not None:
                raise failure from error
            raise
        finally:
            state.lifecycle = "closed"
            state.token = None
            if getattr(service, "_replay_native_state", None) is state:
                if previous_present:
                    service._replay_native_state = previous
                else:
                    delattr(service, "_replay_native_state")
