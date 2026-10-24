"""Typed decimal contexts for synchronous and asynchronous financial calculations."""

from collections.abc import Awaitable, Callable
from decimal import localcontext
from functools import wraps


def precise[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    """Run a synchronous financial calculation without default-context rounding."""

    @wraps(function)
    def synchronous(*args: P.args, **kwargs: P.kwargs) -> R:
        with localcontext() as context:
            context.prec = 100
            return function(*args, **kwargs)

    return synchronous


def async_precise[**P, R](function: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Keep the financial context active through awaited acquisition and computation."""

    @wraps(function)
    async def asynchronous(*args: P.args, **kwargs: P.kwargs) -> R:
        with localcontext() as context:
            context.prec = 100
            return await function(*args, **kwargs)

    return asynchronous
