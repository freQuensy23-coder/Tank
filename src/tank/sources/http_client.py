"""Direct source HTTP transport admits each built request, including redirects."""

from collections.abc import Awaitable, Callable, Mapping

import httpx

from tank.contracts.sources import HttpFetchPlan
from tank.sources.capture_fetch import is_exchange_url

type RequestAdmission = Callable[[HttpFetchPlan, httpx.Request], Awaitable[None]]
type RequestStarted = Callable[[], Awaitable[None]]


class SourceHttpTransport:
    def __init__(self, client: httpx.AsyncClient, admission: RequestAdmission) -> None:
        self.client = client
        self.admission = admission

    async def request(
        self,
        feed: HttpFetchPlan,
        headers: Mapping[str, str],
        on_start: RequestStarted | None,
    ) -> httpx.Response:
        request = self.client.build_request(
            feed.method,
            feed.url,
            headers=headers,
            json=feed.body if feed.method == "POST" else None,
            data=feed.form if feed.method == "POST" else None,
        )
        for redirect in range(self.client.max_redirects + 1):
            if is_exchange_url(str(request.url)):
                raise ValueError("Source redirect requires exchange capture transport")
            await self.admission(feed, request)
            if redirect == 0 and on_start is not None:
                await on_start()
            response = await self.client.send(request, follow_redirects=False)
            if response.next_request is None:
                return response
            request = response.next_request
        raise httpx.TooManyRedirects("Source redirect limit exceeded", request=request)
