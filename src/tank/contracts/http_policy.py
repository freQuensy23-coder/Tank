"""Explicit outcomes for configured admission immediately before Python HTTP I/O."""

from datetime import datetime
from typing import Literal, Protocol

import httpx
from pydantic import Field

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity


class HttpAdmitted(DomainModel):
    status: Literal["ADMITTED"] = "ADMITTED"
    provider: Literal["sec.gov", "etherscan"]
    host: str = Field(min_length=1)
    versions: ResolvedVersionIdentity


class HttpUnconfigured(DomainModel):
    status: Literal["UNCONFIGURED"] = "UNCONFIGURED"
    host: str = Field(min_length=1)
    reason: Literal["NO_CONFIGURED_PROVIDER_POLICY"] = "NO_CONFIGURED_PROVIDER_POLICY"


class HttpRefused(DomainModel):
    status: Literal["REFUSED"] = "REFUSED"
    host: str = Field(min_length=1)
    reason: Literal["UNSUPPORTED_API_ROUTE", "INVALID_API_CREDENTIAL", "MISSING_PROVIDER_POLICY"]


type HttpAdmissionDecision = HttpAdmitted | HttpUnconfigured | HttpRefused


class HttpAdmission(Protocol):
    async def block(self, request: httpx.Request, until: datetime, reason: str) -> None: ...
    async def admit(self, request: httpx.Request) -> HttpAdmissionDecision: ...
