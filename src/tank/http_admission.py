"""Apply declared provider quotas to actual built Python HTTP requests."""

from hashlib import sha256
from urllib.parse import parse_qsl

import httpx

from tank.contracts.http_policy import HttpAdmissionDecision, HttpAdmitted, HttpRefused, HttpUnconfigured
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.sources import SourceApiLimitsConfiguration, SourceEtherscanBudget, SourceSecBudget
from tank.request_budget import BudgetConstraint, SharedRequestBudgets


class ConfiguredHttpAdmission:
    def __init__(
        self,
        policy: SourceApiLimitsConfiguration,
        versions: ResolvedVersionIdentity,
        budgets: SharedRequestBudgets,
    ) -> None:
        self.policy = SourceApiLimitsConfiguration.model_validate(policy)
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.budgets = budgets

    async def admit(self, request: httpx.Request) -> HttpAdmissionDecision:
        host = request.url.host
        if host == "api.etherscan.io":
            return await self._etherscan(request)
        if host == "sec.gov" or host.endswith(".sec.gov"):
            return await self._sec(request)
        return HttpUnconfigured(host=host)

    async def _sec(self, request: httpx.Request) -> HttpAdmissionDecision:
        host = request.url.host
        if request.url.scheme != "https" or request.url.port not in (None, 443):
            return HttpRefused(host=host, reason="UNSUPPORTED_API_ROUTE")
        rule = next((value for value in self.policy.limits if isinstance(value, SourceSecBudget)), None)
        if rule is None:
            return HttpRefused(host=host, reason="MISSING_PROVIDER_POLICY")
        await self.budgets.acquire_many(
            [
                BudgetConstraint(
                    key="sec.gov",
                    capacity=float(rule.tank_max_requests_per_s),
                    window_s=1.0,
                    weight=1.0,
                    host=host,
                )
            ]
        )
        return HttpAdmitted(provider="sec.gov", host=host, versions=self.versions)

    async def _etherscan(self, request: httpx.Request) -> HttpAdmissionDecision:
        host = request.url.host
        if (
            request.method != "GET"
            or request.url.scheme != "https"
            or request.url.port not in (None, 443)
            or request.url.path != "/v2/api"
            or request.url.userinfo
        ):
            return HttpRefused(host=host, reason="UNSUPPORTED_API_ROUTE")
        try:
            query = parse_qsl(
                request.url.query.decode("ascii"),
                keep_blank_values=True,
                encoding="utf-8",
                errors="strict",
            )
        except (UnicodeError, ValueError):
            return HttpRefused(host=host, reason="INVALID_API_CREDENTIAL")
        keys = [value for name, value in query if name == "apikey"]
        if len(keys) != 1 or not keys[0]:
            return HttpRefused(host=host, reason="INVALID_API_CREDENTIAL")
        rule = next((value for value in self.policy.limits if isinstance(value, SourceEtherscanBudget)), None)
        if rule is None:
            return HttpRefused(host=host, reason="MISSING_PROVIDER_POLICY")
        scope = "etherscan:api-key:" + sha256(keys[0].encode("utf-8")).hexdigest()
        await self.budgets.acquire_many(
            [
                BudgetConstraint(
                    key=scope,
                    capacity=float(rule.tank_max_requests_per_s),
                    window_s=1.0,
                    weight=1.0,
                    host=host,
                ),
                BudgetConstraint(
                    key=scope,
                    capacity=float(rule.tank_max_requests_per_day),
                    window_s=86400.0,
                    weight=1.0,
                    host=host,
                ),
            ]
        )
        return HttpAdmitted(provider="etherscan", host=host, versions=self.versions)
