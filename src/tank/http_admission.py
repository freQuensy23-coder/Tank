"""Apply declared provider quotas to actual built Python HTTP requests."""

from collections.abc import Sequence
from hashlib import sha256
from urllib.parse import parse_qsl

import httpx

from tank.contracts import DomainModel
from tank.contracts.http_policy import HttpAdmissionDecision, HttpAdmitted, HttpRefused, HttpUnconfigured
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.sources import SourceApiLimitsConfiguration, SourceEtherscanBudget, SourceSecBudget
from tank.request_budget import BudgetConstraint, SharedRequestBudgets


class HttpAdmissionPlan(DomainModel):
    decision: HttpAdmissionDecision
    constraints: tuple[BudgetConstraint, ...]


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
        return await self.admit_with_constraints(request, ())

    async def admit_with_constraints(
        self, request: httpx.Request, constraints: Sequence[BudgetConstraint]
    ) -> HttpAdmissionDecision:
        plan = self.plan(request)
        if isinstance(plan.decision, HttpRefused):
            return plan.decision
        values = (*plan.constraints, *constraints)
        if values:
            await self.budgets.acquire_many(values)
        return plan.decision

    def plan(self, request: httpx.Request) -> HttpAdmissionPlan:
        host = request.url.host
        if not host:
            raise ValueError("HTTP admission requires an absolute request destination")
        if host == "api.etherscan.io":
            return self._etherscan(request)
        if host == "sec.gov" or host.endswith(".sec.gov"):
            return self._sec(request)
        return HttpAdmissionPlan(decision=HttpUnconfigured(host=host), constraints=())

    def _sec(self, request: httpx.Request) -> HttpAdmissionPlan:
        host = request.url.host
        if request.url.scheme != "https" or request.url.port not in (None, 443) or request.url.userinfo:
            return HttpAdmissionPlan(
                decision=HttpRefused(host=host, reason="UNSUPPORTED_API_ROUTE"), constraints=()
            )
        rule = next((value for value in self.policy.limits if isinstance(value, SourceSecBudget)), None)
        if rule is None:
            return HttpAdmissionPlan(
                decision=HttpRefused(host=host, reason="MISSING_PROVIDER_POLICY"), constraints=()
            )
        constraints = (
            BudgetConstraint(
                key="sec.gov",
                capacity=float(rule.tank_max_requests_per_s),
                window_s=1.0,
                weight=1.0,
                host=host,
            ),
        )
        return HttpAdmissionPlan(
            decision=HttpAdmitted(provider="sec.gov", host=host, versions=self.versions),
            constraints=constraints,
        )

    def _etherscan(self, request: httpx.Request) -> HttpAdmissionPlan:
        host = request.url.host
        if (
            request.method != "GET"
            or request.url.scheme != "https"
            or request.url.port not in (None, 443)
            or request.url.raw_path.split(b"?", 1)[0] != b"/v2/api"
            or request.url.userinfo
        ):
            return HttpAdmissionPlan(
                decision=HttpRefused(host=host, reason="UNSUPPORTED_API_ROUTE"), constraints=()
            )
        try:
            query = parse_qsl(
                request.url.query.decode("ascii"),
                keep_blank_values=True,
                encoding="utf-8",
                errors="strict",
            )
        except (UnicodeError, ValueError):
            return HttpAdmissionPlan(
                decision=HttpRefused(host=host, reason="INVALID_API_CREDENTIAL"), constraints=()
            )
        keys = [value for name, value in query if name == "apikey"]
        if len(keys) != 1 or not keys[0]:
            return HttpAdmissionPlan(
                decision=HttpRefused(host=host, reason="INVALID_API_CREDENTIAL"), constraints=()
            )
        rule = next((value for value in self.policy.limits if isinstance(value, SourceEtherscanBudget)), None)
        if rule is None:
            return HttpAdmissionPlan(
                decision=HttpRefused(host=host, reason="MISSING_PROVIDER_POLICY"), constraints=()
            )
        scope = "etherscan:api-key:" + sha256(keys[0].encode("utf-8")).hexdigest()
        constraints = (
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
        )
        return HttpAdmissionPlan(
            decision=HttpAdmitted(provider="etherscan", host=host, versions=self.versions),
            constraints=constraints,
        )
