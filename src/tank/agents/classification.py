"""Nongenerative classification use case with a typed immutable score ledger."""

from typing import Protocol

import httpx
from pydantic import JsonValue

from tank.agents.jev import JevQuestion, JevState, binding, classify
from tank.agents.pipeline_contracts import (
    ClassificationIdentity,
    ClassificationName,
    JevScoreRecord,
    MissingClassificationFields,
)
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.time_precision import collection_clock
from tank.util import sha256


class ClassificationSource(Protocol):
    @property
    def item_id(self) -> str: ...


class ClassificationSettings(Protocol):
    @property
    def opencode_api_key(self) -> str | None: ...
    @property
    def replay(self) -> bool: ...


class ClassificationScores(Protocol):
    async def get(self, key: str) -> JevScoreRecord | None: ...
    async def put(self, key: str, value: JevScoreRecord, *, immutable: bool = False) -> JevScoreRecord: ...


JEV_PROMPT_VERSION = "jev-v2"
FILTER = "Is `item` about crypto: crypto assets, crypto exchanges, crypto regulation or enforcement, blockchain networks, or one of these projects: Bitcoin, Ethereum, Uniswap, PancakeSwap, Aerodrome, Curve, 1inch, 0x, CoW Protocol, Hyperliquid, Lighter, Aave, Morpho, Euler, Ondo, LayerZero, Quant, Avalanche, NEAR, Plasma, Ethena, World (Worldcoin)?"
FILTER_CRITERIA = {
    "true": "About crypto, even if it reports nothing new or important.",
    "false": "General finance, technology or politics that does not touch crypto.",
}
ROUTE = "Does `item` report the same real-world event as `event`, or a new development of that event?"
ROUTE_CRITERIA = {
    "true": "The same event, or a development of it: a confirmation, denial, stage change, new detail or new party.",
    "false": "A different event, even one about the same project, company, exchange or token.",
}


class JevClassifier:
    def __init__(
        self,
        scores: ClassificationScores,
        settings: ClassificationSettings,
        http: httpx.AsyncClient,
        *,
        versions: ResolvedVersionIdentity,
    ) -> None:
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.scores = scores
        self.settings = settings
        self.http = http

    async def score(self, item: ClassificationSource, name: ClassificationName, state: JevState) -> float:
        return (await self._score_record(item, name, state)).score

    def score_identity(
        self, item: ClassificationSource, name: ClassificationName, state: JevState
    ) -> ClassificationIdentity:
        match name:
            case "relevant":
                instructions, criteria = FILTER, FILTER_CRITERIA
            case "same_event":
                instructions, criteria = ROUTE, ROUTE_CRITERIA
            case _:
                raise ValueError("Unregistered classification question")
        questions: dict[str, JevQuestion] = {name: JevQuestion(instructions=instructions, criteria=criteria)}
        pinned = binding(self.settings.opencode_api_key)
        wire_state = state.to_wire()
        wire_questions: dict[str, JsonValue] = {
            name: question.to_wire() for name, question in questions.items()
        }
        state_fingerprint = sha256(wire_state)
        question_fingerprint = sha256(wire_questions)
        request_fingerprint = sha256(
            {
                "model": pinned.request_model,
                "state": wire_state,
                "questions": wire_questions,
            }
        )
        key = sha256(
            {
                "item_id": item.item_id,
                "model": pinned.request_model,
                "provider": pinned.provider,
                "base_url": pinned.base_url,
                "prompt_version": JEV_PROMPT_VERSION,
                "questions": wire_questions,
                "state_fingerprint": state_fingerprint,
            }
        )
        return ClassificationIdentity(
            usage_id=key,
            questions=questions,
            binding=pinned,
            state_fingerprint=state_fingerprint,
            question_fingerprint=question_fingerprint,
            request_fingerprint=request_fingerprint,
        )

    async def _score_record(
        self, item: ClassificationSource, name: ClassificationName, state: JevState
    ) -> JevScoreRecord:
        identity = self.score_identity(item, name, state)
        cached = await self.scores.get(identity.usage_id)
        if cached is not None:
            if (
                cached.usage_id != identity.usage_id
                or cached.item_id != item.item_id
                or cached.requested_model_id != identity.binding.request_model
                or cached.questions != identity.questions
                or cached.state_fingerprint != identity.state_fingerprint
                or cached.question_fingerprint != identity.question_fingerprint
                or cached.request_fingerprint != identity.request_fingerprint
            ):
                raise ValueError("Stored classification does not match its requested identity")
            return cached
        if self.settings.replay:
            raise ValueError(
                "Replay requires stored Jev score for the exact wire question and state; live classification is disabled"
            )
        data = await classify(
            self.http,
            api_key=self.settings.opencode_api_key,
            state=state,
            questions=identity.questions,
        )
        usage = data.usage
        cost_reason = "OpenCode does not report usage.cost" if usage.cost is None else None
        completed = collection_clock()
        at = completed.value
        if at is None:
            raise RuntimeError("Classification completion clock has no timestamp")
        record = JevScoreRecord(
            usage_id=identity.usage_id,
            item_id=item.item_id,
            call="FILTER" if name == "relevant" else "ROUTE",
            model_id=data.model,
            semantic_model_id="jev-1.13.0",
            requested_model_id=identity.binding.request_model,
            jev_provider="opencode",
            endpoint="https://opencode.ai/inference/systemone/v1/systemone",
            resolved_model_id=data.model,
            response_id=data.id,
            upstream_provider=data.provider,
            model_pin_evidence="OpenCode serves Jev 1.13 under this model name; no dated snapshot or patch suffix is exposed",
            billing_basis="OPENCODE_REPORTED",
            billing_currency="USD",
            prompt_version=JEV_PROMPT_VERSION,
            questions=identity.questions,
            state_fingerprint=identity.state_fingerprint,
            question_fingerprint=identity.question_fingerprint,
            request_fingerprint=identity.request_fingerprint,
            session_id=None,
            score=data.answers[name].noul,
            usage=usage,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            effort=None,
            stop_reason="NON_GENERATIVE_CLASSIFICATION",
            web_searches=0,
            missing_fields=MissingClassificationFields(
                effort="Not applicable to nongenerative Jev classification",
                cost_usd=cost_reason,
                input_tokens="Provider omitted input tokens" if usage.input_tokens is None else None,
                output_tokens="Provider omitted output tokens" if usage.output_tokens is None else None,
                cache_read_tokens="Not exposed by Jev API",
                cache_write_tokens="Not exposed by Jev API",
            ),
            cost_usd=usage.cost,
            cost_reason=cost_reason,
            at=at,
            at_prec="COLLECTION_ONLY",
            at_prec_reason=None,
            created_at=at,
            created_at_prec="COLLECTION_ONLY",
            created_at_prec_reason=None,
            subject_id=item.item_id,
            model=data.model,
            config_version=self.versions.config_version,
            code_version=self.versions.code_version,
        )
        return await self.scores.put(identity.usage_id, record, immutable=True)
