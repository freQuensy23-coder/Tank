"""Nongenerative classification use case with a typed immutable score ledger."""

from typing import Protocol

import httpx
from pydantic import JsonValue

from tank.agents.decision import DecisionQuestion, DecisionState, binding, classify
from tank.agents.pipeline_contracts import (
    ClassificationIdentity,
    ClassificationName,
    DecisionScoreRecord,
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
    def foundry_api_key(self) -> str | None: ...
    @property
    def foundry_endpoint(self) -> str: ...
    @property
    def foundry_deployment(self) -> str: ...
    @property
    def replay(self) -> bool: ...


class ClassificationScores(Protocol):
    async def get(self, key: str) -> DecisionScoreRecord | None: ...
    async def put(
        self, key: str, value: DecisionScoreRecord, *, immutable: bool = False
    ) -> DecisionScoreRecord: ...


DECISION_PROMPT_VERSION = "decision-1-v1"
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


class DecisionClassifier:
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

    async def score(
        self, item: ClassificationSource, name: ClassificationName, state: DecisionState
    ) -> float:
        return (await self._score_record(item, name, state)).score

    def score_identity(
        self, item: ClassificationSource, name: ClassificationName, state: DecisionState
    ) -> ClassificationIdentity:
        match name:
            case "relevant":
                instructions, criteria = FILTER, FILTER_CRITERIA
            case "same_event":
                instructions, criteria = ROUTE, ROUTE_CRITERIA
            case _:
                raise ValueError("Unregistered classification question")
        questions: dict[str, DecisionQuestion] = {
            name: DecisionQuestion(instructions=instructions, criteria=criteria)
        }
        pinned = binding(self.settings)
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
                "semantic_model": pinned.semantic_model,
                "provider": pinned.provider,
                "base_url": pinned.base_url,
                "prompt_version": DECISION_PROMPT_VERSION,
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
        self, item: ClassificationSource, name: ClassificationName, state: DecisionState
    ) -> DecisionScoreRecord:
        identity = self.score_identity(item, name, state)
        cached = await self.scores.get(identity.usage_id)
        if cached is not None:
            if (
                cached.usage_id != identity.usage_id
                or cached.item_id != item.item_id
                or cached.requested_model_id != identity.binding.request_model
                or cached.classifier_provider != identity.binding.provider
                or cached.endpoint != identity.binding.endpoint
                or cached.semantic_model_id != identity.binding.semantic_model
                or cached.prompt_version != DECISION_PROMPT_VERSION
                or cached.questions != identity.questions
                or cached.state_fingerprint != identity.state_fingerprint
                or cached.question_fingerprint != identity.question_fingerprint
                or cached.request_fingerprint != identity.request_fingerprint
            ):
                raise ValueError("Stored classification does not match its requested identity")
            return cached
        if self.settings.replay:
            raise ValueError(
                "Replay requires stored Decision-1 score for the exact wire question and state; live classification is disabled"
            )
        data = await classify(
            self.http,
            api_key=self.settings.foundry_api_key,
            state=state,
            questions=identity.questions,
            pinned=identity.binding,
        )
        usage = data.usage
        cost_reason = "Azure Foundry does not report usage.cost" if usage.cost is None else None
        completed = collection_clock()
        at = completed.value
        if at is None:
            raise RuntimeError("Classification completion clock has no timestamp")
        record = DecisionScoreRecord(
            usage_id=identity.usage_id,
            item_id=item.item_id,
            call="FILTER" if name == "relevant" else "ROUTE",
            model_id=data.model,
            semantic_model_id="microsoft-decision-1:1",
            requested_model_id=identity.binding.request_model,
            classifier_provider="azure-foundry",
            endpoint=identity.binding.endpoint,
            resolved_model_id=data.model,
            response_id=data.id,
            upstream_provider=data.provider,
            model_pin_evidence="Foundry deployment is configured for Microsoft-Decision-1 version 1; the response identifies microsoft-decision-1 but does not attest its version",
            billing_basis="AZURE_FOUNDRY_REPORTED",
            billing_currency="USD",
            prompt_version=DECISION_PROMPT_VERSION,
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
                effort="Not applicable to nongenerative Decision-1 classification",
                cost_usd=cost_reason,
                input_tokens="Provider omitted input tokens" if usage.input_tokens is None else None,
                output_tokens="Provider omitted output tokens" if usage.output_tokens is None else None,
                cache_read_tokens="Not exposed by Decision-1 API",
                cache_write_tokens="Not exposed by Decision-1 API",
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
