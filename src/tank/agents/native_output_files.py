"""Owned output-file reads and confirmed Telegram outcomes at the native tool boundary."""

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Annotated, Protocol, Self
from urllib.parse import quote

import httpx
from pydantic import Field, JsonValue, RootModel, model_validator

from tank.agents.context_binding import ContextBinding
from tank.agents.outbound_inputs import outbound_message_record
from tank.contracts import DomainModel
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.contracts.telegram_state import (
    KnownTelegramFileDelivery,
    TelegramFileDeliveryOutcome,
    TelegramFileSent,
)
from tank.question_answers import validate_question_identity
from tank.runtime_contracts import UserQuestionMessage


class OutputFileArguments(DomainModel):
    filename: Annotated[str, Field(min_length=1, max_length=200)]
    caption: Annotated[str, Field(max_length=1024)] | None = None

    @model_validator(mode="after")
    def actual_tool_arguments(self) -> Self:
        if self.filename in {".", ".."} or "/" in self.filename or "\\" in self.filename:
            raise ValueError("filename must be a plain name inside /mnt/session/outputs")
        if "caption" in self.model_fields_set and self.caption is None:
            raise ValueError("A supplied caption must be a string")
        return self


class OutputFileRequest(DomainModel):
    target: FollowupRefusalTarget
    call_id: Annotated[str, Field(min_length=1, pattern=r"\S")]
    arguments: OutputFileArguments


class OutputReadPolicy(DomainModel):
    max_bytes: Annotated[int, Field(gt=0)]
    attempts: Annotated[int, Field(ge=1)]
    retry_delay_s: Annotated[int | float, Field(ge=0)]
    timeout_s: Annotated[int | float, Field(gt=0)]


class SessionFileSender(Protocol):
    async def lookup_session_file(
        self,
        target: FollowupRefusalTarget,
        call_id: str,
        filename: str,
    ) -> KnownTelegramFileDelivery | None: ...

    async def send_session_file(
        self,
        target: FollowupRefusalTarget,
        call_id: str,
        filename: str,
        content: bytes,
        caption: str | None,
    ) -> TelegramFileDeliveryOutcome: ...


class OutputQuestionStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...


class OutputFileOutcome(RootModel[TelegramFileDeliveryOutcome]):
    model_config = {"strict": True, "hide_input_in_errors": True, "revalidate_instances": "always"}


class KnownOutputFileOutcome(RootModel[KnownTelegramFileDelivery]):
    model_config = {"strict": True, "hide_input_in_errors": True, "revalidate_instances": "always"}


def output_request(
    session: dict[str, JsonValue],
    arguments: dict[str, JsonValue],
    call_id: str,
) -> OutputFileRequest:
    admitted = OutputFileArguments.model_validate(arguments)
    binding = ContextBinding.from_session(session)
    if binding.active_message_id is None:
        raise ValueError("send_telegram_file is available only while answering a Telegram follow-up")
    owner = outbound_message_record(session, binding.active_message_id)
    payload = owner["payload"]
    if not isinstance(payload, dict):
        raise ValueError("Output-file owner requires its structured input payload")
    if payload.get("kind") != "USER_QUESTION":
        raise ValueError("send_telegram_file is available only while answering a Telegram follow-up")
    message = UserQuestionMessage.model_validate(payload)
    if message.session_id != binding.session_id:
        raise ValueError("Output-file question owner belongs to another session")
    return OutputFileRequest(
        target=FollowupRefusalTarget(question_id=message.question_id, session_id=binding.session_id),
        call_id=call_id,
        arguments=admitted,
    )


class SessionOutputFiles:
    def __init__(
        self,
        store: OutputQuestionStore,
        sender: SessionFileSender,
        http: httpx.AsyncClient,
        base_url: str,
        headers: Callable[[], dict[str, str]],
        policy: OutputReadPolicy,
    ) -> None:
        self.store, self.sender, self.http = store, sender, http
        self.base_url, self.headers = base_url.rstrip("/"), headers
        self.policy = OutputReadPolicy.model_validate(policy)

    async def send(self, request: OutputFileRequest) -> TelegramFileDeliveryOutcome:
        request = OutputFileRequest.model_validate(request)
        async with self.store.lock("question:" + request.target.question_id):
            question = await self.store.get("out.user_questions", request.target.question_id)
            if question is None:
                raise KeyError(request.target.question_id)
            validate_question_identity(question, request.target)
        known = await self.sender.lookup_session_file(
            request.target,
            request.call_id,
            request.arguments.filename,
        )
        if known is not None:
            return KnownOutputFileOutcome.model_validate(known).root
        session_id = request.target.session_id
        if session_id in {".", ".."}:
            raise ValueError("Native output session cannot form an unambiguous path segment")
        filename = request.arguments.filename
        path = f"/v1/sessions/{quote(session_id, safe='')}/outputs/{quote(filename, safe='')}"
        content: bytes | None = None
        for _ in range(self.policy.attempts):
            async with self.http.stream(
                "GET",
                self.base_url + path,
                headers=self.headers(),
                timeout=self.policy.timeout_s,
            ) as response:
                if response.status_code == 200:
                    received = bytearray()
                    async for block in response.aiter_bytes():
                        received.extend(block)
                        if len(received) > self.policy.max_bytes:
                            raise ValueError("File exceeds the 45 MB Telegram bot limit")
                    content = bytes(received)
                    break
                if response.status_code != 404:
                    response.raise_for_status()
            await asyncio.sleep(self.policy.retry_delay_s)
        if content is None:
            raise ValueError("Requested output file was not found in this session")
        outcome = OutputFileOutcome.model_validate(
            await self.sender.send_session_file(
                request.target,
                request.call_id,
                filename,
                content,
                request.arguments.caption,
            )
        ).root
        if isinstance(outcome, TelegramFileSent) and (
            outcome.filename != filename or outcome.bytes != len(content)
        ):
            raise ValueError("File delivery receipt conflicts with its actual requested output")
        return outcome
