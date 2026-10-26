"""Admitted question context projected into a native-session prompt."""

from collections.abc import Mapping

from pydantic import JsonValue

from tank.contracts.telegram_state import (
    QuestionOpsReference,
    QuestionPrompt,
    QuestionPromptAttachment,
    QuestionPromptReference,
    QuestionReference,
    QuestionUnreportedReference,
)

REFERENCE_LIMIT = 1024


def question_prompt(record: Mapping[str, JsonValue]) -> QuestionPrompt:
    """Project actual optional historical context without synthesizing reference facts."""
    raw_reference = record.get("reference")
    reference: QuestionPromptReference | None
    if raw_reference is None:
        reference = None
    elif not isinstance(raw_reference, dict):
        raise ValueError("Question prompt reference must be a recorded context")
    else:
        kind = raw_reference.get("kind")
        if kind == "OPS_CARD":
            reference = QuestionOpsReference.model_validate(
                {field: raw_reference[field] for field in ("kind", "item_id") if field in raw_reference}
                | {"text": raw_reference.get("text")}
            )
        elif kind is None:
            reference = QuestionUnreportedReference.model_validate(
                {"kind": None, "text": raw_reference.get("text")}
            )
        else:
            reference = QuestionReference.model_validate({"kind": kind, "text": raw_reference.get("text")})
    raw_attachments = record.get("attachments")
    attachments: list[QuestionPromptAttachment] | None
    if raw_attachments is None:
        attachments = None
    elif not isinstance(raw_attachments, list):
        raise ValueError("Question prompt attachments must be a recorded list")
    else:
        attachments = []
        for item in raw_attachments:
            if not isinstance(item, dict):
                raise ValueError("Question prompt attachment must be a recorded file")
            attachments.append(
                QuestionPromptAttachment.model_validate(
                    {field: item[field] for field in ("filename", "size") if field in item}
                    | {"path": item.get("path")}
                )
            )
    return QuestionPrompt.model_validate(
        {"text": record.get("text"), "reference": reference, "attachments": attachments}
    )


def question_message(question: QuestionPrompt) -> str:
    """Format only admitted context; unavailable content remains unreported."""
    question = QuestionPrompt.model_validate(question)
    reference = question.reference
    reference_text = "" if reference is None or reference.text is None else reference.text[:REFERENCE_LIMIT]
    lines = [f"Пришёл фоллоуап через Telegram на сообщение с текстом:\n{reference_text}"]
    if isinstance(reference, QuestionOpsReference):
        lines.append(f"Вопрос задан в чате Tank Ops о материале {reference.item_id}; ответ уйдёт туда же.")
    lines.append("(без текста)" if question.text is None or question.text == "" else question.text)
    if question.attachments is not None:
        for item in question.attachments:
            path = "файл недоступен" if item.path is None else item.path
            lines.append(f"Пользователь приложил файл {item.filename} ({item.size} байт): {path}.")
    lines.append(
        "Твоё последнее сообщение в этом ходе уйдёт пользователю ответом в Telegram; "
        "файл можно отправить тулом send_telegram_file."
    )
    return "\n\n".join(lines)
