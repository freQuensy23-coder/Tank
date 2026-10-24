"""Native Telegram rich presentation with validated score contracts."""

from collections.abc import Mapping
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.score import ScoreAssessment

SCORE_DIMENSIONS = (
    ("Economic directness", "economic_directness"),
    ("Magnitude", "magnitude"),
    ("Permanence", "permanence"),
    ("Distribution expansion", "distribution_expansion"),
    ("Mechanical flows", "mechanical_flows"),
    ("Surprise", "surprise"),
    ("Implementation", "implementation"),
)
RICH_TEXT_LIMIT = 32768
CARD_TEXT_LIMIT = 5000
CARD_LABELS = (
    "WHAT",
    "VERIFIED",
    "NOT VERIFIED",
    "WHO",
    "MARKET TRUTH",
    "OUTLOOK",
    "WHAT TO DO",
    "WHAT CHANGES THE ACTION",
    "CONFIDENCE",
)


class BoldText(DomainModel):
    type: Literal["bold"] = "bold"
    text: str


class ParagraphBlock(DomainModel):
    type: Literal["paragraph"] = "paragraph"
    text: str | BoldText | list[str | BoldText]


class TableCell(DomainModel):
    text: str
    align: Literal["left", "center", "right"]
    valign: Literal["top", "middle", "bottom"]
    is_header: bool = False


class ScoreTable(DomainModel):
    type: Literal["table"] = "table"
    is_bordered: bool = True
    is_striped: bool = True
    is_compact: bool = True
    cells: list[list[TableCell]]


class RichMessage(DomainModel):
    blocks: list[ParagraphBlock | ScoreTable]

    def readable_text(self) -> str:
        lines: list[str] = []
        for block in self.blocks:
            if isinstance(block, ScoreTable):
                lines.extend(" | ".join(cell.text for cell in row) for row in block.cells)
            elif isinstance(block.text, str):
                lines.append(block.text)
            elif isinstance(block.text, BoldText):
                lines.append(block.text.text)
            else:
                lines.append("".join(part if isinstance(part, str) else part.text for part in block.text))
        return "\n".join(line for line in lines if line)

    def telegram_payload(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", exclude_none=True)


class IncomingCell(BaseModel):
    """Telegram layout extensions do not participate in text matching."""

    model_config = ConfigDict(strict=True, extra="ignore")
    text: JsonValue


class IncomingListItem(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    label: str = ""
    blocks: list["IncomingBlock"]


class IncomingBlock(BaseModel):
    """Validated wire projection, separate from approved outgoing content."""

    model_config = ConfigDict(strict=True, extra="ignore")
    type: str
    text: JsonValue = None
    expression: JsonValue = None
    caption: JsonValue = None
    summary: JsonValue = None
    cells: list[list[IncomingCell]] | None = None
    items: list[IncomingListItem] | None = None
    blocks: list["IncomingBlock"] | None = None

    @model_validator(mode="after")
    def required_content(self) -> Self:
        if self.type == "table" and self.cells is None:
            raise ValueError("Telegram table requires cells")
        if self.type == "list" and self.items is None:
            raise ValueError("Telegram list requires items")
        if self.type == "details" and self.blocks is None:
            raise ValueError("Telegram details requires blocks")
        if self.type in {"paragraph", "heading"} and self.text is None:
            raise ValueError("Telegram text block requires text")
        return self


class IncomingRichMessage(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")
    blocks: list[IncomingBlock]


def rich_text(value: JsonValue) -> str:
    """Read recursive Telegram wire text without interpreting markup."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(rich_text(part) for part in value)
    if isinstance(value, dict):
        for key in ("text", "alternative_text", "expression"):
            if key in value:
                return rich_text(value[key])
    return ""


def _blocks_text(blocks: list[IncomingBlock]) -> str:
    lines: list[str] = []
    for block in blocks:
        if block.type == "table" and block.cells is not None:
            lines.extend(" | ".join(rich_text(cell.text) for cell in row) for row in block.cells)
        elif block.type == "list" and block.items is not None:
            lines.extend((item.label + " " + _blocks_text(item.blocks)).strip() for item in block.items)
        elif block.blocks is not None:
            if block.summary is not None:
                lines.append(rich_text(block.summary))
            lines.append(_blocks_text(block.blocks))
        else:
            lines.append(rich_text(block.text if block.text is not None else block.expression))
        if block.caption is not None:
            lines.append(rich_text(block.caption))
    return "\n".join(line for line in lines if line)


def rich_message_text(message: Mapping[str, JsonValue]) -> str:
    return _blocks_text(IncomingRichMessage.model_validate(message).blocks)


def message_text(message: Mapping[str, JsonValue]) -> str:
    rich = message.get("rich_message")
    if rich is not None:
        if not isinstance(rich, dict):
            raise ValueError("Telegram rich_message must be an object")
        rendered = rich_message_text(rich)
        if rendered:
            return rendered
    for field in ("text", "caption"):
        value = message.get(field)
        if value is not None:
            if not isinstance(value, str):
                raise ValueError(f"Telegram {field} must be text")
            if value:
                return value
    return ""


def _assessment(value: ScoreAssessment | Mapping[str, JsonValue]) -> ScoreAssessment:
    if isinstance(value, ScoreAssessment):
        return value
    return ScoreAssessment.model_validate({"dims": value["dims"]})


def score_summary(assessment: ScoreAssessment | Mapping[str, JsonValue]) -> str:
    parts: list[str] = []
    for code, dimension in zip("DMPXFSI", _assessment(assessment).dims.ordered(), strict=True):
        parts.append(
            code + str(dimension.value)
            if dimension.value is not None
            else code + " n/a (" + dimension.explanation("estimate not supplied") + ")"
        )
    return "Scores " + " · ".join(parts)


def _score_table(assessment: ScoreAssessment) -> ScoreTable:
    cells = [
        [
            TableCell(text=label, is_header=True, align="left", valign="top")
            for label in ("Dimension", "Score / 10", "Reason")
        ]
    ]
    for (label, _), dimension in zip(SCORE_DIMENSIONS, assessment.dims.ordered(), strict=True):
        cells.append(
            [
                TableCell(text=label, align="left", valign="top"),
                TableCell(
                    text="Not assessed" if dimension.value is None else str(dimension.value),
                    align="center",
                    valign="top",
                ),
                TableCell(text=dimension.explanation("Explanation not supplied"), align="left", valign="top"),
            ]
        )
    return ScoreTable(cells=cells)


def score_table(assessment: ScoreAssessment | Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    return _score_table(_assessment(assessment)).model_dump(mode="json")


def card_rich_message(
    text: str, assessment: ScoreAssessment | Mapping[str, JsonValue], *, max_chars: int = CARD_TEXT_LIMIT
) -> dict[str, JsonValue]:
    """Freeze approved text and scores without changing assessment decisions."""
    scores = _assessment(assessment)
    marker = "\n" + score_summary(scores)
    before, found, after = text.partition(marker)
    if not found or after and not after.startswith("\n"):
        raise ValueError("CARD_LAYOUT: canonical score summary is missing")
    blocks: list[ParagraphBlock | ScoreTable] = []
    for section, part in enumerate((before, after)):
        if section:
            blocks.append(_score_table(scores))
        for index, line in enumerate(part.splitlines()):
            if not line:
                continue
            content: str | BoldText | list[str | BoldText] = line
            if (section == 0 and index == 0) or line.startswith("── ") or line == "WHY IT MATTERS":
                content = BoldText(text=line)
            elif label := next((label for label in CARD_LABELS if line.startswith(label + ":")), None):
                content = [BoldText(text=label + ":"), line[len(label) + 1 :]]
            blocks.append(ParagraphBlock(text=content))
    message = RichMessage(blocks=blocks)
    length = len(message.readable_text())
    limit = min(max_chars, RICH_TEXT_LIMIT)
    block_count = len(blocks) + sum(len(block.cells) for block in blocks if isinstance(block, ScoreTable))
    if length > limit or block_count > 500:
        raise ValueError(
            f"CARD_LENGTH: rich message has {length} characters and {block_count} blocks; "
            f"maximum {limit} characters and 500 blocks"
        )
    return message.telegram_payload()
