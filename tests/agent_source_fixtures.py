"""Complete synthetic publisher records used by agent and routing boundary tests."""

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote

from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.configuration import Thresholds
from tank.contracts.source_items import FeedSourceItem, parse_feed_source_item
from tank.settings import Settings
from tank.sources.policy import initial_relevance, is_filtered_source
from tank.util import iso


def source_item_fixture(
    item_id: str,
    *,
    source_id: str = "S01",
    source: str = "Synthetic Publisher",
    author: str | None = None,
    title: str = "",
    text: str = "",
    **fields: JsonValue,
) -> FeedSourceItem:
    role = fields.get("author_role")
    subtype = fields.get("subtype")
    if role is not None and not isinstance(role, str):
        raise ValueError("Synthetic source author_role must be text or null")
    if subtype is not None and not isinstance(subtype, str):
        raise ValueError("Synthetic source subtype must be text or null")
    at = iso()
    record: dict[str, JsonValue] = {
        "item_id": item_id,
        "source_id": source_id,
        "external_id": item_id,
        "url": "https://example.test/source/" + quote(item_id, safe=""),
        "feed": "synthetic-fixture",
        "source": source,
        "source_name": source,
        "author": source if author is None else author,
        "author_role": None,
        "original_author_role": None,
        "author_organization": None,
        "monitored_accounts": [],
        "title": title,
        "text": text,
        "published_at": None,
        "published_prec": None,
        "published_prec_reason": "SYNTHETIC_SOURCE_DID_NOT_DECLARE_PUBLICATION_PRECISION",
        "deferrable": is_filtered_source(source_id, subtype, role),
        "raw_ref": "/synthetic-fixture/raw",
        "raw_sha256": "a" * 64,
        "content_hash": "b" * 64,
        "observed_at": at,
        "observed_at_prec": "COLLECTION_ONLY",
        "observed_at_prec_reason": None,
        "source_class": "PRIMARY",
        "relevance": initial_relevance(source_id, subtype, role).model_dump(mode="json"),
        "extraction_status": "PENDING",
        "observation_count": 1,
        "config_version": "synthetic-fixture",
        "code_version": "synthetic-fixture",
    }
    if source_id == "S29" and "monitored_author" not in fields:
        record["monitored_author"] = source if author is None else author
    if fields.get("published_prec") is not None and "published_prec_reason" not in fields:
        record["published_prec_reason"] = None
    if fields.get("published_prec") == "COLLECTION_ONLY" and "published_at" not in fields:
        record["published_at"] = at
    record.update(fields)
    return parse_feed_source_item(record)


def _text(record: Mapping[str, JsonValue], field: str, preset: str) -> str:
    if field not in record:
        return preset
    value = record[field]
    if not isinstance(value, str):
        raise ValueError(f"Synthetic source {field} must be text")
    return value


def source_record_fixture(record: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    if "item_id" not in record:
        raise ValueError("Synthetic source fixture requires its item identity")
    item_id = _text(record, "item_id", "")
    source_id = _text(record, "source_id", "S01")
    source = _text(record, "source", "Synthetic Publisher")
    author = _text(record, "author", source)
    title = _text(record, "title", "")
    text = _text(record, "text", "")
    fields = {
        key: value
        for key, value in record.items()
        if key not in {"item_id", "source_id", "source", "author", "title", "text"}
    }
    return source_item_fixture(
        item_id, source_id=source_id, source=source, author=author, title=title, text=text, **fields
    ).storage_record()


class _FixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@dataclass
class PipelineFixtureSettings:
    configurations: dict[str, dict[str, JsonValue]]
    opencode_api_key: str | None
    replay: bool
    config_version: str
    code_version: str

    def load_config(self, name: str) -> dict[str, JsonValue]:
        return self.configurations[name]

    def thresholds(self) -> Thresholds:
        baseline = _FixtureSettings().thresholds().model_dump(mode="json")
        return Thresholds.model_validate({**baseline, **self.configurations["thresholds"]})


def pipeline_settings_fixture(
    configurations: dict[str, dict[str, JsonValue]],
    *,
    opencode_api_key: str | None = None,
    replay: bool = False,
    config_version: str = "test",
    code_version: str = "test",
) -> PipelineFixtureSettings:
    """Declare complete synthetic test settings without changing runtime configuration policy."""
    return PipelineFixtureSettings(configurations, opencode_api_key, replay, config_version, code_version)
