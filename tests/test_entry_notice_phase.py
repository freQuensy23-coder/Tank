"""Final report notices are validated after rendering and before durable effects."""

from pathlib import Path

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict
from test_storage import repo as repo

import tank.ops_entry_errors as reporting
from tank.contracts.source_errors import EntryErrorNotice, EntryErrorRenderContent, ProposalEntryError
from tank.contracts.source_reports import SourceReportRequest, SourceReportSubject
from tank.settings import Settings
from tank.storage import StorageBackend


class NoticeSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def request() -> SourceReportRequest:
    return SourceReportRequest(
        subject=SourceReportSubject(
            source_id="S24", source_name="Declared snapshot", feed_url="https://snapshot.example/graphql"
        ),
        seen_at="2026-10-27T00:00:00Z",
        errors=[ProposalEntryError(reason="PENDING_PROPOSAL_UNAVAILABLE", proposal_id="proposal")],
    )


async def test_invalid_rendered_result_cannot_commit_markers_notice_or_delivery(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = NoticeSettings(data_dir=tmp_path, ops_progress_enabled=True)
    seen: list[EntryErrorRenderContent] = []

    def invalid_render(content: EntryErrorRenderContent) -> int:
        seen.append(content)
        return 7

    with monkeypatch.context() as invalid:
        invalid.setattr(reporting, "render", invalid_render)
        with pytest.raises(ValidationError):
            await reporting.report_entry_errors(repo, settings, request())
    assert len(seen) == 1
    assert seen[0].source_name == "Declared snapshot"
    assert not hasattr(seen[0], "text")
    assert await repo.list("out.ops_notices") == []
    assert await repo.list("ops.jobs") == []

    outcome = await reporting.report_entry_errors(repo, settings, request())
    assert outcome.kind == "RECORDED"
    notice_wire = await repo.get("out.ops_notices", outcome.notice_id)
    assert notice_wire is not None
    notice = EntryErrorNotice.model_validate(notice_wire)
    assert notice.text == reporting.render(seen[0])
    assert notice.text.startswith("TANK OPS · Записи источника не разобраны")
    assert "document_manifest" not in notice_wire
    assert notice.storage_record() == notice_wire
    before = await repo.list("out.ops_notices")
    again = await reporting.report_entry_errors(repo, settings, request())
    assert again.kind == "DEDUPLICATED"
    assert await repo.list("out.ops_notices") == before
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 1
    assert jobs[0]["payload"] == {"notice_id": outcome.notice_id, "channel": "telegram:ops"}


async def test_storage_record_revalidates_a_tampered_admitted_notice(
    repo: StorageBackend, tmp_path: Path
) -> None:
    outcome = await reporting.report_entry_errors(
        repo, NoticeSettings(data_dir=tmp_path, ops_progress_enabled=True), request()
    )
    assert outcome.kind == "RECORDED"
    stored = await repo.get("out.ops_notices", outcome.notice_id)
    assert stored is not None
    notice = EntryErrorNotice.model_validate(stored)
    tampered = notice.model_copy(update={"text": None})
    with pytest.raises(ValidationError):
        tampered.storage_record()
    assert await repo.get("out.ops_notices", outcome.notice_id) == stored


@pytest.mark.parametrize("missing_fact", ["source_name", "entry_errors"])
def test_render_content_requires_actual_nonempty_source_name_and_errors(missing_fact: str) -> None:
    actual = request()
    fields = {
        "notice_id": "entry_errors_declared",
        "source_id": actual.subject.source_id,
        "source_name": actual.subject.source_name,
        "feed_url": actual.subject.feed_url,
        "seen_at": actual.seen_at,
        "entry_errors": [error.model_dump(mode="json") for error in actual.errors],
    }
    if missing_fact == "source_name":
        fields[missing_fact] = ""
    else:
        fields[missing_fact] = []
    with pytest.raises(ValidationError):
        EntryErrorRenderContent.model_validate(fields)
