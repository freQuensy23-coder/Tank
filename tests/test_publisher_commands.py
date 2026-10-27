from pathlib import Path

import pytest
from publisher_state_support import rig
from pydantic import ValidationError

from tank.deployment.commands import PublisherCommand, execute_command
from tank.deployment.publisher import PublisherStateError


def test_closed_commands_drive_actual_durable_publication_lifecycle(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    sha = fixture.publisher.sha
    prepared = execute_command(PublisherCommand(action="prepare", release_sha=sha), fixture.publisher)
    pending = fixture.journal.read_transaction()
    assert pending is not None
    candidate = pending.candidate
    assert prepared.lines == (
        f"worker prepared release={sha} digest={candidate.source_digest} build={candidate.build_id}",
    )
    assert execute_command(
        PublisherCommand(action="recovery_release", release_sha=sha), fixture.publisher
    ).lines == (sha,)
    assert (
        execute_command(PublisherCommand(action="activate", release_sha=sha), fixture.publisher).lines == ()
    )
    committed = execute_command(PublisherCommand(action="commit", release_sha=sha), fixture.publisher)
    assert committed.lines == (f"worker committed release={sha} build={candidate.build_id}",)
    fixture.marker.path.write_text(sha + "\n")
    assert execute_command(PublisherCommand(action="finish", release_sha=sha), fixture.publisher).lines == ()
    assert fixture.journal.read_transaction() is None
    assert fixture.api.direct == candidate.build_id


def test_unknown_action_and_mismatched_identity_refuse_before_effects(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    with pytest.raises(ValidationError):
        PublisherCommand.model_validate({"action": "__init__", "release_sha": fixture.publisher.sha})
    with pytest.raises(PublisherStateError, match="command release mismatch"):
        execute_command(PublisherCommand(action="prepare", release_sha="c" * 40), fixture.publisher)
    assert fixture.api.lookups == []
    assert fixture.api.uploads == []
    assert fixture.journal.read_transaction() is None


def test_command_rollback_preserves_previous_identity_and_recovery_observations(tmp_path: Path) -> None:
    fixture = rig(tmp_path, fault="NONE")
    sha = fixture.publisher.sha
    previous = fixture.journal.read_active()
    assert previous is not None
    execute_command(PublisherCommand(action="prepare", release_sha=sha), fixture.publisher)
    observed = execute_command(
        PublisherCommand(action="previous_release", release_sha=sha), fixture.publisher
    )
    assert observed.lines == (previous.release_sha,)
    execute_command(PublisherCommand(action="activate", release_sha=sha), fixture.publisher)
    restored = execute_command(PublisherCommand(action="rollback", release_sha=sha), fixture.publisher)
    assert restored.lines == (f"worker restored build={previous.build_id}",)
    assert fixture.api.direct == previous.build_id
    execute_command(PublisherCommand(action="rollback_finish", release_sha=sha), fixture.publisher)
    assert fixture.journal.read_transaction() is None
    assert (
        execute_command(PublisherCommand(action="rollback", release_sha=sha), fixture.publisher).lines == ()
    )
