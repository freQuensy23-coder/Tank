"""Persist replay evidence coordinates without replacing concurrent session state."""

from tank.agents.session_ownership import require_owned_session_identity
from tank.replay_evidence_contracts import SealedReplayEvidence
from tank.storage import Repository


async def update_replay_session_evidence(
    repo: Repository, session_id: str, evidence: SealedReplayEvidence
) -> None:
    async with repo.lock("session:" + session_id):
        current = await repo.get("ops.agent_sessions", session_id)
        if current is None:
            raise KeyError(session_id)
        require_owned_session_identity(session_id, current)
        current.update(
            replay_evidence_sha256=evidence.manifest_sha256,
            replay_cutoff=evidence.manifest.cutoff,
        )
        await repo.put("ops.agent_sessions", session_id, current)
