"""Project real claimed commands into the existing typed session admission policy."""

from tank.contracts.session_admission import AdmissionCandidate, AdmissionOrder, SessionTarget
from tank.runtime_contracts import agent_message_target
from tank.runtime_dispatch_contracts import AgentMessageCommand, ClaimedWork, ScoutReviewCommand
from tank.util import parse_time


def worker_admission(work: ClaimedWork) -> AdmissionCandidate | None:
    work = ClaimedWork.model_validate(work, by_name=True)
    command = work.command
    if isinstance(command, AgentMessageCommand):
        target = agent_message_target(command.payload)
    elif isinstance(command, ScoutReviewCommand):
        target = SessionTarget(
            session_id=None, event_id=None, item_id=None, proposal_id=command.payload.proposal_id
        )
    else:
        return None
    since = work.job.slot_waiting_since
    if since is None:
        since = work.job.created_at
    return AdmissionCandidate(
        job_id=work.job.job_id,
        target=target,
        order=AdmissionOrder(priority=work.job.priority, since=parse_time(since)),
    )
