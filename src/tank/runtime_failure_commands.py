"""Build terminal effects from the actual closed command and owned queue outcome."""

from tank.contracts.jobs import JobEnvelope
from tank.contracts.progress import FailureProgressTarget, JobFailureProgressView
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.runtime_contracts import (
    FailedAnswerNotification,
    FailedDeliveryNotification,
    FailedQuestionNotification,
    FailureJobHeader,
    FailureReason,
    NoFailureProgress,
    NoTerminalDelivery,
    ScoutMessage,
    TerminalDeliveryNotification,
    TerminalJobFailure,
    UserQuestionMessage,
    agent_message_target,
)
from tank.runtime_dispatch_contracts import (
    AgentMessageCommand,
    ClaimedWork,
    DeliverCommand,
    EscalateTokensCommand,
    FilterItemCommand,
    MarketTruthCommand,
    RouteItemCommand,
    TelegramAnswerCommand,
)
from tank.util import sha256


def terminal_work_failure(
    work: ClaimedWork,
    terminal: JobEnvelope,
    reason: FailureReason,
    *,
    progress_requested: bool,
    notifications_requested: bool,
) -> TerminalJobFailure:
    work = ClaimedWork.model_validate(work, by_name=True)
    terminal = JobEnvelope.model_validate(terminal)
    status = terminal.status
    if status != "FAILED" and status != "EXPIRED":
        raise ValueError("Worker terminal effects require an actual terminal queue outcome")
    if (
        terminal.job_id != work.job.job_id
        or terminal.type != work.job.type
        or terminal.claim_token != work.job.claim_token
        or terminal.attempts != work.job.attempts
        or sha256(terminal.payload) != work.payload_revision
    ):
        raise ValueError("Worker terminal effects differ from their actual claimed command")
    header = FailureJobHeader(job_id=terminal.job_id, job_type=terminal.type, status=status)
    command = work.command
    notification: TerminalDeliveryNotification
    if not notifications_requested:
        notification = NoTerminalDelivery(kind="NONE", reason="NOT_REQUESTED")
    elif isinstance(command, DeliverCommand):
        notification = FailedDeliveryNotification.model_validate(
            {"kind": "DELIVERY", "payload": command.payload, "attempt": terminal.attempts}, by_name=True
        )
    elif isinstance(command, TelegramAnswerCommand):
        notification = FailedAnswerNotification.model_validate(
            {"kind": "ANSWER", "payload": command.payload}, by_name=True
        )
    elif isinstance(command, AgentMessageCommand) and isinstance(command.payload, UserQuestionMessage):
        notification = FailedQuestionNotification(
            kind="QUESTION",
            target=FollowupRefusalTarget(
                question_id=command.payload.question_id, session_id=command.payload.session_id
            ),
        )
    else:
        notification = NoTerminalDelivery(kind="NONE", reason="NOT_APPLICABLE")
    progress: JobFailureProgressView | NoFailureProgress
    if not progress_requested:
        progress = NoFailureProgress(reason="DISABLED")
    elif isinstance(command, (FilterItemCommand, RouteItemCommand)):
        progress = JobFailureProgressView(
            job_id=terminal.job_id,
            type=command.type,
            status=terminal.status,
            target=FailureProgressTarget(item_id=command.payload.item_id),
        )
    elif isinstance(command, (EscalateTokensCommand, MarketTruthCommand)):
        progress = JobFailureProgressView(
            job_id=terminal.job_id,
            type=command.type,
            status=terminal.status,
            target=FailureProgressTarget(event_id=command.payload.event_id),
        )
    elif isinstance(command, AgentMessageCommand) and not isinstance(
        command.payload, (ScoutMessage, UserQuestionMessage)
    ):
        target = agent_message_target(command.payload)
        progress = JobFailureProgressView(
            job_id=terminal.job_id,
            type="AGENT_MESSAGE",
            status=terminal.status,
            target=FailureProgressTarget(item_id=target.item_id, event_id=target.event_id),
        )
    else:
        progress = NoFailureProgress(reason="NOT_APPLICABLE")
    return TerminalJobFailure.model_validate(
        {
            "header": header,
            "reason": reason,
            "notify_delivery": notifications_requested,
            "report_progress": progress_requested,
            "notification": notification,
            "progress": progress,
        },
        by_name=True,
    )
