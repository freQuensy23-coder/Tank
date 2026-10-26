"""Explicit refusal trap for fixtures that cannot legitimately close a reader question."""

from tank.contracts.session_followups import FollowupClosure, FollowupRefusalTarget


class UnexpectedFollowupRefusals:
    async def record(self, target: FollowupRefusalTarget, closure: FollowupClosure) -> None:
        raise AssertionError(
            f"Unexpected follow-up refusal for {target.question_id}/{target.session_id}: {closure.reason}"
        )
