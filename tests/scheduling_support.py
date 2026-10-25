"""Observe explicit reaction-scheduling intent in unit fixtures."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class RecordingReactionScheduling:
    requested_event_ids: list[str] = field(default_factory=list, init=False)

    async def ensure_event_reactions(self, event_id: str) -> None:
        self.requested_event_ids.append(event_id)
