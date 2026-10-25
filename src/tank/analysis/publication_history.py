"""Select publication revisions from actual scoped history without using clock tie-breaks."""

from collections.abc import Iterable, Mapping
from typing import Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel


class PublicationRevisionRef(DomainModel):
    event_id: str = Field(min_length=1, pattern=r"\S")
    alert_id: str = Field(min_length=1, pattern=r"\S")
    version: int = Field(gt=0)
    kind: Literal["PARTIAL", "FULL", "UPDATE", "PRELIMINARY"]

    def storage_key(self) -> str:
        return self.alert_id + ":" + str(self.version)


class PublicationHistory(DomainModel):
    event_id: str = Field(min_length=1, pattern=r"\S")
    revisions: tuple[PublicationRevisionRef, ...]

    @model_validator(mode="after")
    def one_actual_chain(self) -> Self:
        if any(ref.event_id != self.event_id for ref in self.revisions):
            raise ValueError("Publication history contains a different event")
        if len({ref.alert_id for ref in self.revisions}) > 1:
            raise ValueError("Publication history contains ambiguous alert chains")
        versions = [ref.version for ref in self.revisions]
        if len(versions) != len(set(versions)):
            raise ValueError("Publication history contains duplicate revisions")
        return self

    def latest_revision(self) -> PublicationRevisionRef | None:
        return max(self.revisions, key=lambda ref: ref.version, default=None)

    def latest_ordinary(self) -> PublicationRevisionRef | None:
        return max(
            (ref for ref in self.revisions if ref.kind != "PRELIMINARY"),
            key=lambda ref: ref.version,
            default=None,
        )

    def first_full(self) -> PublicationRevisionRef | None:
        return min(
            (ref for ref in self.revisions if ref.kind == "FULL"),
            key=lambda ref: ref.version,
            default=None,
        )


def publication_history_from_records(
    event_id: str, records: Iterable[Mapping[str, JsonValue]]
) -> PublicationHistory:
    revisions: list[PublicationRevisionRef] = []
    for record in records:
        try:
            fields = {key: record[key] for key in ("event_id", "alert_id", "version", "kind")}
        except KeyError as exc:
            raise ValueError("Publication revision is missing a required identity fact") from exc
        revisions.append(PublicationRevisionRef.model_validate(fields))
    return PublicationHistory(event_id=event_id, revisions=tuple(revisions))
