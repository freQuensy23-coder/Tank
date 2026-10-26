"""The two observed native event-list wire contracts."""

from pydantic import Field, JsonValue

from tank.contracts import DomainModel

type NativeWireEvent = dict[str, JsonValue]


class ManagedEventPage(DomainModel):
    data: list[NativeWireEvent]
    next_page: str | None = Field(min_length=1)


class NodeEventPage(DomainModel):
    data: list[NativeWireEvent]
    has_more: bool


type NativeEventPage = ManagedEventPage | NodeEventPage
