"""Mechanical stored-document and native-column read envelopes."""

from tank.contracts import DomainModel
from tank.models import Record


class ImmutableDocument(DomainModel):
    document: Record
    immutable: bool


class NativeBackfillWitness(DomainModel):
    """Existing decoded native columns, without invented document or immutable metadata."""

    record: Record
