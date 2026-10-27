"""Project actual authored source/feed facts into the reporting request boundary."""

from collections.abc import Sequence

from tank.contracts.source_errors import SourceEntryError
from tank.contracts.source_reports import SourceReportRequest, SourceReportSubject
from tank.contracts.sources import SourceDefinition, SourceFeed


def report_request(
    source: SourceDefinition, feed: SourceFeed, errors: Sequence[SourceEntryError], seen_at: str
) -> SourceReportRequest:
    return SourceReportRequest(
        subject=SourceReportSubject(source_id=source.source_id, source_name=source.name, feed_url=feed.url),
        errors=list(errors),
        seen_at=seen_at,
    )
