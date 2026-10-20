"""pytest plugin (evidence only): keep a copy of the E2E database before the test drops it.

Before the fixture runs DROP DATABASE tank_sse_*, the database is cloned as
KEEP_E2E_DATABASE (a tank_test_* name) so the CLI can be checked against real cards.
"""

import os
import re

import psycopg

_execute = psycopg.AsyncConnection.execute


async def _keeping_execute(self, query, params=None, **kwargs):
    text = query.as_string(self) if hasattr(query, "as_string") else str(query)
    match = re.match(r"DROP DATABASE \"?(tank_sse_[0-9a-f]+)\"?", text)
    target = os.environ.get("KEEP_E2E_DATABASE")
    if match and target and target.startswith("tank_test_"):
        await _execute(self, f"DROP DATABASE IF EXISTS \"{target}\"")
        await _execute(
            self,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",
            (match.group(1),),
        )
        await _execute(self, f"CREATE DATABASE \"{target}\" TEMPLATE \"{match.group(1)}\"")
        os.environ.pop("KEEP_E2E_DATABASE")
    return await _execute(self, query, params, **kwargs)


psycopg.AsyncConnection.execute = _keeping_execute

# Optional: start the synthetic chain KEEP_E2E_SHIFT_HOURS in the past, so the
# outlook horizon of its cards has already passed when the CLI reads them.
_shift = os.environ.get("KEEP_E2E_SHIFT_HOURS")
if _shift:
    import datetime as _dt

    _real_datetime = _dt.datetime

    class _ShiftedDatetime(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            return _real_datetime.now(tz) - _dt.timedelta(hours=float(_shift))

    _dt.datetime = _ShiftedDatetime
