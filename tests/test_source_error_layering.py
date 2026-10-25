"""Source admission remains outside reporting and byte presentation."""

import json
import subprocess
import sys

import pytest
from pydantic import JsonValue
from test_x_diagnostic_admission import recorded_diagnostics

from tank.contracts.source_errors import AccountEntryError, XDiagnosticResponse
from tank.diagnostic_documents import documents
from tank.sources.x_diagnostics import admit_source_errors
from tank.util import sha256


@pytest.mark.parametrize(
    "module",
    ["tank.contracts.source_errors", "tank.diagnostic_documents", "tank.ops_entry_errors", "tank.delivery"],
)
def test_downstream_import_does_not_initialize_source_application(module: str) -> None:
    program = (
        "import importlib,json,sys;"
        f"importlib.import_module({module!r});"
        "print(json.dumps([name for name in sys.modules if name=='tank.sources' or name.startswith('tank.sources.')]))"
    )
    completed = subprocess.run(
        [sys.executable, "-B", "-c", program], check=True, capture_output=True, text=True, timeout=8
    )
    assert json.loads(completed.stdout) == []


def test_admission_sanitizes_once_before_typed_reporting_and_documents() -> None:
    secret = "synthetic-source-boundary-secret"
    original = recorded_diagnostics()
    response = XDiagnosticResponse(
        status=503,
        headers={"content-type": "text/plain", "authorization": secret},
        body="prefix " + secret + " suffix",
        bodyEncoding="utf-8",
        bodyBytes=len(("prefix " + secret + " suffix").encode()),
        bodySha256=sha256(("prefix " + secret + " suffix").encode()),
        bodyTruncated=False,
        redacted=False,
    )
    original = type(original).model_validate(
        original.model_copy(
            update={"attempts": [original.attempts[0].model_copy(update={"response": response})]}
        )
    )
    wire: dict[str, JsonValue] = {
        "account": "alice",
        "reason": "PUBLIC_PROFILE_HTTP_503",
        "diagnostics": original.model_dump(mode="json"),
    }
    admitted = admit_source_errors([wire], secret)
    assert len(admitted) == 1 and isinstance(admitted[0], AccountEntryError)
    assert secret not in admitted[0].model_dump_json()
    assert secret in original.model_dump_json()
    attachments = documents(admitted)
    assert len(attachments) == 2
    assert attachments[0].content == b"prefix [REDACTED] suffix"
    assert all(secret.encode() not in attachment.content for attachment in attachments)
    assert documents(admit_source_errors(admitted, secret)) == attachments
