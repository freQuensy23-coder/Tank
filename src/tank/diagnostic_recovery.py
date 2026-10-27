from tank.sources.x_diagnostics import legacy_serialization_mismatch, verified_documents
from tank.time_precision import collection_clock, propagate_timestamp
from tank.util import parse_time, sha256, utcnow

LEGACY_RECOVERY_PREFIX = "x-diagnostic-legacy-v1:"


def _source_job_eligible(job):
    return (
        job is not None
        and job.get("type") == "DELIVER"
        and job.get("status") == "FAILED"
        and job.get("last_error") == "DeliveryError"
        and type(job.get("attempts")) is int
        and type(job.get("max_attempts")) is int
        and job["attempts"] >= job["max_attempts"] > 0
        and isinstance(job.get("payload"), dict)
        and job["payload"].get("channel") == "telegram:ops"
        and isinstance(job["payload"].get("notice_id"), str)
        and "_diagnostic_recovery" not in job["payload"]
        and not job.get("idem_key", "").startswith(LEGACY_RECOVERY_PREFIX)
        and job.get("expires_at") is not None
        and parse_time(job["expires_at"]) > utcnow()
    )


def _parent_reference(ledger, chat):
    parents = [row for row in ledger if row.get("status") == "SENT" and row.get("method") != "sendDocument"]
    if not chat or not parents:
        return None
    references = set()
    for row in parents:
        reference = row.get("message_ref")
        if (
            row.get("method") not in {None, "sendMessage"}
            or str(row.get("chat_id") or "") != chat
            or type(reference) not in {str, int}
            or not str(reference).isascii()
            or not str(reference).isdecimal()
            or len(str(reference)) > 20
            or int(reference) <= 0
        ):
            return None
        references.add(str(reference))
    return next(iter(references)) if len(references) == 1 else None


def _pending_documents(attachments, ledger, chat, parent):
    documents = {
        sha256({"filename": document["filename"], "sha256": document["sha256"]}): document
        for document in attachments
    }
    histories = {document_id: [] for document_id in documents}
    for row in ledger:
        if row.get("method") != "sendDocument" and "document_id" not in row:
            continue
        document_id = row.get("document_id")
        if (
            row.get("method") != "sendDocument"
            or not isinstance(document_id, str)
            or document_id not in histories
            or row.get("status") not in {"GENERATED", "FAILED", "SENT"}
            or type(row.get("attempt")) is not int
            or row["attempt"] < 1
            or (
                row.get("status") == "SENT"
                and (
                    str(row.get("chat_id") or "") != chat
                    or str(row.get("request_reply_to_message_ref") or "") != parent
                )
            )
        ):
            return False
        document = documents[document_id]
        if (
            row.get("filename") != document["filename"]
            or row.get("document_sha256") != document["sha256"]
            or type(row.get("document_bytes")) is not int
            or row["document_bytes"] != len(document["content"])
        ):
            return False
        histories[document_id].append(row)
    return any(
        not any(row.get("status") == "SENT" or row.get("terminal") for row in history)
        and max((row["attempt"] for row in history), default=0) < 5
        for history in histories.values()
    )


def validate_recovery_destination(payload, settings):
    proof = payload.get("_diagnostic_recovery")
    if "_diagnostic_recovery" in payload and (
        not isinstance(proof, dict)
        or type(proof.get("version")) is not int
        or proof["version"] != 1
        or payload.get("channel") != "telegram:ops"
        or proof.get("chat_id") != str(getattr(settings, "telegram_ops_chat", None) or "")
    ):
        raise ValueError("Diagnostic recovery destination changed")


async def recover_legacy_diagnostic_job(repo, settings, candidate):
    if not _source_job_eligible(candidate):
        return None
    notice_id = candidate["payload"]["notice_id"]
    idem_key = LEGACY_RECOVERY_PREFIX + notice_id
    async with (
        repo.lock("ops-documents:" + notice_id),
        repo.lock("deliver:" + notice_id + ":telegram:ops"),
        repo.lock("job:" + candidate["job_id"]),
        repo.lock("queue:DELIVER:" + idem_key),
        repo.transaction(),
    ):
        job = await repo.get("ops.jobs", candidate["job_id"])
        if not _source_job_eligible(job) or job["payload"]["notice_id"] != notice_id:
            return None
        existing = await repo.list("ops.jobs", filters={"type": "DELIVER", "idem_key": idem_key}, limit=1)
        if existing:
            return existing[0]
        notice = await repo.get("out.ops_notices", notice_id)
        if not notice or notice.get("kind") != "ENTRY_ERRORS":
            return None
        version = notice.get("document_serialization_version", 1)
        if type(version) is not int or version != 1:
            return None
        try:
            attachments = verified_documents(notice)
            if not legacy_serialization_mismatch(notice):
                return None
        except ValueError:
            return None
        ledger = [
            row
            async for row in repo.iter_all(
                "out.deliveries", filters={"notice_id": notice_id, "channel": "telegram:ops"}
            )
        ]
        chat = str(getattr(settings, "telegram_ops_chat", None) or "")
        parent = _parent_reference(ledger, chat)
        if parent is None or not _pending_documents(attachments, ledger, chat, parent):
            return None
        return await repo.enqueue(
            "DELIVER",
            {
                **job["payload"],
                "_diagnostic_recovery": {
                    "version": 1,
                    "classification": "legacy_serialization_mismatch",
                    "source_job_id": job["job_id"],
                    "source_idem_key": job["idem_key"],
                    "notice_sha256": sha256(notice),
                    "manifest_sha256": sha256(notice["document_manifest"]),
                    "parent_message_ref": parent,
                    "chat_id": chat,
                },
            },
            idem_key,
            priority=job["priority"],
            run_after=collection_clock(utcnow()),
            expires_at=propagate_timestamp(job, "expires_at"),
            max_attempts=job["max_attempts"],
        )
