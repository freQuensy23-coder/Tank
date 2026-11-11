"""PostgreSQL-backed, approval-gated token research used by OpenMA analyst sandboxes."""

import difflib
import html
import re
import subprocess
from datetime import timedelta


from tank.time_precision import collection_clock, offset_schedule
from tank.util import new_id, sha256, utcnow

FOLDER = "/workspace/token-context"
MAX_MARKDOWN_BYTES = 180_000
MAX_REASON_LENGTH = 240
TOKEN_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,95}$")


def filename(token_id):
    if not TOKEN_NAME.fullmatch(token_id):
        raise ValueError("Invalid registry token key for a context file")
    return f"{FOLDER}/{token_id}.md"


def revision(markdown):
    return sha256(markdown)


def registry_tokens(settings):
    return {row["token_id"]: row for row in settings.load_config("registry")["tokens"]}


def check_markdown(markdown):
    if not isinstance(markdown, str) or len(markdown.encode("utf-8")) > MAX_MARKDOWN_BYTES:
        raise ValueError("Token context must be UTF-8 Markdown under 180 KB")
    if "\x00" in markdown:
        raise ValueError("Token context cannot contain NUL")
    return markdown.replace("\r\n", "\n")


def headings(markdown):
    return [line.strip() for line in markdown.splitlines() if re.match(r"^#{1,6} ", line)]


def change_lines(old, new):
    removed = [line for line in difflib.ndiff(old.splitlines(), new.splitlines()) if line.startswith("- ")]
    added = [line for line in difflib.ndiff(old.splitlines(), new.splitlines()) if line.startswith("+ ")]
    return removed, added


def reason_sentence(reason):
    reason = " ".join(str(reason or "").split())
    if not reason or len(reason) > MAX_REASON_LENGTH:
        raise ValueError("Give one concise sentence explaining why the facts changed")
    if len(re.findall(r"[.!?](?=\s|$)", reason)) > 1:
        raise ValueError("The Ops explanation must contain at most one sentence")
    return reason


def diff_svg(token_id, old, new):
    """Render a readable GitHub-style unified diff; long changes are explicitly clipped."""
    raw = list(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile="a/" + token_id + ".md",
            tofile="b/" + token_id + ".md",
            n=2,
            lineterm="",
        )
    )
    lines = raw[2:]
    shown = lines[:54]
    if len(lines) > len(shown):
        shown.append(f"… {len(lines) - len(shown)} additional diff lines")
    width, height = 1200, 100 + 24 * max(1, len(shown)) + 25
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<rect width="100%" height="64" fill="#f6f8fa"/>',
        '<rect x="0" y="63" width="100%" height="1" fill="#d0d7de"/>',
        '<text x="24" y="26" font-size="19" font-family="DejaVu Sans, sans-serif" '
        'font-weight="bold" fill="#24292f">Token research update</text>',
        f'<text x="24" y="51" font-size="16" font-family="DejaVu Sans Mono, monospace" '
        f'fill="#57606a">{html.escape(token_id)}.md</text>',
    ]
    for idx, value in enumerate(shown):
        y = 77 + idx * 24
        bg, fg = "#ffffff", "#24292f"
        if value.startswith("+"):
            bg, fg = "#dafbe1", "#116329"
        elif value.startswith("-"):
            bg, fg = "#ffebe9", "#82071e"
        elif value.startswith("@@"):
            bg, fg = "#ddf4ff", "#0550ae"
        elif value.startswith("…"):
            bg, fg = "#f6f8fa", "#57606a"
        line = "".join(c for c in value.replace("\t", "    ") if ord(c) >= 32)
        if len(line) > 110:
            line = line[:109] + "…"
        out.append(f'<rect y="{y - 17}" width="{width}" height="24" fill="{bg}"/>')
        out.append(
            f'<text x="24" y="{y}" font-size="15" font-family="DejaVu Sans Mono, monospace" '
            f'fill="{fg}" xml:space="preserve">{html.escape(line)}</text>'
        )
    out.append("</svg>")
    return "\n".join(out)


def diff_png(token_id, old, new):
    svg = diff_svg(token_id, old, new)
    try:
        result = subprocess.run(
            ["rsvg-convert", "--format", "png"],
            input=svg.encode("utf-8"),
            capture_output=True,
            check=True,
            timeout=12,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise RuntimeError("GitHub-style diff image could not be rendered") from error
    return result.stdout


class TokenContextService:
    def __init__(self, repo, settings):
        self.repo, self.settings = repo, settings

    async def read(self, token_id):
        if token_id not in registry_tokens(self.settings):
            raise ValueError("Unknown registry token")
        value = await self.repo.get("reg.token_context", token_id)
        return value or {"token_id": token_id, "markdown": "", "revision": revision("")}

    async def snapshot(self):
        result = {}
        for token_id in registry_tokens(self.settings):
            result[filename(token_id)] = (await self.read(token_id))["markdown"]
        return result

    async def import_text(self, token_id, markdown):
        markdown = check_markdown(markdown)
        if token_id not in registry_tokens(self.settings):
            raise ValueError("Unknown registry token")
        async with self.repo.lock("token-context:" + token_id), self.repo.transaction():
            old = await self.read(token_id)
            if old["markdown"] == markdown:
                return {"token_id": token_id, "changed": False, "revision": old["revision"]}
            await self.repo.put(
                "reg.token_context",
                token_id,
                {
                    "token_id": token_id,
                    "markdown": markdown,
                    "revision": revision(markdown),
                    **collection_clock(utcnow()).fields("updated_at"),
                    "updated_by": "operator-import",
                },
            )
            return {"token_id": token_id, "changed": True, "revision": revision(markdown)}

    async def propose(self, arguments, context):
        if context.get("role") != "analyst" or not context.get("session_id"):
            raise ValueError("Only an analyst with an active event can propose a token edit")
        if not self.settings.telegram_ops_chat or not self.settings.telegram_token:
            raise ValueError("Token edits require a configured Tank Ops channel")
        token_id = arguments["token_id"]
        if token_id not in registry_tokens(self.settings):
            raise ValueError("Unknown registry token")
        event_id = context.get("event_id")
        event = await self.repo.get("evt.events", event_id or "")
        if event is None:
            raise ValueError("Token-context changes must cite the current recorded event")
        followup = context.get("trigger_kind") == "USER_QUESTION"
        expansion = bool(arguments.get("user_requested_expansion"))
        if expansion and not followup:
            raise ValueError("Extra sections or additions require an explicit user follow-up")
        if not expansion:
            if token_id not in event.get("tokens_mentioned", []):
                edges = await self.repo.list("evt.beneficiary_edges", filters={"event_id": event_id})
                if token_id not in {edge.get("token_id") for edge in edges}:
                    raise ValueError("The current event must affect this token")
            evidence_ids = arguments.get("claim_ids") or []
            if not evidence_ids:
                raise ValueError("An automatic edit needs verified claims from the current event")
            for claim_id in evidence_ids:
                claim = await self.repo.get("evt.claims", claim_id)
                if (
                    not claim
                    or claim.get("event_id") != event_id
                    or not claim.get("accepted")
                    or not claim.get("verified")
                ):
                    raise ValueError("Every cited claim must be accepted and verified for this event")
        reason = reason_sentence(arguments["reason"])
        replacement = check_markdown(arguments["updated_markdown"])
        current = await self.read(token_id)
        original = current["markdown"]
        if replacement == original:
            raise ValueError("The proposed Markdown is unchanged")
        if not expansion:
            if not original.strip():
                raise ValueError("Adding initial research requires an explicit user follow-up")
            if headings(original) != headings(replacement):
                raise ValueError("Automatic edits cannot add, remove or rename Markdown sections")
            removed, added = change_lines(original, replacement)
            if not removed or not added:
                raise ValueError("Automatic edits must replace outdated facts, not append new blocks")
        proposal_id = new_id("tcp")
        notice_id = "token-context-" + proposal_id
        stamp = collection_clock(utcnow())
        proposal = {
            "proposal_id": proposal_id,
            "token_id": token_id,
            "event_id": event_id,
            "session_id": context["session_id"],
            "status": "PENDING",
            "base_revision": current["revision"],
            "old_markdown": original,
            "updated_markdown": replacement,
            "reason": reason,
            "claim_ids": arguments.get("claim_ids") or [],
            "user_requested_expansion": expansion,
            **stamp.fields("created_at"),
        }
        async with self.repo.transaction():
            await self.repo.put("reg.token_context_proposals", proposal_id, proposal)
            await self.repo.put(
                "out.ops_notices",
                notice_id,
                {
                    "notice_id": notice_id,
                    "kind": "TOKEN_CONTEXT_PROPOSAL",
                    "subject_id": proposal_id,
                    "text": reason,
                    **stamp.fields("created_at"),
                    "created_by": "agent-token-context",
                },
                immutable=True,
            )
            await self.repo.enqueue(
                "DELIVER",
                {"notice_id": notice_id, "channel": "telegram:ops"},
                notice_id,
                priority=1,
                run_after=stamp,
                max_attempts=5,
                expires_at=offset_schedule(stamp, timedelta(days=7)),
            )
        return {"status": "PENDING_HUMAN_APPROVAL", "token_id": token_id, "applied": False}

    async def decide(self, proposal_id, action, *, chat_id, message_id, user_id):
        if action not in {"approve", "reject", "agree"}:
            raise ValueError("Unknown token-context decision")
        pending = await self.repo.get("reg.token_context_proposals", proposal_id)
        if pending is None:
            raise ValueError("Token-context proposal not found")
        token_id = pending["token_id"]
        async with self.repo.lock("token-context:" + token_id), self.repo.transaction():
            proposal = await self.repo.get("reg.token_context_proposals", proposal_id)
            if proposal["status"] != "PENDING":
                raise ValueError("This proposal has already been decided")
            if str(proposal.get("ops_chat_id")) != str(chat_id) or str(proposal.get("ops_message_id")) != str(
                message_id
            ):
                raise ValueError("Approval must come from the proposal message in Tank Ops")
            if action != "reject":
                current = await self.read(token_id)
                if current["revision"] != proposal["base_revision"]:
                    raise ValueError("Stale token-context revision: apply failed; nothing was changed")
                new_text = proposal["updated_markdown"]
                await self.repo.put(
                    "reg.token_context",
                    token_id,
                    {
                        "token_id": token_id,
                        "markdown": new_text,
                        "revision": revision(new_text),
                        **collection_clock(utcnow()).fields("updated_at"),
                        "updated_by": "ops-approval",
                        "proposal_id": proposal_id,
                    },
                )
            decision = "REJECTED" if action == "reject" else "APPROVED"
            await self.repo.put(
                "reg.token_context_proposals",
                proposal_id,
                {
                    **proposal,
                    "status": decision,
                    "decided_by": str(user_id),
                    **collection_clock(utcnow()).fields("decided_at"),
                },
            )
        return {"status": decision, "token_id": token_id}
