# Model context and host record identity

Tank's automatic analyst/scout messages and custom-tool contracts use source documents, quotes, project names and event descriptions. Durable identifiers, content hashes, code/prompt/config versions, storage paths and audit metadata stay on the host. Protocol correlation values used by OpenMA itself remain in the transport envelope; they are not business arguments the model must copy.

This change does not remove SQL, sandbox tools, database credentials, the data guide, or the agent's ability to inspect data independently when needed. Those capabilities are unchanged. It cleans the automatic context and ordinary tool arguments, rather than restricting investigation.

## Inputs

The host selects the current event from the session and the triggering publication from the active input. `record_event`, `add_observation`, assessments, decisions, watches, milestones and publication no longer ask the model to repeat those internal keys. `record_review` uses the current proposal. Unknown internal fields are rejected at the model boundary; the existing domain scope checks still execute after host resolution.

A source citation is a document description with a verbatim quote:

```json
{
  "document": {
    "url": "https://example.org/announcement",
    "observed_at": "2026-10-23T10:00:00Z"
  },
  "quote": "The product is live."
}
```

Assessments and beneficiary edges refer to a recorded claim by its quote and, when needed, its subject, predicate and object. They do not invent short numbered aliases. Sources with the same URL must be distinguished by their displayed observation time. Ambiguous or unknown descriptions fail explicitly. The server keeps the lookup catalog in the session so replies survive retries and restarts without fuzzy matching.

`get_event` defaults to the current event. Reading or merging another event requires its displayed statement and public time. Disarming a watch uses its displayed text, condition expression and token, with role and creation time available to distinguish re-armed watches from their predecessors. A duplicate scout proposal is selected by its subject. Tokens use project names and tickers; instruments use venue, market type and symbol.

## Outputs and persistence

`ModelContext` projects incoming publications and custom-tool results. It replaces useful identity-bearing references with human-readable content and drops audit-only fields recursively. Search results retain their article text, URLs, source names and clocks. Literal source quotes and user-supplied questions/filenames are preserved, including hash-shaped public content; metadata redaction must not invalidate evidence or file paths. Jev does not substitute an internal source code when a human-readable publisher name is unavailable. Diagnostic text replaces embedded registry keys and obsolete registry-argument names with display names; verbatim source text and URLs are preserved. Market data retains economic values, units, windows, uncertainty and coverage. The raw results, hashes, relational links and evidence archives remain in the database. Tool logs retain the exact model input in `model_arguments` and the host-resolved domain arguments in `arguments`; replay and causal receipts keep using the latter.

Frozen custom-tool response bodies contain the projected result. A delivery retry reuses that same body. Legacy frozen response bodies are retained separately when a new clean response is prepared. Existing sessions are not reset, and previously sent history is not retroactively erased. Before another callback or a pending custom-tool continuation, an idle native session receives the current analyst or scout tools and a notice to follow their current definitions. Busy sessions defer migration; a rejected update is retried without advancing the stored contract version. Pre-deployment turns retain a host-only compatibility path through the original domain schemas and scope checks, while later turns reject identifier arguments. Their replies are still projected without identifiers. Session creation identities include the model-context version, so an orphaned old remote snapshot cannot be misclassified as a new session.

Telegram attachments are uploaded and mounted by the host. The model receives a readable file path instead of a SQL command containing the attachment's storage key. The file bytes and existing database copy are preserved.

Public schemas live with the model skills. Internal domain validation schemas live in `config/tool_contracts`, outside the uploaded skill files. Existing domain services continue to use their internal record identifiers; changing the model interface does not change the event state machine or scoring rules.

## Verification

Boundary tests exercise actual event creation and source/claim binding, recursive metadata removal, ambiguous references, event selection, watch cancellation, scout duplicates and meaningful registry names. Lifecycle, publication, retry, replay and attachment tests cover the changed model inputs while retaining their original domain assertions. The data guide and sandbox database configuration are preserved.
