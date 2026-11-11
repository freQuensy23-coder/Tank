# Tank token research context

PostgreSQL is the sole permanent source of token Markdown content and revisions. Migration 012 seeds the 22 original Google Docs plain-text exports byte for byte, so deployment needs no Google Drive access and no secrets.

1. Migrate the database via the normal deployment process.
2. Run: tank token-context status. Empty files are created virtually for any absent registered token.
3. Test a new agent session and a verified token-context proposal in Tank Ops, then approve it and verify its PostgreSQL revision.

Every new OpenMA analyst session receives read-only Markdown snapshots at /workspace/token-context/<token_id>.md. Already-running sessions keep their snapshots. Event analysts must read affected files after identifying token relevance, before assessing economic effects.

An analyst edit proposal requires accepted verified event evidence. An automatic edit substitutes lines in place: it keeps every Markdown heading and plain-text section title, adds no line to a replaced block and lengthens the text by at most 1,000 characters; appending or adding sections needs an express user request in a follow-up. A proposal whose diff does not fit one Ops image (360 rows of 110 characters, long lines wrapped) is refused. The Ops channel presents a one-sentence reason, a diff PNG and Approve/Agree buttons. Only Ops administrators can approve. A stale version fails without merging or writing anything.

No production changes should be made manually over SSH. Release only with the manual Deploy workflow. Live OpenMA and Telegram integration tests remain required before production acceptance.
