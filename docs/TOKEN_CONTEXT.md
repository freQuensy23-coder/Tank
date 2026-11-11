# Tank token research context

The existing Google Drive folder provides a one-time initial import. PostgreSQL is the sole permanent source of token Markdown content and revisions; no local or Google Drive copies are subsequently synchronized.

1. Migrate the database via the normal deployment process.
2. Obtain a short-lived Google OAuth access token with access to the Drive folder and its Docs.
3. Run: GOOGLE_DRIVE_ACCESS_TOKEN=<oauth-token> tank token-context seed-drive --folder-id https://drive.google.com/drive/folders/1T8V-wQ76lnRLT-j2fKiN55STpcNAq2sB
4. Run: tank token-context status. Empty files are created virtually for any absent registered token.
5. Test a new agent session and a verified token-context proposal in Tank Ops, then approve it and verify its PostgreSQL revision.

Every new OpenMA analyst session receives read-only Markdown snapshots at /workspace/token-context/<token_id>.md. Already-running sessions keep their snapshots. Event analysts must read affected files after identifying token relevance, before assessing economic effects.

An analyst edit proposal requires accepted verified event evidence. It cannot append or add sections unless a user expressly requests that in a follow-up. The Ops channel presents a one-sentence reason, a diff PNG and Approve/Agree buttons. Only Ops administrators can approve. A stale version fails without merging or writing anything.

No production changes should be made manually over SSH. Release only with the manual Deploy workflow. Live OpenMA and Telegram integration tests remain required before production acceptance.
