# Telegram card formatting

New cards use Telegram Bot API sendRichMessage with rich_message.blocks. The score line becomes an InputRichBlockTable with full dimension names, scores out of 10 and the saved explanation for all seven dimensions. Missing scores are “Not assessed”, while zero remains zero. Labels follow the card's existing English language; explanations are preserved as supplied by the analyst.

The table has headers, borders, striped rows and compact cell spacing. Agent-authored strings are plain RichText strings, never parsed as HTML or Markdown. Paragraph emphasis is expressed with explicit bold objects.

Publication freezes the entire rich payload in both the immutable alert and its evidence bundle. Delivery sends that saved payload, including on retries. Existing delivery receipts, idempotency, call activation and replies to the first card remain in force. Historical alerts without a rich payload retain their original sendMessage path.

Tank sets no card length limit of its own. The rich check includes table explanations and enforces only Telegram's own ceiling of 32,768 characters and 500 blocks; a card above it is refused at publication with the counts, without silently dropping evidence. The existing plain card_text remains available for historical consumers; Telegram follow-ups use the frozen rich text when present.

Telegram replies and forwards may carry rich_message instead of text. The inbox flattens native text, paragraphs, tables, nested blocks and lists before matching a forwarded card and composing the analyst's reference context.

The main branch now contains code and specifications. CI runs for main and pull requests; deployment remains a manually invoked workflow on main.

API references, checked 23 October 2026:

- https://core.telegram.org/bots/api#sendrichmessage
- https://core.telegram.org/bots/api#inputrichblocktable
- https://core.telegram.org/bots/api#richblocktablecell
- https://core.telegram.org/bots/api#rich-message-limits

Validation covers rendering, literal markup safety, missing and zero scores, the size ceiling, frozen delivery and retry/thread receipts, publication persistence and forwarded rich-card follow-ups. Automated tests use synthetic Telegram responses; client rendering requires an operator-visible preview after deployment.
