# Legacy X diagnostic serialization fixtures

Public synthetic values in the historical consumer schema and field order (`913a58c2`): request/outcome/attempts/attemptCount; attempt/at/status/retryable/error/response; fixed response fields followed by bodyDocument. The six-header fixture requires bounded reconstruction after JSONB key reordering. The timeout fixture has no response. Neither file has a trailing newline, matching the historical serializer.

These are independent original byte oracles, not production actor OUTPUT, notice identifiers, credentials or retained raw headers. Tests pin their lengths and SHA-256 hashes; production code must not regenerate or replace them to obtain a passing manifest.
