# Foundation refactor evidence

This is a progress record, not a claim that the complete foundation scope is finished.

## Completed contracts and adapter tranche

- Shared DomainModel rejects coercion, unknown fields and nonfinite domain numbers; validates defaults and existing instances; freezes attributes; hides raw validation input in diagnostics.
- Collection[T] binds a concrete schema to a collection. Repository[T] provides typed get, put, delete, list, scan, range and latest-before operations. Writes check the runtime schema, serialize with warnings treated as errors, and validate the serialized schema before calling storage. Reads validate before returning a domain model.
- JSON codecs validate finite JSON at actual serialization boundaries. JsonDocument is a wire codec, never an application record schema. JsonInput enumerates Python serialization inputs. Reflection over arbitrary dataclasses, hasattr(model_dump), Any and casts were removed from the serializer.
- Search returns strict SearchHit objects; DDGS mappings are validated at the adapter boundary. Endpoint restrictions, retries, shared rate limits and cancellation draining retain their behavioral tests.
- Settings has typed snapshots, a typed environment-source wrapper, load_config_as, required Thresholds/DeliveryConfiguration and a single configured_secrets method. Contact and chat values come from complete validated configuration. The fixture-only threshold fallback was deleted after its consumers migrated.
- Timestamp is a strict domain model. Precision/occurrence absence requires an explicit reason; bucket anchors and timestamp declarations are validated; contradictory value aliases are rejected. The existing typed positional constructor and explicit flattened fields serialization remain.
- ScoreDimension includes evidence, proposal origin and applied caps. Explicit presentation views prevent a presentation projection from masquerading as the full score domain. Generation provenance has named prompt/skill objects.
- JobMetadata declares lifecycle state and clocks. JobEnvelope is the persisted payload/result wire representation; concrete application commands belong to dispatchers. JobRepository validates before writes and preserves genuinely absent lifecycle fields. Completion clocks must agree with terminal state.
- ClickHouse and market_storage are fully checked by strict mypy, including imports. Their fields, cursors, metadata responses, counts, parameters and batches have precise types. The pure market codec has strict typing and validates primitive SQL representations and nested contribution maps.

## Persistence layering

The former cycle was storage -> market_storage -> clickhouse_market -> market_storage, with market_storage also importing private storage helpers. Query methods were attached to repository classes by assignment.

Pure encoding, decoding, fingerprinting, point merges and batch-key preparation moved to market_codec. ClickHouse depends inward on that codec. Market storage has explicit memory/PostgreSQL capability ports and asks its port for SQL predicates instead of importing storage internals. A literal backend discriminator replaces attribute discovery. Repository query methods are declared on a mixin instead of monkey-patched onto classes. The write-clock index is explicitly initialized.

The physical-store rules remain: exchange series on a production PostgreSQL repository require ClickHouse; memory is the explicit test/replay implementation. No production deployment or service/container modification was performed.

## Exact retained dictionary, absence and default cases

1. SQL table/column/type/alias maps are associative schema metadata. They are not application records. Shadowed legacy market metadata and the retired budget-state entry were removed; final TABLE_COLUMNS and TIMESTAMP_PRECISION_FIELDS were compared against the preceding implementation and were identical.
2. Record means dict[str, JsonValue] only at document persistence boundaries. SQL rows include explicitly typed datetime and Decimal values. Application-facing typed ports return concrete models; remaining application raw-document consumers are not approved as a final domain API.
3. source_observations is keyed by capture source; ticker field_sources is keyed by component name. These are real indexes inside named JSON columns. Missing contributions mean no update; malformed non-object contributions are rejected. Funding source preference and component-clock selection retain their existing semantics.
4. Batch deduplication, physical-key lookup, per-instrument grouping/counts and the write-clock cache are real associative indexes. Zero initializes a count of an empty set. The cache key (collection, None, None) denotes the whole collection. Nullable grouping values preserve SQL NULL groups.
5. A missing record, unknown legacy recorded_at, an unbounded query endpoint, a missing optional bucket anchor, and a lifecycle event that has not occurred are genuine absence. Unknown availability remains visible where consumers classify it, but cannot become an availability dependency head. An empty instrument scope never broadens into all instruments.
6. A requested dependency head that does not exist is None. A request for no heads returns an empty result without querying. Empty SQL IN scopes use (NULL), which matches no rows and avoids invalid IN () syntax.
7. ClickHouse's default username and empty password apply only when credentials are absent. Scheme and hostname are required. Optional injected clients/limiters select defaults only for None. Standard compression and page/retry constants are physical/operational policies, not invented domain evidence.
8. The recent-day latest-point optimization reads older history only for instruments missing a recent point. It returns the same result as the complete requested range.
9. Persisted queue payload/result JSON is a dispatcher wire boundary. No-result jobs and queued jobs genuinely lack a result. Optional recovery, admission, interruption and reporting markers are absent until their respective lifecycle event. JobRetryState reads only execution fields, allowing direct SQL jobs without fabricating missing provenance.
10. Historical session snapshots can genuinely lack an agent identifier. Their required nullable agent_id is never filled with the current configured agent, which would misattribute history.
11. Timestamp aliases and legacy precision names preserve existing declared provider formats and reject contradictions. They never infer EXACT from formatting. Numeric epoch parsing and iso's implicit clock convenience remain on the utility fallback audit until their producers are explicitly classified; typing alone does not close that audit.

### Raw nonfinite market evidence

The existing compact-projection regressions deliberately persist kline c = "NaN". PostgreSQL keeps raw decimal evidence, and ClickHouse keeps the exact numeric text. The document/decoded field is a string, not a valid numeric domain value. market_series_counts and day-count queries explicitly require a finite positive close; the NaN row never becomes a valid counted observation. DomainModel still forbids nonfinite numbers, and JSON floating NaN is rejected before HTTP writes. No domain validator was weakened to retain the raw evidence.

## Validation evidence

All work ran in the isolated server worktree with Python 3.13.13 and the frozen environment prepared using uv 0.11.7.

- Full owned unit selection after integration: 114 passed, zero skips, 2.02 seconds; maximum case 0.284 seconds and p90 0.033 seconds including fixtures.
- Subsequent market/foundation/storage focused selection: 48 passed, zero skips, 1.28 seconds.
- Strict mypy passes contracts/settings/util/time/search/logging/backup and all three market modules. Storage's remaining helpers are explicitly still open.
- Actual physical-store regression selection is every collected integration case in tests/test_storage*.py and tests/test_clickhouse_market.py: 91 passed, zero skips, 11.46 seconds after the final market adapter changes. Isolated PostgreSQL 16 and ClickHouse 25.8 services were used under the shared heavy-job lock.
- Tests cover constructed/mutated invalid-model refusal before writes, wrong runtime schemas, nested nonfinite values, malformed persisted data, missing configuration, exact score projection, lifecycle clocks, idempotent re-enqueue with stored results, real memory-store physical keys, and secret-safe validation errors.
- Evidence files remain in the foundation worktree: foundation-unit.xml, foundation-integration.xml and foundation-integration.log. No credentials are included in this document.

## Remaining owned work

Storage still has the legacy protocol, query wrappers and approximately fifteen query implementations to finish typing and audit. The complete owned test fixture/helper annotation audit remains open beyond the already migrated search/settings/time/backup/foundation tests. The utility implicit-clock and numeric-epoch audit noted above remains open. Final whole-branch verification must follow those changes. Existing SQL migrations and agent_grants.sql remain retained historical/security evidence; no migration or production schema change was introduced.
