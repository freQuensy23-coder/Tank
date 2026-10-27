# Measured CI lane refresh

This refresh uses complete reports from CI run `40407818694`. The actual PR test merge is `62d0dd0c8f41cfb401cbfb0e19b7087bc40f0b8b`; its tree `1ab534e14c303fba432b846d4801b42e493a0966` is identical to head `4e2d85773d24139b58232d71c56ad168b08f3fa3`. Independent checks verify the complete union of 4,898 passing units (p90 0.369 s, maximum 5.989 s) and 649 passing integration cases, with zero skips. The workflow exceeded its 240-second budget after lane 0 passed all 163 cases and its completeness check. Queueing, setup, cleanup and aggregate startup still need enough time.

## Authoritative unit calibration

The unchanged `build_unit_plan` consumes both complete unit JUnit reports and the full recorded collection. It refuses missing/duplicate cases, source mismatches and failed timing/skips before producing the plan. The checked-in plan is an exact rebuild; no per-file duration is guessed.

| Input | SHA256 |
| --- | --- |
| Full collection | 734ecac744a02a7d85847b8188c655a8e903fe2665b0ce35c95fc8b268699dc5 |
| Unit report0 | f3d8940b2e943c0d928193c45063ee079c8eeeffaa73839d37be1760744acf03 |
| Unit report1 | fbae730e714cd508ef96641663e8d34acf9f442dfb571d9e9025bfa0a732537e |
| New plan | 82158eb73372195d883d3d6c9d822a5fe0e37530b6afa4f05a44a66c60a070db |

All 307 observed files retain actual case counts and summed case durations. The unchanged deterministic allocator moves 166 complete files between lanes and balances observed sums of 345.989/270.847 to 308.418/308.418 case-seconds. The source remains the actual historical calibration merge; newer files and changed case counts are reported explicitly through the existing coverage mechanism and deterministic unmeasured-file routing. No timing value is invented for the five later YAML cases or another future cohort.

## Minimal integration movement

The existing whole-file SHA routing and replay/reference/workflow/Apify placements remain. Only two additional measured files move to lane 1:

| File | Previous lane | New lane | Cases | Observed case-seconds |
| --- | --- | --- | --- | --- |
| tests/test_milestone_checks.py | 0 | 1 | 21 | 25.292 |
| tests/test_storage.py | 2 | 1 | 36 | 10.493 |

Observed lane costs of 240.763/182.172/227.051 become 215.471/217.957/216.558 case-seconds; the same 649 identities partition into 142/212/295 cases. These are sums from actual case records, not wall-clock predictions or guarantees about queue/network/setup/upload/aggregate time. The next pushed CI provides the actual margin measurement.

## Qualification and retained limits

At source `2e20aa9041fd31ab139a777f815b09c22bc3a856`, the pinned pytest console and approved read-only dependency runner pass 73 selector/budget regressions in 3.99 s (maximum 0.043 s, p90 0.019 s), with zero skips. Actual console collection records 4,900 units (the 4,898 calibration cases plus two placement regressions) and the same 649 integrations. Complete, disjoint selections contain 2,296/2,604 units and 142/212/295 integrations; this check does not execute the full suites again. Rebuilding the plan from the retained complete CI artifacts yields exact model equality. The existing missing/duplicate/omitted-case refusal tests remain; the stability regression now includes all actual override files as well as 500 synthetic paths.

External proof is `ci-plan-refresh-2e20-proof/proof.json`, with exact console argument vectors, generated collections, the 73-case XML and logs. `ci-4e-measured-balancing-analysis.json` retains before/after costs and all measured integration file rows. Ruff/format, independent strict checks of the changed selector/test and the source-comment policy pass. No workflow/job, service, worker count, quota, case timeout, p90 threshold, zero-skip rule, aggregate gate or 240-second budget changes. This is partition/calibration proof, not a new full-suite runtime pass or global refactor completion.
