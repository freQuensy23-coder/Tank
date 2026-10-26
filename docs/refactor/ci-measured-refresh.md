# Measured CI lane refresh

This refresh uses complete reports from run40407818694. The actual PR test merge is62d0dd0c8f41cfb401cbfb0e19b7087bc40f0b8b, whose tree1ab534e14c303fba432b846d4801b42e493a0966 is identical to head4e2d85773d24139b58232d71c56ad168b08f3fa3. Independent complete-union checks admit4898/4898 units (p90.369seconds, maximum5.989) and649/649 actual cases, with zero skips. The later global240-second cancellation is not relabelled as a test failure: lane0 had already passed its163 cases and completeness check before upload/cleanup and cancellation. Queue, setup, cleanup and aggregate startup still need margin.

## Authoritative unit calibration

The unchanged `build_unit_plan` consumes both complete unit JUnit reports and the full recorded collection. It refuses missing/duplicate cases, source mismatches and failed timing/skips before producing the plan. The checked-in plan is an exact rebuild; no per-file duration is guessed.

| Input | SHA256 |
| --- | --- |
| Full collection | 734ecac744a02a7d85847b8188c655a8e903fe2665b0ce35c95fc8b268699dc5 |
| Unit report0 | f3d8940b2e943c0d928193c45063ee079c8eeeffaa73839d37be1760744acf03 |
| Unit report1 | fbae730e714cd508ef96641663e8d34acf9f442dfb571d9e9025bfa0a732537e |
| New plan | 82158eb73372195d883d3d6c9d822a5fe0e37530b6afa4f05a44a66c60a070db |

All307 observed files retain actual case counts and summed case durations. Applying the unchanged deterministic whole-file allocator changes166 files' lane and balances observed sums345.989/270.847 to308.418/308.418 case-seconds. The source remains the actual historical calibration merge; newer files and changed case counts are reported explicitly through the existing coverage mechanism and deterministic unmeasured-file routing. No timing value is invented for the five later YAML cases or another future cohort.

## Minimal integration movement

The existing whole-file SHA routing and replay/reference/workflow/Apify placements remain. Only two additional measured files move to lane1:

| File | Previous lane | New lane | Cases | Observed case-seconds |
| --- | --- | --- | --- | --- |
| tests/test_milestone_checks.py | 0 | 1 | 21 | 25.292 |
| tests/test_storage.py | 2 | 1 | 36 | 10.493 |

Observed lane costs240.763/182.172/227.051 become215.471/217.957/216.558; the same649 identities partition142/212/295. These are sums from actual case records, not wall-clock predictions or guarantees about queue/network/setup/upload/aggregate time. The next pushed CI provides the actual margin measurement.

## Qualification and retained limits

At source2e20aa9041fd31ab139a777f815b09c22bc3a856, the normal pinned pytest console and approved read-only dependency runner execute73 selector/budget regressions in3.99seconds, maximum.043/p90.019, zero skips. Actual console collection then records4900 current units (the4898 calibration cases plus the two placement regressions) and the same649 integrations. Disjoint complete selection counters produce unit2296/2604 and integration142/212/295 without executing either full suite again. Rebuilding the plan from the retained complete CI artifacts yields exact model equality. The existing missing/duplicate/omitted-case refusal tests remain; the stability regression now includes all actual override files as well as500 synthetic paths.

External proof is `ci-plan-refresh-2e20-proof/proof.json`, with exact console argument vectors, generated collections,73-case XML and logs. `ci-4e-measured-balancing-analysis.json` retains before/after costs and all measured integration file rows. Ruff/format, independent strict checks of the changed selector/test and the source-comment policy pass. No workflow/job, service, worker count, quota, case timeout, p90 threshold, zero-skip rule, aggregate gate or240-second budget changes. This is partition/calibration proof, not a new full-suite runtime pass or global refactor completion.
