# Docker build benchmark results

Final product: `0549017a1a019a641fce1b929b61c6ed54c1c7d0`. Before recipes: `c9d2ced6cefcfc3e6b26cfccc2b0aa0e634caa81`. Final controlled workflow/helper head: `f937e991e2fea62c08ec58a077abe939cab259c7`.

The final controlled CI observation improved from **265 s (4:25) to 224 s (3:44)**, a reduction of **41 s / 15.47%**. Both runs passed all seven jobs. Tank and Capture cold image builds were slower in this observation; the larger gains are in specific cached rebuilds. These are single observed pairs, with no claim of a stable median or guaranteed speedup.

## Paired image builds

Seconds cover image build and load. Old and new recipes run sequentially on the same hosted runner for each image, with independent empty builders for cold builds; warm cases reuse each arm's own cache. Both arms use the same frozen base digests/dependency resolution and archived application source. Recipe-associated deployment assets are overlaid by the helper. Every measured case also passed its offline runtime smoke. Dependency-text edits preserve dependency versions; they do not measure upgrading packages.

| Image | Scenario | Before seconds | After seconds | Change |
| --- | --- | ---: | ---: | ---: |
| tank | cold | 92.257 | 95.404 | +3.4% |
| tank | unchanged rebuild | 3.884 | 3.937 | +1.4% |
| tank | source comment edit | 6.562 | 6.191 | -5.6% |
| tank | dependency text edit; same versions | 77.916 | 8.486 | -89.1% |
| sandbox | cold | 153.013 | 151.978 | -0.7% |
| sandbox | unchanged rebuild | 6.002 | 5.410 | -9.9% |
| sandbox | browser worker comment edit | 9.004 | 8.852 | -1.7% |
| sandbox | dependency text edit; same versions | 103.848 | 52.828 | -49.1% |
| capture | cold | 25.594 | 27.254 | +6.5% |
| capture | unchanged rebuild | 0.367 | 0.394 | +7.4% |
| capture | source comment edit | 15.742 | 1.403 | -91.1% |
| capture | dependency text edit; same versions | 15.820 | 0.783 | -95.0% |
| openma | cold | 176.010 | 161.296 | -8.4% |
| openma | source TS file addition | 165.443 | 16.036 | -90.3% |
| openma | dependency text edit; same versions | UNMEASURED | UNMEASURED | — |
| openma | unchanged rebuild | UNMEASURED | UNMEASURED | — |

Tank, Sandbox and Capture are from [paired run 39651244453](https://github.com/freQuensy23-coder/Tank/actions/runs/39651244453), after-product `fca4e38bee455d22c30dabcd4a758e3fb3261c2e`, inputs SHA256 `f199817d951fee43bbdb2cf593f95d1fe3fbe0b811ab2208267f0efb492b59e1`. Their recipes, application source tree and dependency files are identical at final product 054. Sandbox's consumed browser files and Capture's source tree are identical too. Tank also copies deployment assets, including the changed OpenMA renderer, so its paired results retain their original fca reference. Capture's embedded version stamp intentionally follows the recorded product reference. Exact Git object comparisons are saved in [component-input-equivalence.json](benchmarks/docker/component-input-equivalence.json).

OpenMA is from [final bounded paired run 39655937087](https://github.com/freQuensy23-coder/Tank/actions/runs/39655937087), after-product 054, benchmark head f937, inputs SHA256 `497cf63d94aa56725310c9b7116ebf07ce03c46260ea093ee4ddcf31c3a697b5`. Final OpenMA no-change and dependency-text cases are **UNMEASURED**: the selected final profile measures cold and source edit only. Earlier full-profile OpenMA measurements used superseded recipes and are excluded from this final table.

## Controlled whole CI

[Before run 39655929438](https://github.com/freQuensy23-coder/Tank/actions/runs/39655929438) and [after run 39655933389](https://github.com/freQuensy23-coder/Tank/actions/runs/39655933389) ran concurrently on separate hosted runners, on the same final benchmark head and inputs. The identical seven-job graph contains Go, Python/Go parity, 54 focused deployment tests, and four complete image builds with shared smokes. It includes full API-only OpenMA (`SKIP_CONSOLE=1`), exceeding ordinary deps-only Docker checks; historical capture-only runs are not a comparable baseline.

Time to green is run creation through the latest required job completion, including waiting and cleanup: before 18:47:46Z–18:52:11Z, after 18:47:47Z–18:51:31Z on 2026-10-21. It is **265 → 224 s**; parallel job sums of 919 → 845 s are not elapsed time. Workflow start equaled creation in both runs; the first job began 3 → 4 s later.

OpenMA remained the critical job: 261 → 220 s, beginning 4 s after each run was created. Its build/load duration was 176.832 → 165.032 s; other time in that critical job was 84.168 → 54.968 s. This arithmetic decomposition does not isolate a causal contribution from setup, context checks, cache export, cleanup or hosted-runner variation. See [critical-path.json](benchmarks/docker/critical-path.json).

Cold image build/load durations in those separate-run CI observations:

| Image | Before seconds | After seconds | Change |
| --- | ---: | ---: | ---: |
| tank | 93.802 | 104.879 | +11.8% |
| sandbox | 164.414 | 151.994 | -7.6% |
| capture | 29.004 | 32.357 | +11.6% |
| openma | 176.832 | 165.032 | -6.7% |

Cold image sizes from those same artifacts (MiB = 1,048,576 bytes):

| Image | Before MiB | After MiB | Change MiB |
| --- | ---: | ---: | ---: |
| tank | 1732.84 | 1711.70 | -21.13 |
| sandbox | 2791.10 | 2772.21 | -18.89 |
| capture | 13.88 | 13.88 | +0.00 |
| openma | 1973.93 | 1900.69 | -73.24 |

Both focused test artifacts contain 54 tests, zero failures/errors/skips, in 1.020 s and 1.631 s. Go and parity artifacts identify source 054 in both variants. The optimization diff contains 13 Docker/CI/helper/documentation/new renderer-test files; application code, schema, migrations, lock files, existing tests and capture CI are unchanged relative to the stated baseline. [Product scope audit](benchmarks/docker/product-scope-audit.json) and [verification identity](benchmarks/docker/verification-identity.json) preserve those checks.

The [six product PR checks](https://github.com/freQuensy23-coder/Tank/pull/6/checks) also succeeded on final product 054; the [recorded check-run API result](benchmarks/docker/product-pr6-checks.json) preserves their identities. These ordinary checks are distinct from the seven-job controlled benchmark graph above.

## Evidence and limits

The accepted sets remain separate: [paired common images](benchmarks/docker/paired-common.json), [final paired OpenMA](benchmarks/docker/paired-openma.json), and [whole CI](benchmarks/docker/whole-ci.json). Each set passed the [strict analyzer](../tools/benchmarks/analyze-build-benchmarks.py), including exact captured-input SHA, complete successful arms/scenarios and runtime-contract checks. Input hashes were not relaxed to combine different versions. [Provenance](benchmarks/docker/provenance.json) records references, run links and small-file hashes; raw run/job JSON is retained beside these reports. Large logs/artifacts remain in Actions and server evidence.

Tank/Sandbox cold arms recorded equal Python/OS inventories within each pair. Capture checks cover static executable, version, configuration and health behavior. OpenMA checks cover exact lock/workspace/entrypoint/native bytes, shared Node base and offline runtime file/TypeScript checks; a full OS/npm inventory was not recorded. These image checks do not qualify real providers, X, KVM execution or the optional console. No production runtime was changed to obtain these hosted measurements.
