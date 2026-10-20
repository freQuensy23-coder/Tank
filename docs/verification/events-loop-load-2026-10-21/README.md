# Scheduler share of the event loop (tank-events)

Method: py-spy record --pid <tank serve> --nonblocking --idle -r 50 -d 120 --format speedscope on prod, read-only; share.py computes main-thread busy % and the share of frames under scheduler.py. Raw profile: /srv/tank-work/logs/tank-events/pyspy/before.json (1.6 MB, not committed).

Before (r102, 2026-10-20 22:16–22:18 UTC): main thread 99.2% busy; scheduler 31.0% of busy samples. reaction_capture 29.2% (market.snapshot ~22.7%, _reaction_plan reading every revision ~4.6%), eval_conditions 1.4%. All 42 REACTION_CAPTURE jobs in the 30 min before were automatic late revisions, mostly of PRE windows. Details: before-r102.txt.

Changes (main 535c429): _reaction_plan reads revisions as metadata and fetches one row; detect_late_data skips windows that end before the change's market_from (needs tank-market to add market_from). Results unchanged.

After: to be measured the same way after the roll that carries 535c429.
