"""Print the exact Jev request bodies Tank sends for three packet items (no network)."""

import json
import sys

src, out = sys.argv[1], sys.argv[2]
sys.path.insert(0, src)
from tank.agents.jev import BINDING
from tank.agents.pipeline import Pipeline
from tank.settings import Settings

P = "/srv/tank-work/wt/tank-calibration/docs/verification/v0b-calibration-2026-10-20/packet.json"
packet = json.load(open(P))
items = {r["item_id"]: r["source_item"] for r in packet["items"]}
events = {e["event_id"]: e for e in packet["events"]}
for e in events.values():
    e.setdefault("known_to_us_at", "2026-10-16T05:44:05Z")
settings = Settings(_env_file=None, config_dir=src.replace("/src", "/config"))
pipe = Pipeline(None, settings, http=object())
cases = [
    ("1 FILTER_ITEM: The Block news", "ITM-01M57D8P1JVB6QEVN8J31PGVQD", "relevant", None),
    (
        "2 ROUTE_ITEM: Reuters repeat of the known Gemini/CFTC event (true: same event)",
        "ITM-01M57D8P4K4EWXYTHKEEQEJ28R",
        "same_event",
        "V2EV-4fe02d2f13c9bb81c3420d44-0",
    ),
    (
        "3 ROUTE_ITEM: OKX notice vs another OKX event (true: different event)",
        "ITM-01M59JRFV9XQWMS7CZSQKAVJPZ",
        "same_event",
        "REF-GLOBAL-bbeca38c7c94522b79ca0f47",
    ),
]
doc = []
for title, item_id, name, event_id in cases:
    item = items[item_id]
    state = pipe.state(item) if name == "relevant" else pipe.routing_state(item, events[event_id])
    questions = pipe.score_identity(item, name, state)[1]
    doc.append(
        {"case": title, "request": {"model": BINDING.request_model, "state": state, "questions": questions}}
    )
json.dump(doc, open(out, "w"), indent=2, ensure_ascii=False)
print(json.dumps(doc, indent=2, ensure_ascii=False))
