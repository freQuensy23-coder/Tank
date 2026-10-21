"""One real FILTER and one real ROUTE call through Pipeline (OpenCode, key from the Docker secret)."""
import asyncio, json
from pathlib import Path
from types import SimpleNamespace
import httpx, yaml
from tank.agents.pipeline import Pipeline
from tank.storage import MemoryRepository

key = Path("/etc/tank/secrets/opencode_api_key").read_text().strip()
registry = yaml.safe_load(Path("config/registry.yaml").read_text())
settings = SimpleNamespace(opencode_api_key=key, replay=False, config_version="smoke", code_version="smoke",
                           load_config=lambda name: {"registry": registry}[name])
item = {"item_id": "smoke-1", "source": "Reuters",
        "title": "Judge rejects CFTC, Gemini bid to vacate $5 million settlement - Reuters",
        "text": "Judge rejects CFTC, Gemini bid to vacate $5 million settlement Reuters"}
event = {"statement": "A U.S. federal judge denied a joint Gemini-CFTC request to undo Gemini's $5 million settlement.",
         "entities": [{"entity_id": "UNRESOLVED:Gemini"}, {"entity_id": "cftc"}], "tokens_mentioned": [], "event_state": "APPROVED"}


async def main():
    repo = MemoryRepository()
    async with httpx.AsyncClient(timeout=30) as http:
        pipe = Pipeline(repo, settings, http=http)
        f = await pipe._score_record(item, "relevant", pipe.state(item))
        r = await pipe._score_record(item, "same_event", pipe.routing_state(item, event))
    for name, row in (("FILTER", f), ("ROUTE", r)):
        print(name, json.dumps({k: row[k] for k in ("score", "requested_model_id", "resolved_model_id", "jev_provider",
                                                      "endpoint", "input_tokens", "cost_usd", "billing_basis")}))

asyncio.run(main())
