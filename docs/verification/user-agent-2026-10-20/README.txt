Deployed tank-tank-1 (tank serve from main 64192b0, started 18:50:41 UTC), 2026-10-20 ~19:10 UTC:
- resolve_versions(Settings()).user_agent and SourceService.http User-Agent = TankV0/1.0 (inbox@ai.com)
- GET https://httpbin.org/headers through the source client echoed the header below (TDD 13, HLD 3)
- TANK_USER_AGENT is not set in the container; the value comes from config/sources.yaml contact_email
