import shutil
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

scratch = Path(sys.argv[1])
name = "grafana.127-0-0-1.sslip.io"
origin = f"https://{name}:{int(sys.argv[2])}"
state = scratch / "session.json"
with sync_playwright() as driver:
    browser = driver.chromium.launch(
        executable_path=shutil.which("google-chrome"),
        args=["--no-proxy-server", f"--host-resolver-rules=MAP {name} 127.0.0.1, MAP * ~NOTFOUND"],
    )
    ctx = browser.new_context(storage_state=state if state.exists() else None)
    ctx.route(
        "**/*",
        lambda route: route.continue_() if route.request.url.startswith(origin + "/") else route.abort(),
    )
    page = ctx.new_page()
    if not state.exists():
        page.goto(origin + "/d/tank-queue/")
        page.wait_for_url("**/login*")
        for path in ("/api/dashboards/uid/tank-queue", "/api/datasources/uid/tank-postgres"):
            assert page.evaluate("async p => (await fetch(p)).status", path) == 401
        page.locator('input[name="user"]').fill("stage-viewer")
        page.locator('input[name="password"]').fill((scratch / "viewer-password").read_text().strip())
        page.get_by_role("button", name="Log in").click()
        page.wait_for_url("**/d/tank-queue/**")
    else:
        page.goto(origin + "/api/user")
    cookies = [c for c in ctx.cookies() if c["name"] == "grafana_session"]
    assert len(cookies) == 1 and cookies[0]["secure"] and cookies[0]["httpOnly"]
    assert page.evaluate("async () => (await (await fetch('/api/user')).json()).login") == "stage-viewer"
    assert page.evaluate("async () => (await fetch('/api/admin/settings')).status") == 403
    for uid in ("tank-queue", "tank-pipeline"):
        dash = ctx.new_page()
        with dash.expect_response(lambda r: "/api/ds/query" in r.url and r.request.method == "POST") as query:
            dash.goto(f"{origin}/d/{uid}/")
        results = query.value.json()["results"]
        assert (
            query.value.ok and results and all("frames" in r and not r.get("error") for r in results.values())
        )
        dash.locator('[data-testid^="data-testid Panel header"]').first.wait_for(state="visible")
        dash.close()
    ctx.storage_state(path=state)
    browser.close()
print("Verified browser TLS, Viewer session, admin denial and both dashboards")
