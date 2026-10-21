"""tank serve sets the GIL switch interval before the service starts."""

import sys

from click.testing import CliRunner

import tank.cli as cli


def test_serve_sets_short_gil_switch_interval_before_running(monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "run_db", lambda fn, **kwargs: seen.append(sys.getswitchinterval()))
    before = sys.getswitchinterval()
    try:
        result = CliRunner().invoke(cli.main, ["serve"])
        assert result.exit_code == 0, result.output
        assert seen == [cli.GIL_SWITCH_INTERVAL_S]
        assert before > cli.GIL_SWITCH_INTERVAL_S
    finally:
        sys.setswitchinterval(before)
