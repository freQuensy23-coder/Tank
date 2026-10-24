"""tank serve sets the GIL switch interval before the service starts."""

import sys

import pytest
from click.testing import CliRunner

import tank.cli as cli


def test_serve_sets_short_gil_switch_interval_before_running(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = []
    monkeypatch.setattr(cli, "run_db", lambda fn, **kwargs: seen.append(sys.getswitchinterval()))
    before = sys.getswitchinterval()
    try:
        sys.setswitchinterval(max(0.05, cli.GIL_SWITCH_INTERVAL_S * 10))
        result = CliRunner().invoke(cli.main, ["serve"])
        assert result.exit_code == 0, result.output
        assert seen == [cli.GIL_SWITCH_INTERVAL_S]
        assert sys.getswitchinterval() == cli.GIL_SWITCH_INTERVAL_S
    finally:
        sys.setswitchinterval(before)
