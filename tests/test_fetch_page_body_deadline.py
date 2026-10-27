import pytest
from test_fetch_page_deadline import assert_real_twenty_second_total_deadline


@pytest.mark.integration(reason="Real default 20-second deadline across HTTP headers or streamed body")
@pytest.mark.parametrize("owned", [False, True])
async def test_real_twenty_second_total_deadline(tmp_path, monkeypatch, owned):
    await assert_real_twenty_second_total_deadline(tmp_path, monkeypatch, owned, "slow_drip")
