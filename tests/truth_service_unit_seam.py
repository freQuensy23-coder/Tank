from copy import deepcopy

from tank.analysis import truth_sql_engine


class InertProvider:
    async def rows(self, *args, **kwargs):
        raise AssertionError("unit evaluator dependency must not execute native SQL")


def bind_evaluator(monkeypatch, repo, evaluator, *, conditions=False):
    provider = InertProvider()
    monkeypatch.setattr(repo, "clickhouse", provider, raising=False)
    monkeypatch.setattr(
        truth_sql_engine, "evaluate_conditions" if conditions else "evaluate", evaluator
    )
    return provider


def assert_input(client, snapshot, config, provider, expected, versions):
    assert client is provider
    assert deepcopy(snapshot) == expected
    assert config == dict(config_version=versions[0], code_version=versions[1])
