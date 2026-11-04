from contextlib import asynccontextmanager

from truth_native_test_safety import require_loopback_native_url
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.analysis import truth_sql_engine as native


@asynccontextmanager
async def native_memory_analytics(monkeypatch, request):
    url = require_loopback_native_url()
    original_evaluate = native.evaluate
    original_conditions = native.evaluate_conditions
    client = EvidenceClickHouse(url, request.node.nodeid)
    request.node.user_properties.extend(
        [("storage_backend", "memory"), ("analytics_backend", "native_clickhouse")]
    )
    try:
        with monkeypatch.context() as scoped:

            async def evaluate(passed_client, snapshot, config=None):
                assert passed_client is None
                return await original_evaluate(client, snapshot, config)

            async def evaluate_conditions(
                passed_client, snapshot, requirements, config=None, *, cache=None
            ):
                assert passed_client is None
                return await original_conditions(
                    client, snapshot, requirements, config, cache=cache
                )

            scoped.setattr(native, "evaluate", evaluate)
            scoped.setattr(native, "evaluate_conditions", evaluate_conditions)
            yield client
    finally:
        await client.close()
