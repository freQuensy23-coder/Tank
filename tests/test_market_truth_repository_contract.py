import pytest
from test_market_truth_request import repository_contract

SOURCE = """
def _projection(collection, record):
    return record

class PostgresRepository:
    async def put(self, collection, key, record, immutable=False):
        return record

    async def lock(self, key):
        yield
"""


def test_request_guard_allows_only_the_record_put_implementation_and_its_private_helper(tmp_path):
    path = tmp_path / "storage.py"
    path.write_text(SOURCE)
    original = repository_contract(path)
    path.write_text(
        SOURCE.replace("        return record", "        return dict(record)")
        + "\ndef _same_json_value(left, right):\n    return left == right\n"
    )
    assert repository_contract(path) == original


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("immutable=False", "immutable=True"),
        ("return record", "return dict(record)"),
        ("        yield", "        yield key"),
        ("async def lock(self, key)", "def lock(self, key)"),
    ],
)
def test_request_guard_rejects_signature_projection_and_lock_changes(tmp_path, before, after):
    path = tmp_path / "storage.py"
    path.write_text(SOURCE)
    original = repository_contract(path)
    path.write_text(SOURCE.replace(before, after))
    assert repository_contract(path) != original
