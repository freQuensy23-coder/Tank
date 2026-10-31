import cProfile
import math
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from enum import Enum
from importlib.machinery import ExtensionFileLoader
from types import CodeType

import pytest

from tank import util


def clone(value):
    function = getattr(util, "owned_json", None)
    assert callable(function), "The repository's owned JSON clone must execute in the native util module"
    return function(value)


def test_owned_json_detaches_every_mutable_alias_without_coercing_primitives():
    shared = [{"value": [1, True, None, "text", -0.0, float("inf"), float("nan")]}]
    original = {"first": shared, "second": shared}
    supported, copied = clone(original)
    assert supported and copied is not original
    assert copied["first"] is not copied["second"] and copied["first"] is not shared
    assert copied["first"][0] is not shared[0]
    assert copied["first"][0]["value"] is not shared[0]["value"]
    values = copied["first"][0]["value"]
    assert values[:4] == [1, True, None, "text"]
    assert type(values[0]) is int and type(values[1]) is bool
    assert values[4].hex() == (-0.0).hex() and math.isinf(values[5]) and math.isnan(values[6])
    values.append("changed")
    assert len(shared[0]["value"]) == len(copied["second"][0]["value"]) == 7


@pytest.mark.parametrize("depth", [0, 1, 62, 63, 64, 65])
def test_owned_json_preserves_the_existing_depth_boundary(depth):
    value = "leaf"
    for _ in range(depth):
        value = [value]
    supported, copied = clone(value)
    assert supported is (depth < 64)
    assert copied == value if supported else copied is None


def test_owned_json_refuses_nonprimitive_values_before_running_any_user_callbacks():
    class ForbiddenDict(dict):
        def items(self):
            raise AssertionError("Subclass callbacks must remain on the original jsonable fallback")

    class Text(str):
        pass

    class Number(int):
        pass

    class Label(str, Enum):
        ONE = "one"

    for value in (ForbiddenDict(a=1), Text("one"), Number(2), Label.ONE, (1,), {Text("key"): 1}):
        assert clone(value) == (False, None)
    cyclic = []
    cyclic.append(cyclic)
    assert clone(cyclic) == (False, None)


def test_owned_json_does_not_convert_money_or_change_decimal_context_on_fallback():
    with localcontext() as context:
        context.prec = 9
        before = context.copy()
        for value in (Decimal("-0.000"), Decimal("1e-900"), datetime(2026, 10, 1, tzinfo=UTC)):
            assert clone({"value": value}) == (False, None)
        assert context.prec == before.prec and context.rounding == before.rounding
        assert context.flags == before.flags and context.traps == before.traps


def test_owned_json_does_not_enter_python_for_each_retained_document_leaf():
    value = {"rows": [list(range(12)) for _ in range(2000)]}
    profile = cProfile.Profile()
    supported, copied = profile.runcall(clone, value)
    assert supported and copied == value
    python_nodes = sum(
        row.callcount for row in profile.getstats() if isinstance(row.code, CodeType)
    )
    assert isinstance(util.__loader__, ExtensionFileLoader) and python_nodes < 8
