from dataclasses import dataclass
from decimal import Decimal
from enum import Enum, IntEnum

from tank import util


def test_json_primitives_do_not_require_dataclass_or_model_reflection(monkeypatch):
    reflected = []
    original = util.dataclasses.is_dataclass

    def observed(value):
        reflected.append(value)
        return original(value)

    monkeypatch.setattr(util.dataclasses, "is_dataclass", observed)
    value = {"value": "100.00", "counts": [1, True, None, 1.25]}
    assert util.jsonable(value) == value
    assert all(type(item) not in (str, int, float, bool, type(None)) for item in reflected)


def test_jsonable_fast_path_keeps_nonprimitive_conversion_and_subclass_hooks():
    class Color(str, Enum):
        BLUE = "blue"

    class Priority(IntEnum):
        HIGH = 2

    @dataclass
    class Amount:
        value: Decimal

    class StructuredText(str):
        def model_dump(self, *, mode):
            assert mode == "python"
            return {"nested": str(self)}

    value = {
        None: Amount(Decimal("-0.00")),
        "color": Color.BLUE,
        "priority": Priority.HIGH,
        "model": StructuredText("literal"),
    }
    assert util.jsonable(value) == {
        "None": {"value": "-0.00"},
        "color": "blue",
        "priority": 2,
        "model": {"nested": "literal"},
    }
