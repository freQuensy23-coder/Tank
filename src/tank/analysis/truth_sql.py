_EXPRESSIONS = {
    "add": "a + b",
    "multiply": "multiplyDecimal(a, b, 24)",
    "divide": "divideDecimal(a, b, 24)",
    "compare": "toInt8(a > b) - toInt8(a < b)",
    "funding_8h": "divideDecimal(multiplyDecimal(a, toDecimal256(8, 24), 24), b, 24)",
}

_INPUT_PATTERN = r"^[+-]?0*[0-9]{1,24}(\.[0-9]{1,24})?$"


def arithmetic_query(operation: str, left: str, right: str):
    if operation not in _EXPRESSIONS:
        raise ValueError("unsupported arithmetic operation")
    denominator_guard = " OR b = 0" if operation in {"divide", "funding_8h"} else ""
    query = (
        "WITH toDecimal256OrNull({left:String}, 24) AS parsed_a, "
        "toDecimal256OrNull({right:String}, 24) AS parsed_b, "
        "ifNull(parsed_a, toDecimal256(0, 24)) AS a, "
        "ifNull(parsed_b, toDecimal256(0, 24)) AS b "
        f"SELECT toString({_EXPRESSIONS[operation]}) AS value "
        "WHERE throwIf(isNull(parsed_a) OR isNull(parsed_b) "
        "OR NOT match({left:String}, {pattern:String}) "
        "OR NOT match({right:String}, {pattern:String})"
        + denominator_guard
        + ", 'Market Truth decimal input outside native financial range') = 0"
    )
    return query, {
        "left": (left, "String"),
        "right": (right, "String"),
        "pattern": (_INPUT_PATTERN, "String"),
    }
