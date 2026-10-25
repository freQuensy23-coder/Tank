"""Identity masking over serialized acceptance documents."""

import copy
import re
from collections.abc import Callable, Collection, Mapping
from typing import Self

from pydantic import Field, JsonValue, ValidationError, model_validator

from tank.contracts import DomainModel


class AssemblyError(Exception):
    pass


class MaskInverseAmbiguity(AssemblyError):
    pass


class MaskPlan(DomainModel):
    replacements: dict[str, str]
    case_sensitive: frozenset[str]

    @model_validator(mode="after")
    def declared_names(self) -> Self:
        if any(not key or not value for key, value in self.replacements.items()):
            raise ValueError("Mask names must be nonempty")
        if not self.case_sensitive.issubset(self.replacements):
            raise ValueError("Case-sensitive names must have replacements")
        return self


class TechnicalContextExceptions(DomainModel):
    tokens: list[str] = Field(min_length=1)
    fields: list[str] = Field(min_length=1)
    basis: str = Field(min_length=1)
    masked_forms: list[str] | None = None

    @model_validator(mode="after")
    def nonempty_declarations(self) -> Self:
        if (
            not self.basis.strip()
            or any(not token for token in self.tokens)
            or any(not field for field in self.fields)
        ):
            raise ValueError("Technical-context declarations require nonempty scope and ruling")
        return self


class LeakScanResult(DomainModel):
    hits: dict[str, str]
    warnings: list[str]


type TextTransform = Callable[[str], str]
type TreeTransform = Callable[[JsonValue], JsonValue]
type Transform = tuple[TextTransform, TreeTransform]


def decode_exceptions(value: JsonValue) -> TechnicalContextExceptions | None:
    if value is None:
        return None
    try:
        return TechnicalContextExceptions.model_validate(value)
    except ValidationError:
        raise AssemblyError("Invalid declared technical-context exception") from None


def mask_plan(replacements: Mapping[str, str], sensitive: Collection[str]) -> MaskPlan:
    try:
        return MaskPlan(replacements=dict(replacements), case_sensitive=frozenset(sensitive))
    except ValidationError:
        raise AssemblyError("Invalid identity mask plan") from None


def transform_pair(
    replacements: Mapping[str, str], sensitive: Collection[str]
) -> tuple[Transform, Transform]:
    plan = mask_plan(replacements, sensitive)

    def build(inverse: bool) -> Transform:
        if not plan.replacements:

            def identity(value: str) -> str:
                return value

            def clone(value: JsonValue) -> JsonValue:
                return copy.deepcopy(value)

            return identity, clone
        ambiguous: set[str] = set()
        if inverse:
            inverse_names: dict[str, list[str]] = {}
            for key, value in plan.replacements.items():
                inverse_names.setdefault(value, []).append(key)
            ambiguous = {value for value, keys in inverse_names.items() if len(keys) != 1}
            mapping = {value: keys[0] for value, keys in inverse_names.items() if len(keys) == 1}
        else:
            mapping = dict(plan.replacements)
        exact = {plan.replacements[key] if inverse else key for key in plan.case_sensitive}
        parts = sorted([*mapping, *sorted(ambiguous)], key=len, reverse=True)
        pattern = re.compile(
            r"(?<!\w)(?:"
            + "|".join(re.escape(key) if key in exact else "(?i:" + re.escape(key) + ")" for key in parts)
            + r")(?!\w)"
        )

        def replacement(match: re.Match[str]) -> str:
            sources = [
                key
                for key in parts
                if (key == match[0] if key in exact else key.casefold() == match[0].casefold())
            ]
            if not sources:
                raise AssemblyError("identity match cannot be represented by declared case rules")
            if inverse and (len(sources) != 1 or sources[0] in ambiguous):
                raise MaskInverseAmbiguity("identity inverse has more than one declared source")
            return mapping[sources[0]]

        def text(value: str) -> str:
            return pattern.sub(replacement, value)

        def tree(value: JsonValue) -> JsonValue:
            if isinstance(value, str):
                return text(value)
            if isinstance(value, list):
                return [tree(item) for item in value]
            if isinstance(value, dict):
                out = {text(key): tree(item) for key, item in value.items()}
                if len(out) != len(value):
                    raise AssemblyError("identity substitution collides JSON keys")
                return out
            return value

        return text, tree

    return build(False), build(True)


TOKEN_ID = re.compile(r"[a-z0-9][a-z0-9._-]*:[A-Za-z0-9._-]+")


def leak_scan(
    masked: Mapping[str, JsonValue],
    replacements: Mapping[str, str],
    venues: Collection[str],
    exceptions: TechnicalContextExceptions | None,
) -> LeakScanResult:
    plan = mask_plan(replacements, ())
    base: set[str] = set()
    for key in plan.replacements:
        base.add(key)
        if TOKEN_ID.fullmatch(key):
            base.update(key.split(":"))
    if not base:
        return LeakScanResult(hits={}, warnings=[])
    names = sorted(
        base | {name.replace(" ", separator) for name in base if " " in name for separator in ("-", "_")},
        key=len,
        reverse=True,
    )
    bounded = re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, names)) + r")(?!\w)", re.I)
    loose = re.compile("|".join(map(re.escape, names)), re.I)
    hits: dict[str, str] = {}
    warnings: set[str] = set()

    def walk(value: JsonValue, path: tuple[str, ...]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                walk(key, path)
                walk(item, (*path, key))
        elif isinstance(value, list):
            for item in value:
                walk(item, path)
        elif isinstance(value, str):
            where = "/".join(path[:2])
            spans: set[tuple[int, int]] = set()
            allowed: list[tuple[int, int]] = []
            if exceptions is not None and path and path[-1] in exceptions.fields:
                tokens = list(exceptions.tokens)
                if exceptions.masked_forms is not None:
                    tokens.extend(exceptions.masked_forms)
                allowed = [match.span() for token in tokens for match in re.finditer(re.escape(token), value)]
            for match in bounded.finditer(value):
                spans.add(match.span())
                if any(start <= match.start() and match.end() <= end for start, end in allowed):
                    warnings.add(f"{where}:{match[0]} (declared technical-context exception)")
                elif match[0] in venues:
                    warnings.add(f"{where}:{match[0]} (technical venue id)")
                else:
                    hits.setdefault(
                        f"{where}:{match[0]}", value[max(0, match.start() - 30) : match.end() + 30]
                    )
            warnings.update(
                f"{where}:{match[0].casefold()}"
                for match in loose.finditer(value)
                if match.span() not in spans
            )

    document: dict[str, JsonValue] = {key: value for key, value in masked.items() if key != "masking"}
    walk(document, ())
    return LeakScanResult(hits=hits, warnings=sorted(warnings)[:200])
