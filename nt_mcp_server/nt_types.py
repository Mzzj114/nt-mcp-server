"""Value normalization helpers for the NetworkTables boundary.

Pure functions that convert ntcore/numpy values into JSON-serializable
primitives and classify homogeneous lists for the array setters. Shared
by the NT layer; no ntcore connection state lives here.
"""

from __future__ import annotations

import fnmatch
import json
import re
from typing import Any, Literal

try:  # numpy is optional — normalize numpy values only when it is present
    import numpy as np
except ImportError:  # pragma: no cover - venv without numpy
    np = None  # type: ignore[assignment]

ArrayKind = Literal["boolean", "integer", "double", "string"]


class TopicSelector:
    """Reusable topic matcher for live streams and offline recordings.

    Multiple values inside ``prefixes`` or ``topics`` are ORed. Different
    selector kinds are ANDed, matching ``nt_list_topics`` filter semantics.
    Exact topics double as efficient NT subscription roots when no prefixes
    are supplied; regex/wildcard-only selection subscribes at ``"/"`` and
    filters events locally.
    """

    def __init__(
        self,
        prefixes: list[str] | None = None,
        topics: list[str] | None = None,
        regex: str | None = None,
        wildcard: str | None = None,
    ) -> None:
        self.prefixes = tuple(prefixes or ())
        self.topics = frozenset(topics or ())
        self.regex = regex
        self.wildcard = wildcard
        try:
            self._pattern = re.compile(regex) if regex is not None else None
        except re.error as exc:
            raise ValueError(f"invalid topic regex {regex!r}: {exc}") from exc

    @property
    def empty(self) -> bool:
        """Whether no selector kind was supplied."""
        return not (
            self.prefixes
            or self.topics
            or self.regex is not None
            or self.wildcard is not None
        )

    @property
    def bare_root_only(self) -> bool:
        """Whether the selector is exactly an unqualified ``'/'`` prefix."""
        return (
            self.prefixes == ("/",)
            and not self.topics
            and self.regex is None
            and self.wildcard is None
        )

    @property
    def subscription_prefixes(self) -> list[str]:
        """Return the narrowest prefixes usable by NT MultiSubscriber."""
        if self.prefixes:
            return list(self.prefixes)
        if self.topics:
            return sorted(self.topics)
        return ["/"]

    def matches(self, topic: str) -> bool:
        """Return whether ``topic`` satisfies every supplied selector kind."""
        if self.prefixes and not any(
            topic.startswith(prefix) for prefix in self.prefixes
        ):
            return False
        if self.topics and topic not in self.topics:
            return False
        if self._pattern is not None and self._pattern.search(topic) is None:
            return False
        if self.wildcard is not None and not fnmatch.fnmatch(topic, self.wildcard):
            return False
        return True


def array_kind(values: list[Any]) -> ArrayKind | None:
    """Element-type tag for a homogeneous list; None if empty/mixed.

    Bools are never counted as ints, so ``[True, 1]`` classifies as None.
    Unsupported element types (nested lists, dicts, ...) classify as None.
    """
    if not values:
        return None
    if all(isinstance(value, bool) for value in values):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        return "integer"
    if all(isinstance(value, float) for value in values):
        return "double"
    if all(isinstance(value, str) for value in values):
        return "string"
    return None


def to_jsonable(value: Any) -> Any:
    """Normalize a value into a JSON-serializable primitive.

    - ``bytes``/``bytearray``/``memoryview`` -> ``None`` (raw NT topics are
      unsupported; bytes must never leak into tool output).
    - numpy scalars -> ``float``/``int``/``bool``/``str`` via casts.
    - numpy arrays -> ``list`` (recursively normalized).
    - ``list``/``tuple``/``dict`` recurse; anything else passes through.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        return None
    if np is not None:
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
        if isinstance(value, np.str_):
            return str(value)
        if isinstance(value, np.ndarray):
            return [to_jsonable(item) for item in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    return value


def _is_numeric_scalar(value: Any) -> bool:
    """True for int/float scalars, excluding bool."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _all_numeric(values: list[Any]) -> bool:
    return all(_is_numeric_scalar(v) for v in values)


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a summary dict for a list of ``{"time": float, "value": ...}`` samples.

    Always includes ``count``, ``last_value`` and ``last_time``. If every
    value is a numeric scalar, also includes ``min``, ``max`` and ``mean``.
    """
    if not samples:
        return {"count": 0}
    values = [s["value"] for s in samples]
    last = samples[-1]
    summary: dict[str, Any] = {
        "count": len(samples),
        "last_value": last["value"],
        "last_time": last["time"],
    }
    if _all_numeric(values):
        nums = [float(v) for v in values]
        summary["min"] = min(nums)
        summary["max"] = max(nums)
        summary["mean"] = sum(nums) / len(nums)
    return summary


def type_warning(value: Any, type_str: str) -> str | None:
    """Return a warning string if ``value`` does not match ``type_str``.

    ``type_str`` comes from NT ``TopicInfo.type_str`` (e.g. ``"double"``,
    ``"boolean"``, ``"string"``, ``"double[]"``, ``"int[]"``). Returns
    ``None`` when the value looks compatible.
    """
    type_str = type_str.strip().lower()
    is_array = "[]" in type_str or type_str.endswith("array")

    if is_array:
        if not isinstance(value, list):
            return f"expected array type {type_str}, got {type(value).__name__}"
        kind = array_kind(value)
        if kind is None and value:
            return "expected homogeneous array"
        # Map NT array type strings to our ArrayKind names.
        if "double" in type_str or "float" in type_str:
            if kind != "double":
                return f"expected double[], got {kind or 'mixed'} array"
        elif "int" in type_str:
            if kind != "integer":
                return f"expected int[], got {kind or 'mixed'} array"
        elif "bool" in type_str:
            if kind != "boolean":
                return f"expected boolean[], got {kind or 'mixed'} array"
        elif "string" in type_str or "str" in type_str:
            if kind != "string":
                return f"expected string[], got {kind or 'mixed'} array"
        return None

    # Scalar checks.
    if "double" in type_str or "float" in type_str:
        if not _is_numeric_scalar(value):
            return f"expected double, got {type(value).__name__}"
    elif "int" in type_str:
        if not isinstance(value, int) or isinstance(value, bool):
            return f"expected int, got {type(value).__name__}"
    elif "bool" in type_str:
        if not isinstance(value, bool):
            return f"expected boolean, got {type(value).__name__}"
    elif "string" in type_str or "str" in type_str or type_str == "json":
        if not isinstance(value, str):
            return f"expected string, got {type(value).__name__}"
    return None


def enforce_max_chars(
    response: dict[str, Any],
    container_key: str,
    max_chars: int,
) -> dict[str, Any]:
    """Trim a tool response's row container until it serializes <= ``max_chars``.

    ``response`` is a JSON-able envelope whose ``container_key`` field holds
    either a flat list of rows or a dict of
    topic -> list of rows (``nt_query_recording`` topics; ``nt_sample``
    sample modes). Rows that do not fit the ceiling are dropped; non-list
    values (summaries) are never touched, and a container with none is
    returned unchanged.

    Deterministic and linear: topic groups are visited in sorted name order
    and rows in list order, each row is serialized exactly once to learn its
    size (a row's ``value`` may be a large nested list/dict, so the cost is
    never assumed constant), and a row is kept iff its serialized size plus
    a 2-char separator allowance fits the remaining budget. A skipped row
    does not stop the scan — a smaller later row can still fit. The 2-char
    allowance over-charges the first row of each list, so the result is
    guaranteed to serialize <= ``max_chars``. If the topic keys alone
    overflow the ceiling, whole topics are dropped from the end of the
    sorted order (alphabetically-last first) until the empty envelope fits.

    Returns a new envelope; the input is not mutated. ``rows`` (when present)
    is recomputed to the rows that actually remain, and ``truncated`` is
    forced True whenever any row or topic was dropped. ``total_rows`` and
    every other field pass through untouched.
    """
    container = response.get(container_key)
    if isinstance(container, list):
        groups: list[tuple[str, list[Any]]] = [("", container)]
        flat = True
    elif isinstance(container, dict):
        groups = sorted(
            (topic, rows)
            for topic, rows in container.items()
            if isinstance(rows, list)
        )
        if not groups:
            return response
        flat = False
    else:
        return response

    # Fixed envelope overhead, measured once with every row list emptied
    # (topic keys kept): each kept row then costs its own JSON size.
    emptied = dict(response)
    if flat:
        emptied[container_key] = []
    else:
        emptied[container_key] = {
            topic: ([] if isinstance(rows, list) else rows)
            for topic, rows in container.items()
        }
    remaining = max_chars - len(json.dumps(emptied))

    kept_rows: dict[str, list[Any]] = {}
    kept_count = 0
    total_count = 0
    for topic, rows in groups:
        survivors: list[Any] = []
        for row in rows:
            total_count += 1
            cost = len(json.dumps(row)) + 2
            if cost <= remaining:
                remaining -= cost
                survivors.append(row)
        kept_rows[topic] = survivors
        kept_count += len(survivors)

    dropped_topics = 0
    if remaining < 0 and not flat:
        # Topic keys alone overflow the ceiling: drop whole topics from the
        # end of the sorted order. Dropping shrinks the overhead, so the
        # freed cost INCREASES the remaining budget. The charged cost
        # (entry + separator) is >= the actual saving, keeping the
        # guarantee intact.
        for topic, _ in reversed(groups):
            if remaining >= 0:
                break
            remaining += len(json.dumps({topic: []})) + 2
            del kept_rows[topic]
            dropped_topics += 1

    result = dict(response)
    if flat:
        result[container_key] = kept_rows[""]
    else:
        new_container = {
            topic: rows
            for topic, rows in container.items()
            if not isinstance(rows, list)
        }
        new_container.update(kept_rows)
        result[container_key] = new_container
    if kept_count < total_count or dropped_topics:
        result["truncated"] = True
    if "rows" in response:
        result["rows"] = kept_count
    return result
