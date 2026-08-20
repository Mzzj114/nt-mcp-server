"""Value normalization helpers for the NetworkTables boundary.

Pure functions that convert ntcore/numpy values into JSON-serializable
primitives and classify homogeneous lists for the array setters. Shared
by the NT layer; no ntcore connection state lives here.
"""

from __future__ import annotations

from typing import Any, Literal

try:  # numpy is optional — normalize numpy values only when it is present
    import numpy as np
except ImportError:  # pragma: no cover - venv without numpy
    np = None  # type: ignore[assignment]

ArrayKind = Literal["boolean", "integer", "double", "string"]


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
