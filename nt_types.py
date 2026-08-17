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
