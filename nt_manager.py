"""NetworkTables connection and state management for nt-mcp-server.

Pure NetworkTables layer: knows about ntcore, knows nothing about MCP,
FastMCP, or tool definitions. The server layer calls into NTManager to
control the client lifecycle and read connection state.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

import ntcore

try:  # numpy is optional — normalize numpy values only when it is present
    import numpy as np
except ImportError:  # pragma: no cover - venv without numpy
    np = None  # type: ignore[assignment]

try:  # assert_never: typing on 3.11+, typing_extensions on 3.10
    from typing import assert_never
except ImportError:  # pragma: no cover - Python 3.10
    from typing_extensions import assert_never

_ArrayKind = Literal["boolean", "integer", "double", "string"]


def _array_kind(values: list[Any]) -> _ArrayKind | None:
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


def _to_jsonable(value: Any) -> Any:
    """Normalize a value into a JSON-serializable primitive.

    - ``bytes``/``bytearray``/``memoryview`` -> ``None`` (raw NT topics are
      unsupported; bytes must never leak into tool output).
    - numpy scalars -> ``float``/``int``/``bool``/``str`` via ``item`` casts.
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
            return [_to_jsonable(item) for item in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    return value


# NT4 clients only receive topics/values they subscribe to. Read methods are
# async and poll the entry store with ``await asyncio.sleep`` for these
# bounds after ensuring a subscription exists, so the event loop is never
# blocked by a read.
_READ_TIMEOUT_SECONDS = 1.0
_ANNOUNCE_WAIT_SECONDS = 0.5


class NTManager:
    """Owns the ntcore client lifecycle for the MCP server.

    The default ``NetworkTableInstance`` is created lazily via
    ``ntcore.NetworkTableInstance.getDefault()``, so importing or
    instantiating ``NTManager`` has no side effects. ``connect`` and
    ``disconnect`` are idempotent and safe to call in any order;
    connection state is exposed as a JSON-serializable dict through
    ``connection_info``.
    """

    def __init__(self) -> None:
        self._inst: ntcore.NetworkTableInstance | None = None
        self._client_running: bool = False
        self._value_subs: dict[str, ntcore.GenericSubscriber] = {}
        self._topics_subscription: tuple[
            ntcore.MultiSubscriber, ntcore.NetworkTableListenerPoller
        ] | None = None
        self._topics_synced: bool = False

    def _instance(self) -> ntcore.NetworkTableInstance:
        """Return the lazily created default NetworkTableInstance."""
        if self._inst is None:
            self._inst = ntcore.NetworkTableInstance.getDefault()
        return self._inst

    def connect(
        self,
        server_ip: str = "127.0.0.1",
        server_port: int = 5810,
        team_number: int | None = None,
        identity: str = "nt-mcp",
    ) -> None:
        """Start the NT4 client against a server or team number.

        Uses ``setServerTeam`` when ``team_number`` is given, otherwise
        ``setServer(server_ip, server_port)``. Idempotent: calling again
        while the client is running is a no-op.
        """
        if self._client_running:
            return
        inst = self._instance()
        if team_number is not None:
            inst.setServerTeam(team_number)
        else:
            inst.setServer(server_ip, server_port)
        inst.startClient4(identity)
        self._client_running = True

    def disconnect(self) -> None:
        """Stop the NT4 client. Idempotent; safe before any connect."""
        if self._client_running and self._inst is not None:
            self._inst.stopClient()
        self._client_running = False
        if self._topics_subscription is not None:
            subscriber, poller = self._topics_subscription
            subscriber.close()
            poller.close()
            self._topics_subscription = None
        self._value_subs.clear()
        self._topics_synced = False

    def _ensure_topics_subscription(
        self,
    ) -> tuple[ntcore.MultiSubscriber, ntcore.NetworkTableListenerPoller]:
        """Return the cached topicsOnly subscription on ``"/"``.

        NT4 servers only announce topics to matching subscribers, so the
        client's topic store (backing ``getTopicInfo``) stays empty until
        a subscription exists. This one is topics-only: announcements
        flow without value traffic.
        """
        if self._topics_subscription is None:
            inst = self._instance()
            subscriber = ntcore.MultiSubscriber(
                inst, ["/"], ntcore.PubSubOptions(topicsOnly=True)
            )
            poller = ntcore.NetworkTableListenerPoller(inst)
            poller.addListener(
                subscriber, ntcore.EventFlags.kTopic | ntcore.EventFlags.kImmediate
            )
            self._topics_subscription = (subscriber, poller)
        return self._topics_subscription

    def connection_info(self) -> dict[str, Any]:
        """Return JSON-serializable connection state.

        ``{"connected": bool, "connections": [{"remote_id", "remote_ip",
        "last_update"}]}``.
        """
        inst = self._instance()
        info: dict[str, Any] = {
            "connected": inst.isConnected(),
            "connections": [
                {
                    "remote_id": conn.remote_id,
                    "remote_ip": conn.remote_ip,
                    "last_update": conn.last_update,
                }
                for conn in inst.getConnections()
            ],
        }
        return _to_jsonable(info)

    async def list_topics(self, prefix: str | None = None) -> list[str]:
        """Return the names of all topics, optionally filtered by prefix.

        Uses ``NetworkTableInstance.getTopicInfo()`` and applies the
        ``startswith`` filter in Python so a ``None`` prefix returns every
        topic. A topics-only subscription on ``"/"`` is kept so server
        announcements populate the topic store; the first call after
        connect waits briefly — yielding via ``await asyncio.sleep`` — for
        the initial announcements to arrive.
        """
        inst = self._instance()
        if self._client_running:
            _, poller = self._ensure_topics_subscription()
            if self._topics_synced:
                poller.readQueue()  # drain; store updates happen in the background
            else:
                deadline = time.monotonic() + _ANNOUNCE_WAIT_SECONDS
                while not poller.readQueue() and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                self._topics_synced = True
        infos = inst.getTopicInfo()
        if prefix is None:
            return [info.name for info in infos]
        return [info.name for info in infos if info.name.startswith(prefix)]

    async def get(self, topic: str) -> Any:
        """Return the JSON-normalized value of ``topic`` or ``None``.

        Unpublished topics produce an invalid ``Value``, which maps to
        ``None`` instead of raising. Metadata (valid/type/time) is never
        exposed; the raw value is normalized through ``_to_jsonable``.

        A client only receives a remote topic's value while a
        subscription exists, so the first read of a topic creates a
        cached ``GenericSubscriber`` and polls the entry store — yielding
        via ``await asyncio.sleep`` — until the value arrives or
        ``_READ_TIMEOUT_SECONDS`` elapses. When the client is not
        running, an invalid value is returned as ``None`` immediately
        without polling.
        """
        inst = self._instance()
        entry = inst.getEntry(topic)
        value = entry.getValue()
        if not value.isValid() and self._client_running:
            subscriber = self._value_subs.get(topic)
            if subscriber is None:
                subscriber = entry.getTopic().genericSubscribe()
                self._value_subs[topic] = subscriber
            deadline = time.monotonic() + _READ_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                value = entry.getValue()
                if value.isValid():
                    break
                await asyncio.sleep(0.01)
            for _ in subscriber.readQueue():  # drain; store updates in the background
                pass
        if not value.isValid():
            return None
        return _to_jsonable(value.value())

    async def get_multiple(self, topics: list[str]) -> dict[str, Any]:
        """Return ``{topic: value}`` for each topic.

        Unpublished topics map to ``None``; every requested key is present
        in the result. Reads run sequentially in submission order.
        """
        return {topic: await self.get(topic) for topic in topics}

    async def get_info(self, topic: str) -> dict | None:
        """Return JSON-serializable metadata for ``topic`` or ``None``.

        ``{"name": str, "type_str": str, "properties": str}`` built from
        the matching ``TopicInfo``; ``None`` if the topic is not published.
        Pure store read — no polling, so the coroutine never yields.
        """
        for info in self._instance().getTopicInfo():
            if info.name == topic:
                return {
                    "name": info.name,
                    "type_str": info.type_str,
                    "properties": info.properties,
                }
        return None

    def set(self, topic: str, value: Any) -> bool:
        """Publish ``value`` to ``topic``; return the setter's bool result.

        Dispatches on the Python type — bool checked before int — to the
        matching entry setter; homogeneous lists use the array setters.
        Unsupported or mixed types return ``False`` without raising.
        """
        entry = self._instance().getEntry(topic)
        if isinstance(value, bool):
            return entry.setBoolean(value)
        if isinstance(value, int):
            return entry.setInteger(value)
        if isinstance(value, float):
            return entry.setDouble(value)
        if isinstance(value, str):
            return entry.setString(value)
        if isinstance(value, list):
            kind = _array_kind(value)
            if kind is None:
                return False
            match kind:
                case "boolean":
                    return entry.setBooleanArray(value)
                case "integer":
                    return entry.setIntegerArray(value)
                case "double":
                    return entry.setDoubleArray(value)
                case "string":
                    return entry.setStringArray(value)
                case unreachable:
                    assert_never(unreachable)
        return False

    def set_multiple(self, updates: dict[str, Any]) -> dict[str, bool]:
        """Write every ``{topic: value}`` pair; return per-topic results."""
        return {topic: self.set(topic, value) for topic, value in updates.items()}

    async def subscribe(
        self, prefixes: list[str], duration: float = 10.0
    ) -> dict[str, list[dict]]:
        """Sample every value update under ``prefixes`` for ``duration`` s.

        Returns ``{topic: [{"time": float_s, "value": jsonable}, ...]}``.
        ``kImmediate`` emits the current value once per topic, then
        ``kValueAll`` on every change. Disconnect or no published topics
        yield ``{}``; subscriber and poller are always closed.
        """
        inst = self._instance()
        subscriber = ntcore.MultiSubscriber(inst, prefixes, ntcore.PubSubOptions())
        poller = ntcore.NetworkTableListenerPoller(inst)
        poller.addListener(
            subscriber, ntcore.EventFlags.kValueAll | ntcore.EventFlags.kImmediate
        )
        results: dict[str, list[dict]] = {}
        end = time.monotonic() + duration
        try:
            while True:
                for event in poller.readQueue():
                    data = event.data
                    results.setdefault(data.topic.getName(), []).append(
                        {
                            "time": data.value.last_change() / 1_000_000.0,
                            "value": _to_jsonable(data.value.value()),
                        }
                    )
                remaining = end - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.02, remaining))
        finally:
            subscriber.close()
            poller.close()
        return results
