"""NetworkTables connection and state management for nt-mcp-server.

Pure NetworkTables layer: knows about ntcore, knows nothing about MCP,
FastMCP, or tool definitions. The server layer calls into NTManager to
control the client lifecycle and read connection state.
"""

from __future__ import annotations

import asyncio
import fnmatch
import re
import time
from collections.abc import AsyncIterator
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import ntcore

from .nt_types import TopicSelector, array_kind, summarize, to_jsonable, type_warning

try:  # assert_never: typing on 3.11+, typing_extensions on 3.10
    from typing import assert_never
except ImportError:  # pragma: no cover - Python 3.10
    from typing_extensions import assert_never

# NT4 clients only receive topics/values they subscribe to. Read methods are
# async and poll the entry store with ``await asyncio.sleep`` for these
# bounds after ensuring a subscription exists, so the event loop is never
# blocked by a read.
_READ_TIMEOUT_SECONDS = 1.0
_ANNOUNCE_WAIT_SECONDS = 0.5

DEFAULT_SERVER_IP = "127.0.0.1"
DEFAULT_SERVER_PORT = 5810


class NTConnectError(RuntimeError):
    """Base error for connect target conflicts."""


class AmbiguousTargetError(NTConnectError):
    """Both a team number and a non-default server IP were requested."""


class TargetMismatchError(NTConnectError):
    """A running client was asked to connect to a different target."""


def _package_version() -> str:
    """Return the installed package version, or ``"unknown"``."""
    try:
        return version("nt-mcp-server")
    except PackageNotFoundError:
        return "unknown"


class StreamDecimator:
    """Stateful per-topic sample filter shared by live and file capture.

    ``keep(topic, value, time_seconds)`` returns ``False`` when the sample
    must be dropped: ``sample_interval`` keeps at most one sample per topic
    per interval (using the event timestamp); ``change_only`` skips samples
    whose numeric value changed by ``<= change_only`` (bools always compare
    by equality, as do non-numeric values). A dropped sample never updates
    the per-topic state, matching the original inline ``subscribe()``
    logic byte-for-byte for a fixed sample sequence.
    """

    def __init__(
        self,
        sample_interval: float | None = None,
        change_only: float | None = None,
    ) -> None:
        self._sample_interval = sample_interval
        self._change_only = change_only
        self._last_time: dict[str, float] = {}
        self._last_value: dict[str, Any] = {}
        self._sentinel = object()

    def keep(self, topic: str, value: Any, time_seconds: float) -> bool:
        """Return whether the sample passes both filters."""
        if self._sample_interval is not None:
            interval = self._sample_interval
            if time_seconds - self._last_time.get(topic, -interval) < interval:
                return False
            self._last_time[topic] = time_seconds
        if self._change_only is not None:
            prev = self._last_value.get(topic, self._sentinel)
            if prev is not self._sentinel:
                if isinstance(value, (int, float)) and isinstance(prev, (int, float)):
                    if (
                        not isinstance(value, bool)
                        and not isinstance(prev, bool)
                        and abs(value - prev) <= self._change_only
                    ):
                        return False
                elif value == prev:
                    return False
            self._last_value[topic] = value
        return True


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
        self._target: dict[str, Any] | None = None
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

    @property
    def target(self) -> dict[str, Any] | None:
        """Return the resolved connect target, or ``None`` before connect.

        JSON-safe: ``{"kind": "server", "server_ip": str, "server_port":
        int}`` or ``{"kind": "team", "team_number": int, "server_port":
        int}``.
        """
        return self._target

    def _target_key(self, target: dict[str, Any]) -> tuple[Any, ...]:
        """Return a hashable canonical key for a resolved target dict."""
        return tuple(sorted(target.items()))

    def connect(
        self,
        server_ip: str = DEFAULT_SERVER_IP,
        server_port: int = DEFAULT_SERVER_PORT,
        team_number: int | None = None,
        identity: str = "nt-mcp",
    ) -> None:
        """Start the NT4 client against a server or team number.

        Pass exactly one target: ``team_number`` (which resolves the
        server address from the team number) or a non-default
        ``server_ip``/``server_port`` pair. The resolved target is
        recorded on :attr:`target` and reported by ``connection_info``.

        Raises :class:`AmbiguousTargetError` when both a ``team_number``
        and a non-default ``server_ip`` are given, before any state
        changes. Raises :class:`TargetMismatchError` when the client is
        already running against a different target — call ``disconnect``
        first. Connecting to the same target while running is an
        idempotent no-op.
        """
        if team_number is not None and server_ip != DEFAULT_SERVER_IP:
            raise AmbiguousTargetError(
                f"both team_number={team_number!r} and server_ip={server_ip!r} "
                "were given; pass exactly one target (team_number, or "
                "server_ip/server_port with the default server_ip)"
            )
        if team_number is not None:
            target: dict[str, Any] = {
                "kind": "team",
                "team_number": team_number,
                "server_port": server_port,
            }
        else:
            target = {
                "kind": "server",
                "server_ip": server_ip,
                "server_port": server_port,
            }
        requested_key = self._target_key(target)
        if self._client_running:
            if self._target is None or self._target_key(self._target) != requested_key:
                raise TargetMismatchError(
                    f"client is already running against target "
                    f"{self._target!r}; refusing to re-target to {target!r} — "
                    "call nt_disconnect first"
                )
            return
        inst = self._instance()
        if target["kind"] == "team":
            inst.setServerTeam(target["team_number"], target["server_port"])
        else:
            inst.setServer(target["server_ip"], target["server_port"])
        inst.startClient4(identity)
        self._client_running = True
        self._target = target

    def disconnect(self) -> None:
        """Stop the NT4 client. Idempotent; safe before any connect."""
        if self._client_running and self._inst is not None:
            self._inst.stopClient()
        self._client_running = False
        self._target = None
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
        "last_update"}], "target": resolved_target | None, "version":
        str}``.
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
            "target": self._target,
            "version": _package_version(),
        }
        return to_jsonable(info)

    def is_connected(self) -> bool:
        """Return whether the NT4 client currently has a server connection."""
        return self._instance().isConnected()

    async def list_topics(
        self,
        prefix: str | None = None,
        regex: str | None = None,
        wildcard: str | None = None,
    ) -> list[str]:
        """Return the names of all topics, optionally filtered.

        ``prefix`` matches ``topic.startswith(prefix)``. ``regex`` matches
        the full topic name against the provided pattern. ``wildcard`` uses
        shell-style globbing via :func:`fnmatch.fnmatch`. All filters are
        applied together when more than one is supplied.

        Uses ``NetworkTableInstance.getTopicInfo()`` and applies the filters
        in Python. A topics-only subscription on ``"/"`` is kept so server
        announcements populate the topic store; the first call after connect
        waits briefly — yielding via ``await asyncio.sleep`` — for the
        initial announcements to arrive.
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
        names = [info.name for info in infos]
        if prefix is not None:
            names = [name for name in names if name.startswith(prefix)]
        if regex is not None:
            pattern = re.compile(regex)
            names = [name for name in names if pattern.search(name)]
        if wildcard is not None:
            names = [name for name in names if fnmatch.fnmatch(name, wildcard)]
        return names

    async def get(self, topic: str) -> Any:
        """Return the JSON-normalized value of ``topic`` or ``None``.

        Unpublished topics produce an invalid ``Value``, which maps to
        ``None`` instead of raising. Metadata (valid/type/time) is never
        exposed; the raw value is normalized through ``to_jsonable``.

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
        return to_jsonable(value.value())

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

    def _topic_type_str(self, topic: str) -> str | None:
        """Return the published type string for ``topic``, or ``None``."""
        for info in self._instance().getTopicInfo():
            if info.name == topic:
                return info.type_str
        return None

    def set(
        self, topic: str, value: Any, strict_type_check: bool = False
    ) -> dict[str, Any]:
        """Publish ``value`` to ``topic``.

        Returns ``{"ok": bool, "warning": str | None}``. If the topic is
        already published with a known type and ``value`` does not match,
        a warning is returned. When ``strict_type_check`` is ``True`` the
        write is refused and ``ok`` is ``False``; otherwise the write is
        still attempted.

        Dispatches on the Python type — bool checked before int — to the
        matching entry setter; homogeneous lists use the array setters.
        Unsupported or mixed types set ``ok`` to ``False``.
        """
        warning: str | None = None
        type_str = self._topic_type_str(topic)
        if type_str is not None:
            warning = type_warning(value, type_str)
            if warning and strict_type_check:
                return {"ok": False, "warning": warning}

        entry = self._instance().getEntry(topic)
        ok = False
        if isinstance(value, bool):
            ok = entry.setBoolean(value)
        elif isinstance(value, int):
            ok = entry.setInteger(value)
        elif isinstance(value, float):
            ok = entry.setDouble(value)
        elif isinstance(value, str):
            ok = entry.setString(value)
        elif isinstance(value, list):
            kind = array_kind(value)
            if kind is not None:
                match kind:
                    case "boolean":
                        ok = entry.setBooleanArray(value)
                    case "integer":
                        ok = entry.setIntegerArray(value)
                    case "double":
                        ok = entry.setDoubleArray(value)
                    case "string":
                        ok = entry.setStringArray(value)
                    case unreachable:
                        assert_never(unreachable)
        return {"ok": ok, "warning": warning}

    def set_multiple(
        self, updates: dict[str, Any], strict_type_check: bool = False
    ) -> dict[str, Any]:
        """Write every ``{topic: value}`` pair.

        Returns ``{"results": {topic: bool}, "warnings": {topic: str | None}}``.
        ``strict_type_check`` is applied per-topic.
        """
        results: dict[str, bool] = {}
        warnings: dict[str, str | None] = {}
        for topic, value in updates.items():
            res = self.set(topic, value, strict_type_check=strict_type_check)
            results[topic] = res["ok"]
            warnings[topic] = res["warning"]
        return {"results": results, "warnings": warnings}

    async def subscribe_stream(
        self,
        prefixes: list[str] | None = None,
        topics: list[str] | None = None,
        regex: str | None = None,
        wildcard: str | None = None,
    ) -> AsyncIterator[tuple[str, Any, float]]:
        """Yield matching ``(topic, value, time_seconds)`` updates.

        Runs indefinitely until the consumer stops iterating. Each stream
        owns its own ``MultiSubscriber`` and ``NetworkTableListenerPoller``
        (fresh per call; never reused from ``self``); both are closed in a
        ``finally`` block when the generator is closed. ``kImmediate`` emits
        the current value once per topic, then ``kValueAll`` on every change.
        Prefix and exact-topic lists are ORed internally; different selector
        kinds are ANDed. At least one selector kind is required.
        """
        selector = TopicSelector(prefixes, topics, regex, wildcard)
        if selector.empty:
            raise ValueError("at least one topic selector is required")
        inst = self._instance()
        subscriber = ntcore.MultiSubscriber(
            inst, selector.subscription_prefixes, ntcore.PubSubOptions()
        )
        poller = ntcore.NetworkTableListenerPoller(inst)
        poller.addListener(
            subscriber, ntcore.EventFlags.kValueAll | ntcore.EventFlags.kImmediate
        )
        try:
            while True:
                events = poller.readQueue()
                if events:
                    for event in events:
                        data = event.data
                        topic = data.topic.getName()
                        if not selector.matches(topic):
                            continue
                        yield (
                            topic,
                            to_jsonable(data.value.value()),
                            data.value.last_change() / 1_000_000.0,
                        )
                else:
                    await asyncio.sleep(0.02)
        finally:
            subscriber.close()
            poller.close()

    async def subscribe(
        self,
        prefixes: list[str] | None = None,
        duration: float = 10.0,
        sample_interval: float | None = None,
        change_only: float | None = None,
        format: str = "samples",
        limit: int | None = None,
        topics: list[str] | None = None,
        regex: str | None = None,
        wildcard: str | None = None,
    ) -> dict[str, Any]:
        """Sample selected value updates for ``duration`` seconds.

        Returns ``{topic: samples | summary}``. Each sample is
        ``{"time": float_s, "value": jsonable}``. ``sample_interval``
        decimates to at most one sample per topic per interval (using the
        event timestamp). ``change_only`` skips samples whose numeric value
        changed by ``<= change_only``; non-numeric values are skipped only
        when unchanged. ``format="summary"`` replaces each topic's sample
        list with ``{"count", "last_value", "last_time", "min", "max",
        "mean"}``. ``limit`` caps each topic's collected sample list to its
        first ``limit`` samples.

        ``asyncio.timeout`` enforces the deadline when no events flow; the
        monotonic check provides an early exit when events flow past the
        deadline.
        """
        results: dict[str, list[dict]] = {}
        end = time.monotonic() + duration
        decimator = StreamDecimator(
            sample_interval=sample_interval, change_only=change_only
        )
        try:
            async with asyncio.timeout(duration):
                async for topic, value, time_seconds in self.subscribe_stream(
                    prefixes, topics, regex, wildcard
                ):
                    if not decimator.keep(topic, value, time_seconds):
                        continue
                    results.setdefault(topic, []).append(
                        {"time": time_seconds, "value": value}
                    )
                    if time.monotonic() >= end:
                        break
        except TimeoutError:
            pass
        if limit is not None:
            results = {topic: samples[:limit] for topic, samples in results.items()}
        if format == "summary":
            return {topic: summarize(samples) for topic, samples in results.items()}
        return results
