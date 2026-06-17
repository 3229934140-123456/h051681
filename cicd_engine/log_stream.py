"""
Log Stream Module
Handles real-time log collection, streaming, and persistence for pipeline steps.

How it works:
1. Each step's execution produces logs (stdout, stderr, command metadata)
2. Logs are captured in real-time as the step runs
3. Multiple log handlers can subscribe to receive logs
4. Logs are persisted to storage for later retrieval
5. Supports streaming via async generators for live viewing
6. Logs are tagged with step name, timestamp, and stream type

Log Flow:
    [Subprocess] → stdout/stderr → [LogStream.capture]
                                        ↓
                                  [Log Handlers]
                                    ├─ Console output
                                    ├─ File storage
                                    ├─ WebSocket streaming
                                    └─ Database persistence
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Set
from collections import defaultdict

from .models import LogEntry


class LogStreamError(Exception):
    pass


class LogHandler:
    """Base class for log handlers."""

    async def handle(self, entry: LogEntry) -> None:
        pass

    async def flush(self) -> None:
        pass


class ConsoleLogHandler(LogHandler):
    """Prints logs to console with color coding."""

    COLORS = {
        "stdout": "\033[0m",
        "stderr": "\033[91m",
        "command": "\033[94m",
        "info": "\033[92m",
        "error": "\033[91m",
        "reset": "\033[0m",
    }

    def __init__(self, use_colors: bool = True):
        self.use_colors = use_colors and os.name != "nt"

    async def handle(self, entry: LogEntry) -> None:
        prefix = f"[{entry.timestamp.strftime('%H:%M:%S')}] "
        prefix += f"[{entry.stage_name}/{entry.step_name}] "

        color = self.COLORS.get(entry.stream, "") if self.use_colors else ""
        reset = self.COLORS["reset"] if self.use_colors else ""

        print(f"{color}{prefix}{entry.message}{reset}", flush=True)


class FileLogHandler(LogHandler):
    """Persists logs to files organized by run and step."""

    def __init__(self, base_dir: str = "./logs"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._files: Dict[str, Any] = {}
        self._buffers: Dict[str, List[str]] = defaultdict(list)

    def _get_log_file(self, run_id: str, step_full_name: str) -> Path:
        stage, step = step_full_name.split("/", 1)
        log_dir = self.base_dir / run_id / stage
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / f"{step}.log"

    async def handle(self, entry: LogEntry, run_id: Optional[str] = None) -> None:
        step_full_name = f"{entry.stage_name}/{entry.step_name}"
        log_line = json.dumps(
            {
                "timestamp": entry.timestamp.isoformat(),
                "step": step_full_name,
                "stream": entry.stream,
                "message": entry.message,
            },
            ensure_ascii=False,
        )

        if run_id:
            self._buffers[f"{run_id}/{step_full_name}"].append(log_line)
            await self._flush_buffer(run_id, step_full_name)

    async def _flush_buffer(self, run_id: str, step_full_name: str) -> None:
        key = f"{run_id}/{step_full_name}"
        buffer = self._buffers[key]
        if not buffer:
            return

        log_file = self._get_log_file(run_id, step_full_name)
        async with asyncio.Lock():
            with open(log_file, "a", encoding="utf-8") as f:
                for line in buffer:
                    f.write(line + "\n")
            self._buffers[key] = []

    def get_logs(
        self, run_id: str, step_full_name: Optional[str] = None
    ) -> List[LogEntry]:
        """Read logs from storage."""
        entries: List[LogEntry] = []

        if step_full_name:
            log_file = self._get_log_file(run_id, step_full_name)
            entries.extend(self._read_log_file(log_file))
        else:
            run_dir = self.base_dir / run_id
            if run_dir.exists():
                for stage_dir in run_dir.iterdir():
                    if stage_dir.is_dir():
                        for log_file in stage_dir.glob("*.log"):
                            entries.extend(self._read_log_file(log_file))

        entries.sort(key=lambda e: e.timestamp)
        return entries

    def _read_log_file(self, log_file: Path) -> List[LogEntry]:
        entries: List[LogEntry] = []
        if not log_file.exists():
            return entries

        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    step_parts = data["step"].split("/", 1)
                    entries.append(
                        LogEntry(
                            timestamp=datetime.fromisoformat(data["timestamp"]),
                            step_name=step_parts[1] if len(step_parts) > 1 else step_parts[0],
                            stage_name=step_parts[0],
                            message=data["message"],
                            stream=data.get("stream", "stdout"),
                        )
                    )
                except (json.JSONDecodeError, KeyError):
                    continue

        return entries

    async def flush(self) -> None:
        pass


class CallbackLogHandler(LogHandler):
    """Delegates log handling to a callback function."""

    def __init__(self, callback: Callable[[LogEntry], None]):
        self.callback = callback

    async def handle(self, entry: LogEntry) -> None:
        self.callback(entry)


class LogStream:
    """
    Central log streaming service that collects and distributes logs.

    Features:
    - Real-time log capture from running steps
    - Multiple handler support (console, file, custom callbacks)
    - Async generator for live log streaming
    - Log persistence and retrieval
    - Filtering by step, stage, or stream type

    Example:
        log_stream = LogStream()
        log_stream.add_handler(ConsoleLogHandler())
        log_stream.add_handler(FileLogHandler("./logs"))

        # Create a log capturer for a step
        capturer = log_stream.create_capturer(
            step_name="compile",
            stage_name="build",
            run_id="run_123"
        )

        # Pass to execution environment
        exit_code, stdout, stderr = await env.execute(
            step=step,
            env={},
            log_callback=capturer.callback
        )

        # Stream logs live
        async for entry in log_stream.stream(run_id="run_123"):
            print(f"[{entry.stream}] {entry.message}")
    """

    def __init__(self):
        self._handlers: List[LogHandler] = []
        self._subscribers: Dict[str, Set[asyncio.Queue]] = defaultdict(set)
        self._all_logs: Dict[str, List[LogEntry]] = defaultdict(list)

    def add_handler(self, handler: LogHandler) -> None:
        """Add a log handler."""
        self._handlers.append(handler)

    def remove_handler(self, handler: LogHandler) -> None:
        """Remove a log handler."""
        if handler in self._handlers:
            self._handlers.remove(handler)

    def create_capturer(
        self,
        step_name: str,
        stage_name: str,
        run_id: str,
    ) -> "LogCapturer":
        """
        Create a log capturer for a specific step execution.

        The capturer provides a callback that can be passed to the
        execution environment for real-time log collection.
        """
        return LogCapturer(
            step_name=step_name,
            stage_name=stage_name,
            run_id=run_id,
            log_stream=self,
        )

    async def log(
        self,
        message: str,
        step_name: str,
        stage_name: str,
        run_id: str,
        stream: str = "stdout",
    ) -> None:
        """
        Log a message for a step.
        Distributes to all handlers and subscribers.
        """
        entry = LogEntry(
            timestamp=datetime.now(),
            step_name=step_name,
            stage_name=stage_name,
            message=message,
            stream=stream,
        )

        self._all_logs[run_id].append(entry)

        for handler in self._handlers:
            try:
                if hasattr(handler, 'handle'):
                    sig = handler.handle.__code__.co_varnames
                    if 'run_id' in sig:
                        await handler.handle(entry, run_id=run_id)
                    else:
                        await handler.handle(entry)
            except Exception:
                pass

        for queue in list(self._subscribers.get(run_id, [])):
            try:
                await queue.put(entry)
            except Exception:
                pass

        for queue in list(self._subscribers.get("*", [])):
            try:
                await queue.put(entry)
            except Exception:
                pass

    async def stream(
        self,
        run_id: str,
        step_full_name: Optional[str] = None,
        stream_filter: Optional[List[str]] = None,
    ) -> AsyncGenerator[LogEntry, None]:
        """
        Stream logs in real-time as they are produced.

        Args:
            run_id: The pipeline run ID to stream logs for
            step_full_name: Optional filter for specific step (stage/step)
            stream_filter: Optional filter for stream types (stdout, stderr, etc.)

        Yields:
            LogEntry objects as they are produced
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        subscriber_key = run_id

        self._subscribers[subscriber_key].add(queue)

        try:
            for entry in self._all_logs.get(run_id, []):
                if self._match_filter(entry, step_full_name, stream_filter):
                    yield entry

            while True:
                entry = await queue.get()
                if entry is None:
                    break

                if self._match_filter(entry, step_full_name, stream_filter):
                    yield entry

                queue.task_done()
        finally:
            self._subscribers[subscriber_key].discard(queue)

    def _match_filter(
        self,
        entry: LogEntry,
        step_full_name: Optional[str],
        stream_filter: Optional[List[str]],
    ) -> bool:
        if step_full_name:
            entry_full_name = f"{entry.stage_name}/{entry.step_name}"
            if entry_full_name != step_full_name:
                return False

        if stream_filter and entry.stream not in stream_filter:
            return False

        return True

    def get_logs(
        self,
        run_id: str,
        step_full_name: Optional[str] = None,
        stream_filter: Optional[List[str]] = None,
    ) -> List[LogEntry]:
        """Get historical logs for a run."""
        logs = self._all_logs.get(run_id, [])
        return [
            entry
            for entry in logs
            if self._match_filter(entry, step_full_name, stream_filter)
        ]

    def close_run(self, run_id: str) -> None:
        """Signal that a run has completed, waking all streamers."""
        for queue in list(self._subscribers.get(run_id, [])):
            try:
                queue.put_nowait(None)
            except Exception:
                pass


class LogCapturer:
    """
    Captures logs from a step execution and forwards them to the LogStream.

    Provides a callback interface compatible with the execution environment's
    log_callback parameter.
    """

    def __init__(
        self,
        step_name: str,
        stage_name: str,
        run_id: str,
        log_stream: LogStream,
    ):
        self.step_name = step_name
        self.stage_name = stage_name
        self.run_id = run_id
        self.log_stream = log_stream
        self.logs: List[LogEntry] = []

    @property
    def callback(self) -> Callable[[str, str], None]:
        """
        Returns a callback function suitable for passing to
        ExecutionEnvironment.execute().

        Callback signature: (message: str, stream: str) -> None
        """

        def _callback(message: str, stream: str = "stdout") -> None:
            entry = LogEntry(
                timestamp=datetime.now(),
                step_name=self.step_name,
                stage_name=self.stage_name,
                message=message,
                stream=stream,
            )
            self.logs.append(entry)

            asyncio.create_task(
                self.log_stream.log(
                    message=message,
                    step_name=self.step_name,
                    stage_name=self.stage_name,
                    run_id=self.run_id,
                    stream=stream,
                )
            )

        return _callback

    def get_log_lines(self) -> List[str]:
        """Get all captured log messages as plain strings."""
        return [entry.message for entry in self.logs]
