"""
Execution Environment Module
Provides isolated environments for executing pipeline steps.

How it works:
1. Each step runs in an isolated environment (subprocess by default)
2. Environment variables are inherited but can be overridden per step
3. Commands execute with proper timeout handling
4. stdout/stderr are captured in real-time and streamed to log handlers
5. Exit codes determine step success/failure
6. Working directory can be configured per step

Future support: Docker containers, Kubernetes pods, virtual machines
"""

import asyncio
import os
import subprocess
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

from .models import Step


class ExecutionError(Exception):
    pass


class ExecutionEnvironment(ABC):
    """Abstract base class for execution environments."""

    @abstractmethod
    async def execute(
        self,
        step: Step,
        env: Dict[str, str],
        log_callback: Optional[Callable[[str, str], None]] = None,
        timeout: Optional[int] = None,
    ) -> Tuple[int, List[str], List[str]]:
        """
        Execute a step's commands in the isolated environment.

        Args:
            step: The step to execute
            env: Environment variables to set
            log_callback: Callback for real-time log streaming (message, stream)
            timeout: Timeout in seconds

        Returns:
            Tuple of (exit_code, stdout_lines, stderr_lines)
        """
        pass


class SubprocessEnvironment(ExecutionEnvironment):
    """
    Executes steps in isolated subprocesses.

    Isolation Features:
    - Each command runs in a separate subprocess
    - Environment variables are merged (global + step-specific)
    - Working directory can be isolated per step
    - Process group management for proper cleanup
    - Timeout enforcement

    Example:
        env = SubprocessEnvironment(base_workdir="/tmp/pipelines")
        exit_code, stdout, stderr = await env.execute(
            step=step,
            env={"PATH": "/usr/bin"},
            log_callback=lambda msg, stream: print(f"[{stream}] {msg}")
        )
    """

    def __init__(
        self,
        base_workdir: Optional[str] = None,
        shell: Optional[str] = None,
        cleanup_process_group: bool = True,
    ):
        """
        Args:
            base_workdir: Base working directory for all steps
            shell: Shell to use for command execution (default: /bin/bash on Unix, cmd.exe on Windows)
            cleanup_process_group: Whether to kill entire process group on timeout/error
        """
        self.base_workdir = Path(base_workdir) if base_workdir else Path.cwd()
        self.shell = shell or self._get_default_shell()
        self.cleanup_process_group = cleanup_process_group

    def _get_default_shell(self) -> str:
        if sys.platform.startswith("win"):
            return "cmd.exe"
        return "/bin/bash"

    async def execute(
        self,
        step: Step,
        env: Dict[str, str],
        log_callback: Optional[Callable[[str, str], None]] = None,
        timeout: Optional[int] = None,
    ) -> Tuple[int, List[str], List[str]]:
        workdir = self._get_workdir(step)
        merged_env = self._merge_environment(env, step.environment)

        all_stdout: List[str] = []
        all_stderr: List[str] = []
        final_exit_code = 0

        for cmd_idx, command in enumerate(step.commands):
            exit_code, stdout, stderr = await self._execute_single_command(
                command=command,
                workdir=workdir,
                env=merged_env,
                log_callback=log_callback,
                timeout=timeout or step.timeout,
                command_index=cmd_idx,
            )

            all_stdout.extend(stdout)
            all_stderr.extend(stderr)
            final_exit_code = exit_code

            if exit_code != 0:
                if log_callback:
                    log_callback(
                        f"Command failed with exit code {exit_code}, "
                        f"stopping step execution",
                        "stderr",
                    )
                break

        return final_exit_code, all_stdout, all_stderr

    async def _execute_single_command(
        self,
        command: str,
        workdir: Path,
        env: Dict[str, str],
        log_callback: Optional[Callable[[str, str], None]],
        timeout: int,
        command_index: int,
    ) -> Tuple[int, List[str], List[str]]:
        if log_callback:
            log_callback(f"$ {command}", "command")

        stdout_lines: List[str] = []
        stderr_lines: List[str] = []

        try:
            kwargs = {
                "stdout": asyncio.subprocess.PIPE,
                "stderr": asyncio.subprocess.PIPE,
                "cwd": str(workdir),
                "env": env,
                "shell": True,
            }

            if sys.platform.startswith("win"):
                kwargs["start_new_session"] = False
                full_command = command
            else:
                kwargs["executable"] = self.shell
                kwargs["start_new_session"] = self.cleanup_process_group
                full_command = command

            process = await asyncio.create_subprocess_shell(
                full_command,
                **kwargs,
            )

            async def read_stream(
                stream: asyncio.StreamReader,
                stream_name: str,
                lines: List[str],
            ) -> None:
                while True:
                    line_bytes = await stream.readline()
                    if not line_bytes:
                        break
                    try:
                        line = line_bytes.decode("utf-8", errors="replace").rstrip(
                            "\n\r"
                        )
                    except Exception:
                        line = str(line_bytes)

                    lines.append(line)
                    if log_callback:
                        log_callback(line, stream_name)

            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        read_stream(process.stdout, "stdout", stdout_lines),
                        read_stream(process.stderr, "stderr", stderr_lines),
                    ),
                    timeout=timeout,
                )
                await process.wait()
            except asyncio.TimeoutError:
                if self.cleanup_process_group:
                    self._kill_process_group(process.pid)
                else:
                    process.kill()
                await process.wait()

                error_msg = f"Command timed out after {timeout}s"
                stderr_lines.append(error_msg)
                if log_callback:
                    log_callback(error_msg, "stderr")
                return -1, stdout_lines, stderr_lines

            exit_code = process.returncode or 0
            return exit_code, stdout_lines, stderr_lines

        except Exception as e:
            error_msg = f"Execution error: {str(e)}"
            stderr_lines.append(error_msg)
            if log_callback:
                log_callback(error_msg, "stderr")
            return -1, stdout_lines, stderr_lines

    def _kill_process_group(self, pid: int) -> None:
        """Kill the entire process group."""
        try:
            if sys.platform.startswith("win"):
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    capture_output=True,
                )
            else:
                import signal

                os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:
            pass

    def _get_workdir(self, step: Step) -> Path:
        """Get the working directory for a step."""
        if step.working_dir:
            workdir = Path(step.working_dir)
        else:
            workdir = self.base_workdir / step.stage / step.name

        workdir.mkdir(parents=True, exist_ok=True)
        return workdir

    def _merge_environment(
        self,
        global_env: Dict[str, str],
        step_env: Dict[str, str],
    ) -> Dict[str, str]:
        """
        Merge environment variables with proper precedence.
        Step env overrides global env, which overrides inherited env.
        """
        merged = os.environ.copy()
        merged.update(global_env)
        merged.update(step_env)

        merged = {k: str(v) for k, v in merged.items()}

        return merged


class DockerEnvironment(ExecutionEnvironment):
    """
    Executes steps in Docker containers (future implementation).

    Would provide:
    - Complete filesystem isolation
    - Resource limits (CPU, memory, disk)
    - Network isolation
    - Clean environment per step
    - Image-based reproducibility
    """

    def __init__(self, image: str = "python:3.11-slim"):
        self.image = image
        raise NotImplementedError(
            "DockerEnvironment is planned but not yet implemented. "
            "Use SubprocessEnvironment for now."
        )

    async def execute(
        self,
        step: Step,
        env: Dict[str, str],
        log_callback: Optional[Callable[[str, str], None]] = None,
        timeout: Optional[int] = None,
    ) -> Tuple[int, List[str], List[str]]:
        raise NotImplementedError()
