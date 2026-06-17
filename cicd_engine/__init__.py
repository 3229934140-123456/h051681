"""
CI/CD Pipeline Execution Engine
Modules:
- pipeline_parser: Pipeline definition parsing and dependency graph construction
- execution_environment: Isolated execution environments (subprocess/container)
- artifact_manager: Artifact management and transfer between steps
- log_stream: Real-time log streaming
- scheduler: Dependency-based scheduling and parallel execution
- state_manager: State aggregation and persistence
"""

from .pipeline_parser import Pipeline, PipelineParser, PipelineParserError
from .execution_environment import ExecutionEnvironment, SubprocessEnvironment
from .artifact_manager import ArtifactManager
from .log_stream import (
    LogStream,
    ConsoleLogHandler,
    FileLogHandler,
    CallbackLogHandler,
    LogCapturer,
)
from .scheduler import PipelineScheduler, ConditionEvaluator, SchedulerError
from .state_manager import StateManager
from .models import (
    StepStatus,
    StepResult,
    PipelineStatus,
    PipelineResult,
    FailureStrategy,
    Step,
    Artifact,
    LogEntry,
)

__all__ = [
    "Pipeline",
    "PipelineParser",
    "PipelineParserError",
    "ExecutionEnvironment",
    "SubprocessEnvironment",
    "ArtifactManager",
    "LogStream",
    "ConsoleLogHandler",
    "FileLogHandler",
    "CallbackLogHandler",
    "LogCapturer",
    "PipelineScheduler",
    "ConditionEvaluator",
    "SchedulerError",
    "StateManager",
    "StepStatus",
    "StepResult",
    "PipelineStatus",
    "PipelineResult",
    "FailureStrategy",
    "Step",
    "Artifact",
    "LogEntry",
]
