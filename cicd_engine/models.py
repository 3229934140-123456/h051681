"""
Core data models for the CI/CD pipeline engine.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime


class StepStatus(str, Enum):
    PENDING = "pending"
    SKIPPED = "skipped"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


class FailureStrategy(str, Enum):
    ABORT = "abort"
    CONTINUE = "continue"
    ROLLBACK = "rollback"


@dataclass
class Step:
    name: str
    stage: str
    commands: List[str]
    depends_on: List[str] = field(default_factory=list)
    environment: Dict[str, str] = field(default_factory=dict)
    artifacts: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)
    condition: Optional[str] = None
    failure_strategy: FailureStrategy = FailureStrategy.ABORT
    timeout: int = 3600
    working_dir: Optional[str] = None

    def get_full_name(self) -> str:
        return f"{self.stage}/{self.name}"


@dataclass
class StepResult:
    step_name: str
    stage_name: str
    status: StepStatus
    exit_code: Optional[int] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    logs: List[str] = field(default_factory=list)
    outputs: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    def get_full_name(self) -> str:
        return f"{self.stage_name}/{self.step_name}"


@dataclass
class Pipeline:
    name: str
    stages: List[str]
    steps: Dict[str, Step]
    variables: Dict[str, Any] = field(default_factory=dict)
    environment: Dict[str, str] = field(default_factory=dict)

    def get_step(self, full_name: str) -> Optional[Step]:
        return self.steps.get(full_name)

    def get_all_step_names(self) -> List[str]:
        return list(self.steps.keys())


@dataclass
class PipelineResult:
    pipeline_name: str
    status: PipelineStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    def get_duration(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def get_failed_steps(self) -> List[StepResult]:
        return [
            result
            for result in self.step_results.values()
            if result.status == StepStatus.FAILED
        ]


@dataclass
class Artifact:
    name: str
    path: str
    step_name: str
    stage_name: str
    source_rel_path: str = ""
    size: int = 0
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class LogEntry:
    timestamp: datetime
    step_name: str
    stage_name: str
    message: str
    stream: str = "stdout"
