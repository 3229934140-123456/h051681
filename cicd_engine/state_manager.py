"""
State Manager Module
Handles aggregation and persistence of pipeline execution state.

How it works:
1. Collects step results as they complete
2. Aggregates step statuses into overall pipeline status
3. Persists pipeline and step results to storage (JSON files by default)
4. Supports querying historical pipeline runs
5. Manages pipeline run metadata and statistics

State Aggregation Rules:
- SUCCESS: All non-skipped steps succeeded
- PARTIAL: Some steps succeeded, some failed (with continue strategy)
- FAILED: Any critical step failed (with abort strategy)
- CANCELLED: Pipeline was cancelled
- RUNNING: Pipeline is currently executing

Persistence Format:
    runs/
        {run_id}/
            result.json      # Pipeline result
            step_results/
                {stage}/
                    {step}.json  # Step result
            metadata.json    # Run metadata
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import asdict, is_dataclass

from .models import (
    PipelineResult,
    StepResult,
    PipelineStatus,
    StepStatus,
)


class StateManagerError(Exception):
    pass


class EnhancedJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and enum types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, (PipelineStatus, StepStatus)):
            return obj.value
        if hasattr(obj, "value"):
            return obj.value
        if is_dataclass(obj):
            return asdict(obj)
        return super().default(obj)


class StateManager:
    """
    Manages pipeline state persistence and retrieval.

    Features:
    - Persist pipeline results to JSON files
    - Load historical pipeline runs
    - Query pipeline status and statistics
    - Aggregate state from step results
    - Support for multiple storage backends

    Example:
        state_manager = StateManager(base_dir="./runs")

        # Save a pipeline result
        await state_manager.save_pipeline_result(run_id, result)

        # Load a specific run
        result = await state_manager.load_pipeline_result(run_id)

        # List all runs
        runs = state_manager.list_runs()

        # Get pipeline statistics
        stats = state_manager.get_pipeline_stats("my-pipeline")
    """

    def __init__(self, base_dir: str = "./runs"):
        """
        Args:
            base_dir: Base directory for storing pipeline run data
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_run_dir(self, run_id: str) -> Path:
        return self.base_dir / run_id

    def _get_pipeline_result_path(self, run_id: str) -> Path:
        return self._get_run_dir(run_id) / "result.json"

    def _get_step_results_dir(self, run_id: str) -> Path:
        return self._get_run_dir(run_id) / "step_results"

    def _get_step_result_path(
        self, run_id: str, stage_name: str, step_name: str
    ) -> Path:
        return (
            self._get_step_results_dir(run_id) / stage_name / f"{step_name}.json"
        )

    def _get_metadata_path(self, run_id: str) -> Path:
        return self._get_run_dir(run_id) / "metadata.json"

    async def save_pipeline_result(
        self, run_id: str, result: PipelineResult
    ) -> None:
        """
        Save a pipeline result to storage.

        Args:
            run_id: Unique run identifier
            result: PipelineResult to save
        """
        run_dir = self._get_run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        result_dict = self._pipeline_result_to_dict(result)
        with open(self._get_pipeline_result_path(run_id), "w", encoding="utf-8") as f:
            json.dump(result_dict, f, cls=EnhancedJSONEncoder, indent=2)

        step_results_dir = self._get_step_results_dir(run_id)
        step_results_dir.mkdir(parents=True, exist_ok=True)

        for step_full_name, step_result in result.step_results.items():
            stage_dir = step_results_dir / step_result.stage_name
            stage_dir.mkdir(parents=True, exist_ok=True)

            step_dict = self._step_result_to_dict(step_result)
            step_path = self._get_step_result_path(
                run_id, step_result.stage_name, step_result.step_name
            )
            with open(step_path, "w", encoding="utf-8") as f:
                json.dump(step_dict, f, cls=EnhancedJSONEncoder, indent=2)

        metadata = {
            "run_id": run_id,
            "pipeline_name": result.pipeline_name,
            "status": result.status.value if isinstance(result.status, PipelineStatus) else result.status,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "duration_seconds": result.get_duration(),
            "step_count": len(result.step_results),
            "failed_steps": len(result.get_failed_steps()),
        }
        with open(self._get_metadata_path(run_id), "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    async def load_pipeline_result(self, run_id: str) -> Optional[PipelineResult]:
        """
        Load a pipeline result from storage.

        Args:
            run_id: Unique run identifier

        Returns:
            PipelineResult if found, None otherwise
        """
        result_path = self._get_pipeline_result_path(run_id)
        if not result_path.exists():
            return None

        with open(result_path, "r", encoding="utf-8") as f:
            result_dict = json.load(f)

        step_results_dir = self._get_step_results_dir(run_id)
        step_results: Dict[str, StepResult] = {}

        if step_results_dir.exists():
            for stage_dir in step_results_dir.iterdir():
                if not stage_dir.is_dir():
                    continue
                for step_file in stage_dir.glob("*.json"):
                    with open(step_file, "r", encoding="utf-8") as f:
                        step_dict = json.load(f)
                    step_result = self._dict_to_step_result(step_dict)
                    step_results[step_result.get_full_name()] = step_result

        result = self._dict_to_pipeline_result(result_dict, step_results)
        return result

    def list_runs(
        self,
        pipeline_name: Optional[str] = None,
        status: Optional[PipelineStatus] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List pipeline runs with optional filtering.

        Args:
            pipeline_name: Filter by pipeline name
            status: Filter by pipeline status
            limit: Maximum number of runs to return

        Returns:
            List of run metadata dictionaries
        """
        runs: List[Dict[str, Any]] = []

        for run_dir in sorted(self.base_dir.iterdir(), reverse=True):
            if not run_dir.is_dir():
                continue

            metadata_path = run_dir / "metadata.json"
            if not metadata_path.exists():
                continue

            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

                if pipeline_name and metadata.get("pipeline_name") != pipeline_name:
                    continue

                if status:
                    status_value = status.value if isinstance(status, PipelineStatus) else status
                    if metadata.get("status") != status_value:
                        continue

                runs.append(metadata)

                if len(runs) >= limit:
                    break
            except Exception:
                continue

        return runs

    def get_pipeline_stats(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Get statistics for a pipeline.

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            Dictionary with statistics:
            - total_runs: Total number of runs
            - success_count: Number of successful runs
            - failed_count: Number of failed runs
            - partial_count: Number of partial runs
            - success_rate: Success rate percentage
            - avg_duration: Average duration in seconds
            - last_run: Last run metadata
        """
        runs = self.list_runs(pipeline_name=pipeline_name, limit=1000)

        if not runs:
            return {
                "total_runs": 0,
                "success_count": 0,
                "failed_count": 0,
                "partial_count": 0,
                "success_rate": 0.0,
                "avg_duration": 0.0,
                "last_run": None,
            }

        success_count = sum(
            1 for r in runs if r.get("status") == PipelineStatus.SUCCESS.value
        )
        failed_count = sum(
            1 for r in runs if r.get("status") == PipelineStatus.FAILED.value
        )
        partial_count = sum(
            1 for r in runs if r.get("status") == PipelineStatus.PARTIAL.value
        )

        durations = [
            r.get("duration_seconds", 0)
            for r in runs
            if r.get("duration_seconds") is not None
        ]
        avg_duration = sum(durations) / len(durations) if durations else 0.0

        return {
            "total_runs": len(runs),
            "success_count": success_count,
            "failed_count": failed_count,
            "partial_count": partial_count,
            "success_rate": (success_count / len(runs) * 100) if runs else 0.0,
            "avg_duration": avg_duration,
            "last_run": runs[0] if runs else None,
        }

    def delete_run(self, run_id: str) -> bool:
        """
        Delete all data for a pipeline run.

        Args:
            run_id: Unique run identifier

        Returns:
            True if deleted, False if not found
        """
        import shutil

        run_dir = self._get_run_dir(run_id)
        if run_dir.exists():
            shutil.rmtree(run_dir)
            return True
        return False

    def _pipeline_result_to_dict(self, result: PipelineResult) -> Dict[str, Any]:
        return {
            "pipeline_name": result.pipeline_name,
            "status": result.status.value if isinstance(result.status, PipelineStatus) else result.status,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "outputs": result.outputs,
            "error_message": result.error_message,
        }

    def _dict_to_pipeline_result(
        self, data: Dict[str, Any], step_results: Dict[str, StepResult]
    ) -> PipelineResult:
        return PipelineResult(
            pipeline_name=data.get("pipeline_name", "unknown"),
            status=PipelineStatus(data.get("status", PipelineStatus.FAILED)),
            started_at=datetime.fromisoformat(data["started_at"])
            if data.get("started_at")
            else None,
            completed_at=datetime.fromisoformat(data["completed_at"])
            if data.get("completed_at")
            else None,
            step_results=step_results,
            outputs=data.get("outputs", {}),
            error_message=data.get("error_message"),
        )

    def _step_result_to_dict(self, result: StepResult) -> Dict[str, Any]:
        return {
            "step_name": result.step_name,
            "stage_name": result.stage_name,
            "status": result.status.value if isinstance(result.status, StepStatus) else result.status,
            "exit_code": result.exit_code,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "logs": result.logs,
            "outputs": result.outputs,
            "error_message": result.error_message,
        }

    def _dict_to_step_result(self, data: Dict[str, Any]) -> StepResult:
        return StepResult(
            step_name=data.get("step_name", "unknown"),
            stage_name=data.get("stage_name", "unknown"),
            status=StepStatus(data.get("status", StepStatus.FAILED)),
            exit_code=data.get("exit_code"),
            started_at=datetime.fromisoformat(data["started_at"])
            if data.get("started_at")
            else None,
            completed_at=datetime.fromisoformat(data["completed_at"])
            if data.get("completed_at")
            else None,
            logs=data.get("logs", []),
            outputs=data.get("outputs", {}),
            error_message=data.get("error_message"),
        )
