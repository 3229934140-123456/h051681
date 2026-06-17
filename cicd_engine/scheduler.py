"""
Pipeline Scheduler Module
Orchestrates pipeline execution based on dependency graphs.

How it works:
1. Parses the pipeline to build a dependency graph (DAG)
2. Uses topological sorting to determine execution order
3. Steps with no pending dependencies are dispatched for parallel execution
4. Manages a thread/process pool for concurrent step execution
5. Handles conditional execution based on previous results
6. Implements failure strategies (abort, continue, rollback)
7. Aggregates step results into final pipeline status

Scheduling Algorithm:
    While there are steps to run:
        1. Find all steps where all dependencies are complete
        2. For each ready step:
           a. Check condition (if any)
           b. If condition passes, dispatch to execution pool
           c. If condition fails, mark as skipped
        3. Wait for at least one step to complete
        4. Process results, update dependency tracking
        5. Handle failures according to strategy
        6. Repeat until all steps complete or pipeline aborts

Condition Evaluation:
    - "success": All dependencies must have succeeded
    - "failure": Any dependency failed
    - "always": Run regardless of dependency status
    - "variable.KEY == value": Check pipeline variables
    - "step('stage/step').output.KEY == value": Check step outputs
"""

import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict

from .models import (
    Step,
    Pipeline,
    StepResult,
    PipelineResult,
    StepStatus,
    PipelineStatus,
    FailureStrategy,
    Artifact,
)
from .pipeline_parser import PipelineParser
from .execution_environment import ExecutionEnvironment, SubprocessEnvironment
from .artifact_manager import ArtifactManager
from .log_stream import LogStream


class SchedulerError(Exception):
    pass


class ConditionEvaluator:
    """
    Evaluates step conditions to determine if a step should run.

    Supported condition formats:
    1. Simple keywords:
       - "success": All dependencies succeeded
       - "failure": Any dependency failed
       - "always": Always run regardless of dependencies

    2. Variable comparisons:
       - "variable.VERSION == '1.0.0'"
       - "variable.BUILD_NUMBER > 100"
       - "variable.DEBUG == true"

    3. Step output comparisons:
       - "step('build/compile').output.exit_code == 0"
       - "step('test/unit').output.coverage >= 80"

    4. Boolean expressions:
       - "variable.ENV == 'prod' && variable.DEPLOY == true"
       - "success || variable.FORCE_DEPLOY == true"
    """

    def __init__(
        self,
        variables: Dict[str, Any],
        step_results: Dict[str, StepResult],
    ):
        self.variables = variables
        self.step_results = step_results

    def evaluate(self, condition: str, step: Step) -> bool:
        if not condition:
            return True

        condition = condition.strip()
        condition_lower = condition.lower()

        if condition_lower == "success":
            return self._check_dependencies_success(step)
        elif condition_lower == "failure":
            return self._check_dependencies_failure(step)
        elif condition_lower == "always":
            return True

        return self._evaluate_expression(condition, step)

    def _check_dependencies_success(self, step: Step) -> bool:
        for dep in step.depends_on:
            result = self.step_results.get(dep)
            if not result or result.status != StepStatus.SUCCESS:
                return False
        return True

    def _check_dependencies_failure(self, step: Step) -> bool:
        for dep in step.depends_on:
            result = self.step_results.get(dep)
            if result and result.status == StepStatus.FAILED:
                return True
        return False

    def _evaluate_expression(self, expression: str, step: Step) -> bool:
        expr = expression

        expr = expr.replace("&&", " and ").replace("||", " or ")

        expr = self._replace_variables(expr)
        expr = self._replace_step_outputs(expr)
        expr = self._replace_keywords(expr, step)

        try:
            eval_globals = {
                "__builtins__": {},
                "true": True,
                "false": False,
                "none": None,
            }
            return bool(eval(expr, eval_globals, {}))
        except Exception as e:
            raise SchedulerError(
                f"Failed to evaluate condition '{expression}': {e}"
            )

    def _replace_variables(self, expr: str) -> str:
        import re

        pattern = r"variable\.(\w+)"
        for match in re.finditer(pattern, expr, re.IGNORECASE):
            var_name = match.group(1)
            actual_var_name = None
            for key in self.variables:
                if key.lower() == var_name.lower():
                    actual_var_name = key
                    break

            if actual_var_name:
                value = self.variables[actual_var_name]
                replacement = self._format_value_for_eval(value)
                expr = expr.replace(match.group(0), replacement)
            else:
                expr = expr.replace(match.group(0), "None")
        return expr

    def _replace_step_outputs(self, expr: str) -> str:
        import re

        pattern = r"step\(['\"]([^'\"]+)['\"]\)\.output\.(\w+)"
        for match in re.finditer(pattern, expr, re.IGNORECASE):
            step_name = match.group(1)
            output_key = match.group(2)

            actual_step_name = None
            for key in self.step_results:
                if key.lower() == step_name.lower():
                    actual_step_name = key
                    break

            result = self.step_results.get(actual_step_name) if actual_step_name else None
            value = None
            found = False

            if result:
                actual_output_key = None
                for key in result.outputs:
                    if key.lower() == output_key.lower():
                        actual_output_key = key
                        break

                if actual_output_key:
                    value = result.outputs[actual_output_key]
                    found = True
                else:
                    if hasattr(result, output_key.lower()):
                        value = getattr(result, output_key.lower())
                        found = True
                    else:
                        for attr in dir(result):
                            if attr.lower() == output_key.lower():
                                value = getattr(result, attr)
                                found = True
                                break

            if found:
                replacement = self._format_value_for_eval(value)
                expr = expr.replace(match.group(0), replacement)
            else:
                expr = expr.replace(match.group(0), "None")
        return expr

    def _format_value_for_eval(self, value: Any) -> str:
        """Format a value for safe inclusion in an eval expression.

        This method properly handles strings containing quotes, spaces,
        and other special characters by using repr() for proper escaping.

        Args:
            value: The value to format

        Returns:
            A string representation of the value that can be safely used in eval()
        """
        if value is None:
            return "None"
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, str):
            return repr(value)
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return repr(str(value))

    def _replace_keywords(self, expr: str, step: Step) -> str:
        import re

        success_val = str(self._check_dependencies_success(step)).lower()
        failure_val = str(self._check_dependencies_failure(step)).lower()

        expr = re.sub(r'\bsuccess\b', success_val, expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bfailure\b', failure_val, expr, flags=re.IGNORECASE)

        return expr


class PipelineScheduler:
    """
    Main pipeline execution scheduler.

    Orchestrates the execution of pipeline steps according to their
    dependencies, conditions, and failure strategies.

    Example:
        parser = PipelineParser()
        pipeline = parser.parse_file("pipeline.yaml")

        scheduler = PipelineScheduler(
            execution_env=SubprocessEnvironment(base_workdir="./workspace"),
            artifact_manager=ArtifactManager(base_dir="./artifacts"),
            log_stream=LogStream(),
            max_parallel=4,
        )

        # Add log handlers
        scheduler.log_stream.add_handler(ConsoleLogHandler())
        scheduler.log_stream.add_handler(FileLogHandler("./logs"))

        # Run the pipeline
        result = await scheduler.run(pipeline)

        print(f"Pipeline status: {result.status}")
        print(f"Duration: {result.get_duration()}s")
        for step_name, step_result in result.step_results.items():
            print(f"  {step_name}: {step_result.status}")
    """

    def __init__(
        self,
        execution_env: Optional[ExecutionEnvironment] = None,
        artifact_manager: Optional[ArtifactManager] = None,
        log_stream: Optional[LogStream] = None,
        state_manager: Optional[Any] = None,
        max_parallel: int = 4,
        base_workdir: str = "./workspace",
    ):
        """
        Args:
            execution_env: Environment for executing steps (default: SubprocessEnvironment)
            artifact_manager: Manager for artifacts (default: ArtifactManager)
            log_stream: Log streaming service (default: LogStream)
            state_manager: State persistence manager (optional)
            max_parallel: Maximum number of steps to run in parallel
            base_workdir: Base working directory for step execution
        """
        self.execution_env = execution_env or SubprocessEnvironment(
            base_workdir=base_workdir
        )
        self.artifact_manager = artifact_manager or ArtifactManager()
        self.log_stream = log_stream or LogStream()
        self.state_manager = state_manager
        self.max_parallel = max_parallel
        self.base_workdir = Path(base_workdir)

        self._running: bool = False
        self._cancelled: bool = False

    async def run(
        self,
        pipeline: Pipeline,
        variables: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> PipelineResult:
        """
        Execute a pipeline.

        Args:
            pipeline: The pipeline to execute
            variables: Additional variables to merge into pipeline variables
            run_id: Unique run identifier (auto-generated if not provided)

        Returns:
            PipelineResult with overall status and step results
        """
        if self._running:
            raise SchedulerError("Scheduler is already running a pipeline")

        self._running = True
        self._cancelled = False

        run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
        started_at = datetime.now()

        merged_vars = {**pipeline.variables, **(variables or {})}

        result = PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.RUNNING,
            started_at=started_at,
        )

        step_results: Dict[str, StepResult] = {}
        artifacts: Dict[str, List[Artifact]] = defaultdict(list)
        completed: Set[str] = set()
        abort_pipeline = False

        try:
            await self._log_pipeline_start(pipeline, run_id, merged_vars)

            execution_levels = PipelineParser.topological_sort(pipeline)
            forward_deps, _ = PipelineParser.build_dependency_graph(pipeline)

            for level_idx, level_steps in enumerate(execution_levels):
                if abort_pipeline or self._cancelled:
                    break

                await self._log_level_start(level_idx, level_steps, run_id)

                pending = [name for name in level_steps if name not in completed]
                if not pending:
                    continue

                semaphore = asyncio.Semaphore(self.max_parallel)
                tasks = []

                for step_full_name in pending:
                    step = pipeline.get_step(step_full_name)
                    if not step:
                        continue

                    task = asyncio.create_task(
                        self._execute_step_with_semaphore(
                            semaphore=semaphore,
                            step=step,
                            pipeline=pipeline,
                            run_id=run_id,
                            merged_vars=merged_vars,
                            step_results=step_results,
                            artifacts=artifacts,
                        )
                    )
                    tasks.append((step_full_name, task))

                for step_full_name, task in tasks:
                    step_result = await task
                    step_results[step_full_name] = step_result
                    completed.add(step_full_name)

                    current_step = pipeline.get_step(step_full_name)

                    if step_result.status == StepStatus.SUCCESS:
                        step_artifacts = await self.artifact_manager.collect_artifacts(
                            step=current_step,
                            source_dir=str(
                                self.base_workdir
                                / step_result.stage_name
                                / step_result.step_name
                            ),
                            run_id=run_id,
                        )
                        artifacts[step_full_name] = step_artifacts

                    if (
                        step_result.status == StepStatus.FAILED
                        and current_step.failure_strategy == FailureStrategy.ABORT
                    ):
                        abort_pipeline = True
                        await self._log_abort(step_full_name, run_id)

            final_status = self._compute_pipeline_status(step_results, abort_pipeline)
            result.status = final_status
            result.completed_at = datetime.now()
            result.step_results = step_results

            await self._log_pipeline_end(result, run_id)

        except Exception as e:
            result.status = PipelineStatus.FAILED
            result.completed_at = datetime.now()
            result.error_message = str(e)
            await self._log_pipeline_error(e, run_id)
        finally:
            self._running = False
            self.log_stream.close_run(run_id)

            if self.state_manager:
                await self.state_manager.save_pipeline_result(run_id, result)

        return result

    async def _execute_step_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        step: Step,
        pipeline: Pipeline,
        run_id: str,
        merged_vars: Dict[str, Any],
        step_results: Dict[str, StepResult],
        artifacts: Dict[str, List[Artifact]],
    ) -> StepResult:
        async with semaphore:
            return await self._execute_step(
                step=step,
                pipeline=pipeline,
                run_id=run_id,
                merged_vars=merged_vars,
                step_results=step_results,
                artifacts=artifacts,
            )

    async def _execute_step(
        self,
        step: Step,
        pipeline: Pipeline,
        run_id: str,
        merged_vars: Dict[str, Any],
        step_results: Dict[str, StepResult],
        artifacts: Dict[str, List[Artifact]],
    ) -> StepResult:
        step_full_name = step.get_full_name()
        result = StepResult(
            step_name=step.name,
            stage_name=step.stage,
            status=StepStatus.RUNNING,
            started_at=datetime.now(),
        )

        try:
            failed_deps = []
            for dep_name in step.depends_on:
                dep_result = step_results.get(dep_name)
                if dep_result and dep_result.status == StepStatus.FAILED:
                    failed_deps.append(dep_name)

            if failed_deps:
                condition_str = step.condition or "success"
                condition_lower = condition_str.lower().strip()

                allows_failure = (
                    "failure" in condition_lower
                    or "always" in condition_lower
                )

                if not allows_failure:
                    result.status = StepStatus.SKIPPED
                    result.completed_at = datetime.now()
                    skip_reason = f"skipped due to failed dependencies: {', '.join(failed_deps)}"
                    await self._log(
                        f"{skip_reason} (condition: {condition_str})",
                        step_full_name,
                        "info",
                        run_id,
                    )
                    return result

            condition_evaluator = ConditionEvaluator(merged_vars, step_results)
            if step.condition and not condition_evaluator.evaluate(
                step.condition, step
            ):
                result.status = StepStatus.SKIPPED
                result.completed_at = datetime.now()
                await self._log_step_skip(step_full_name, step.condition, run_id)
                return result

            await self._log_step_start(step_full_name, run_id)

            await self.artifact_manager.prepare_inputs(
                step=step,
                target_dir=str(self.base_workdir / step.stage / step.name),
                run_id=run_id,
                available_artifacts=dict(artifacts),
            )

            merged_env = {**pipeline.environment, **step.environment}
            merged_env = {
                k: self._substitute_variables(v, merged_vars)
                for k, v in merged_env.items()
            }

            capturer = self.log_stream.create_capturer(
                step_name=step.name,
                stage_name=step.stage,
                run_id=run_id,
            )

            substituted_commands = [
                self._substitute_variables(cmd, merged_vars)
                for cmd in step.commands
            ]

            import copy
            step_for_execution = copy.copy(step)
            step_for_execution.commands = substituted_commands

            exit_code, stdout, stderr = await self.execution_env.execute(
                step=step_for_execution,
                env=merged_env,
                log_callback=capturer.callback,
                timeout=step.timeout,
            )

            result.exit_code = exit_code
            result.logs = capturer.get_log_lines()
            result.outputs = {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
            }

            if exit_code == 0:
                result.status = StepStatus.SUCCESS
            else:
                result.status = StepStatus.FAILED
                result.error_message = f"Command failed with exit code {exit_code}"

        except Exception as e:
            result.status = StepStatus.FAILED
            result.error_message = str(e)
            await self._log_step_error(step_full_name, str(e), run_id)
        finally:
            if result.status == StepStatus.RUNNING:
                result.status = StepStatus.CANCELLED
            result.completed_at = datetime.now()

        await self._log_step_end(step_full_name, result.status, run_id)
        return result

    def _substitute_variables(
        self, value: str, variables: Dict[str, Any]
    ) -> str:
        """Substitute ${VAR_NAME} and ${{VAR_NAME}} placeholders with variable values."""
        import re

        if not isinstance(value, str):
            return str(value)

        def replace(match):
            var_name = match.group(1)
            return str(variables.get(var_name, match.group(0)))

        value = re.sub(r"\$\{\{(\w+)\}\}", replace, value)
        value = re.sub(r"\$\{(\w+)\}", replace, value)
        return value

    def _compute_pipeline_status(
        self,
        step_results: Dict[str, StepResult],
        aborted: bool,
    ) -> PipelineStatus:
        if not step_results:
            return PipelineStatus.SUCCESS

        all_results = list(step_results.values())
        non_skipped = [r for r in all_results if r.status != StepStatus.SKIPPED]

        if not non_skipped:
            return PipelineStatus.SUCCESS

        has_failed = any(r.status == StepStatus.FAILED for r in non_skipped)
        has_success = any(r.status == StepStatus.SUCCESS for r in non_skipped)

        if aborted and has_failed:
            return PipelineStatus.FAILED
        if has_failed and has_success:
            return PipelineStatus.PARTIAL
        if has_failed:
            return PipelineStatus.FAILED
        if all(r.status == StepStatus.SUCCESS for r in non_skipped):
            return PipelineStatus.SUCCESS
        if any(r.status == StepStatus.CANCELLED for r in non_skipped):
            return PipelineStatus.CANCELLED

        return PipelineStatus.RUNNING

    async def cancel(self) -> None:
        """Cancel the currently running pipeline."""
        if self._running:
            self._cancelled = True
            await self._log("Pipeline cancellation requested", "system", "info")

    async def _log_pipeline_start(
        self, pipeline: Pipeline, run_id: str, variables: Dict[str, Any]
    ) -> None:
        await self._log(
            f"Starting pipeline: {pipeline.name}",
            "system",
            "info",
            run_id,
        )
        await self._log(
            f"Stages: {', '.join(pipeline.stages)}",
            "system",
            "info",
            run_id,
        )
        await self._log(
            f"Variables: {variables}",
            "system",
            "info",
            run_id,
        )

    async def _log_pipeline_end(
        self, result: PipelineResult, run_id: str
    ) -> None:
        await self._log(
            f"Pipeline completed with status: {result.status}",
            "system",
            "info",
            run_id,
        )
        if result.get_duration():
            await self._log(
                f"Duration: {result.get_duration():.2f}s",
                "system",
                "info",
                run_id,
            )

    async def _log_pipeline_error(self, error: Exception, run_id: str) -> None:
        await self._log(
            f"Pipeline error: {error}",
            "system",
            "error",
            run_id,
        )

    async def _log_level_start(
        self, level_idx: int, steps: List[str], run_id: str
    ) -> None:
        await self._log(
            f"Starting execution level {level_idx + 1}: "
            f"{', '.join(steps)}",
            "system",
            "info",
            run_id,
        )

    async def _log_step_start(self, step_full_name: str, run_id: str) -> None:
        stage, step = step_full_name.split("/", 1)
        await self._log(
            f"Starting step: {step_full_name}",
            step,
            "info",
            run_id,
            stage_name=stage,
        )

    async def _log_step_end(
        self, step_full_name: str, status: StepStatus, run_id: str
    ) -> None:
        stage, step = step_full_name.split("/", 1)
        await self._log(
            f"Step completed: {step_full_name} - {status}",
            step,
            "info",
            run_id,
            stage_name=stage,
        )

    async def _log_step_skip(
        self, step_full_name: str, condition: str, run_id: str
    ) -> None:
        stage, step = step_full_name.split("/", 1)
        await self._log(
            f"Step skipped: {step_full_name} (condition: {condition})",
            step,
            "info",
            run_id,
            stage_name=stage,
        )

    async def _log_step_error(
        self, step_full_name: str, error: str, run_id: str
    ) -> None:
        stage, step = step_full_name.split("/", 1)
        await self._log(
            f"Step error: {step_full_name} - {error}",
            step,
            "error",
            run_id,
            stage_name=stage,
        )

    async def _log_abort(self, failed_step: str, run_id: str) -> None:
        await self._log(
            f"Aborting pipeline due to failure in: {failed_step}",
            "system",
            "error",
            run_id,
        )

    async def _log(
        self,
        message: str,
        step_name: str,
        stream: str,
        run_id: str,
        stage_name: str = "system",
    ) -> None:
        await self.log_stream.log(
            message=message,
            step_name=step_name,
            stage_name=stage_name,
            run_id=run_id,
            stream=stream,
        )
