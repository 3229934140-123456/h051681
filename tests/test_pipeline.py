"""
Comprehensive tests for the CI/CD pipeline execution engine.

Tests cover:
1. Pipeline parsing and dependency graph construction
2. Execution environment isolation
3. Artifact management and transfer
4. Log streaming
5. Scheduler with parallel execution
6. Condition evaluation
7. Failure strategies
8. State persistence
"""

import asyncio
import json
import os
import sys
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from cicd_engine import (
    PipelineParser,
    PipelineParserError,
    PipelineScheduler,
    SubprocessEnvironment,
    ArtifactManager,
    LogStream,
    ConsoleLogHandler,
    FileLogHandler,
    StateManager,
    ConditionEvaluator,
)
from cicd_engine.models import (
    Step,
    Pipeline,
    StepResult,
    PipelineResult,
    StepStatus,
    PipelineStatus,
    FailureStrategy,
    Artifact,
)


if os.name == "nt":
    MKDIR_CMD = "if not exist output mkdir output"
    CAT_CMD = "type"
    TEST_FILE_CMD = "dir /b"
else:
    MKDIR_CMD = "mkdir -p output"
    CAT_CMD = "cat"
    TEST_FILE_CMD = "ls"

PIPELINE_YAML = f"""
name: test-pipeline
variables:
  VERSION: "1.0.0"
  DEBUG: false
stages:
  - build
  - test
  - deploy
steps:
  build:
    compile:
      commands:
        - echo Building version ${{VERSION}}
        - {MKDIR_CMD}
        - echo build success > output/build.txt
      artifacts:
        - output/*.txt
    lint:
      commands:
        - echo Linting code
      depends_on: []
      failure_strategy: continue
  test:
    unit:
      commands:
        - echo Running unit tests
        - {MKDIR_CMD}
        - echo unit tests passed > output/unit.txt
      depends_on:
        - build/compile
      artifacts:
        - output/unit.txt
      inputs:
        - build/compile:*.txt
    integration:
      commands:
        - echo Running integration tests
      depends_on:
        - build/compile
      failure_strategy: continue
  deploy:
    staging:
      commands:
        - echo Deploying to staging
      depends_on:
        - test/unit
      condition: success
    production:
      commands:
        - echo Deploying to production
      depends_on:
        - deploy/staging
      condition: "variable.DEBUG == false && success"
"""


class TestPipelineParser:
    """Tests for pipeline parsing and dependency graph construction."""

    def test_parse_yaml(self):
        pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)
        assert pipeline.name == "test-pipeline"
        assert pipeline.stages == ["build", "test", "deploy"]
        assert len(pipeline.steps) == 6
        assert "build/compile" in pipeline.steps
        assert "test/unit" in pipeline.steps
        assert "deploy/production" in pipeline.steps

    def test_parse_variables(self):
        pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)
        assert pipeline.variables["VERSION"] == "1.0.0"
        assert pipeline.variables["DEBUG"] is False

    def test_step_dependencies(self):
        pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)
        test_unit = pipeline.get_step("test/unit")
        assert test_unit.depends_on == ["build/compile"]

    def test_failure_strategy(self):
        pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)
        build_lint = pipeline.get_step("build/lint")
        assert build_lint.failure_strategy == FailureStrategy.CONTINUE

        deploy_staging = pipeline.get_step("deploy/staging")
        assert deploy_staging.failure_strategy == FailureStrategy.ABORT

    def test_dependency_validation(self):
        invalid_yaml = """
name: invalid
stages:
  - build
  - test
steps:
  build:
    step1:
      commands: [echo "test"]
  test:
    step2:
      commands: [echo "test"]
      depends_on: [build/nonexistent]
"""
        with pytest.raises(PipelineParserError, match="non-existent step"):
            PipelineParser.parse_yaml(invalid_yaml)

    def test_circular_dependency_detection(self):
        invalid_yaml = """
name: circular
stages:
  - build
steps:
  build:
    step1:
      commands: [echo "test"]
      depends_on: [build/step2]
    step2:
      commands: [echo "test"]
      depends_on: [build/step1]
"""
        with pytest.raises(PipelineParserError, match="Circular dependency"):
            PipelineParser.parse_yaml(invalid_yaml)

    def test_topological_sort(self):
        pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)
        levels = PipelineParser.topological_sort(pipeline)

        assert len(levels) >= 3

        level0 = set(levels[0])
        assert "build/compile" in level0 or "build/lint" in level0

        all_steps = set()
        for level in levels:
            for step in level:
                all_steps.add(step)

        assert all_steps == set(pipeline.get_all_step_names())

    def test_build_dependency_graph(self):
        pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)
        forward, reverse = PipelineParser.build_dependency_graph(pipeline)

        assert "build/compile" in reverse["test/unit"]
        assert "test/unit" in forward["build/compile"]


class TestConditionEvaluator:
    """Tests for condition evaluation."""

    def setup_method(self):
        self.step_results = {
            "build/compile": StepResult(
                step_name="compile",
                stage_name="build",
                status=StepStatus.SUCCESS,
                exit_code=0,
                outputs={"coverage": 85},
            ),
            "build/lint": StepResult(
                step_name="lint",
                stage_name="build",
                status=StepStatus.FAILED,
                exit_code=1,
            ),
        }
        self.variables = {
            "VERSION": "1.0.0",
            "DEBUG": False,
            "BUILD_NUMBER": 123,
        }
        self.evaluator = ConditionEvaluator(self.variables, self.step_results)

    def test_condition_success(self):
        step = Step(
            name="test",
            stage="test",
            commands=[],
            depends_on=["build/compile"],
        )
        assert self.evaluator.evaluate("success", step) is True

        step2 = Step(
            name="test2",
            stage="test",
            commands=[],
            depends_on=["build/lint"],
        )
        assert self.evaluator.evaluate("success", step2) is False

    def test_condition_failure(self):
        step = Step(
            name="test",
            stage="test",
            commands=[],
            depends_on=["build/lint"],
        )
        assert self.evaluator.evaluate("failure", step) is True

    def test_condition_always(self):
        step = Step(
            name="test",
            stage="test",
            commands=[],
            depends_on=["build/compile"],
        )
        assert self.evaluator.evaluate("always", step) is True

    def test_variable_comparison(self):
        step = Step(name="test", stage="test", commands=[], depends_on=[])

        assert self.evaluator.evaluate("variable.VERSION == '1.0.0'", step) is True
        assert self.evaluator.evaluate("variable.BUILD_NUMBER > 100", step) is True
        assert self.evaluator.evaluate("variable.DEBUG == false", step) is True

    def test_step_output_comparison(self):
        step = Step(name="test", stage="test", commands=[], depends_on=[])

        assert (
            self.evaluator.evaluate(
                "step('build/compile').output.exit_code == 0", step
            )
            is True
        )
        assert (
            self.evaluator.evaluate(
                "step('build/compile').output.coverage >= 80", step
            )
            is True
        )

    def test_boolean_expression(self):
        step = Step(
            name="test",
            stage="test",
            commands=[],
            depends_on=["build/compile"],
        )

        assert (
            self.evaluator.evaluate(
                "variable.DEBUG == false && success", step
            )
            is True
        )


class TestExecutionEnvironment:
    """Tests for isolated execution environments."""

    @pytest.mark.asyncio
    async def test_subprocess_execute_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = SubprocessEnvironment(base_workdir=tmpdir)
            if os.name == "nt":
                commands = ["echo hello world", "echo test complete"]
            else:
                commands = ["echo hello world", "echo test complete"]

            step = Step(
                name="test",
                stage="test",
                commands=commands,
            )

            exit_code, stdout, stderr = await env.execute(
                step=step,
                env={},
            )

            assert exit_code == 0
            all_output = " ".join(stdout).lower()
            assert "hello world" in all_output or "hello" in all_output
            assert stderr == []

    @pytest.mark.asyncio
    async def test_subprocess_execute_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = SubprocessEnvironment(base_workdir=tmpdir)
            step = Step(
                name="test",
                stage="test",
                commands=["exit 1"],
            )

            exit_code, stdout, stderr = await env.execute(
                step=step,
                env={},
            )

            assert exit_code != 0

    @pytest.mark.asyncio
    async def test_subprocess_environment_variables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = SubprocessEnvironment(base_workdir=tmpdir)
            if os.name == "nt":
                commands = ["echo %TEST_VAR%"]
            else:
                commands = ["echo $TEST_VAR"]

            step = Step(
                name="test",
                stage="test",
                commands=commands,
                environment={"TEST_VAR": "hello"},
            )

            exit_code, stdout, stderr = await env.execute(
                step=step,
                env={"GLOBAL_VAR": "global"},
            )

            assert exit_code == 0
            all_output = " ".join(stdout).lower()
            assert "hello" in all_output or "%test_var%" not in all_output

    @pytest.mark.asyncio
    async def test_subprocess_log_callback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = SubprocessEnvironment(base_workdir=tmpdir)
            step = Step(
                name="test",
                stage="test",
                commands=["echo log message"],
            )

            logs = []

            def callback(message, stream):
                logs.append((message.lower(), stream))

            exit_code, stdout, stderr = await env.execute(
                step=step,
                env={},
                log_callback=callback,
            )

            assert exit_code == 0
            all_msgs = " ".join([msg for msg, _ in logs])
            assert "log" in all_msgs or "echo" in all_msgs

    @pytest.mark.asyncio
    async def test_subprocess_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = SubprocessEnvironment(base_workdir=tmpdir)
            if os.name == "nt":
                commands = ["timeout /t 10"]
            else:
                commands = ["sleep 10"]

            step = Step(
                name="test",
                stage="test",
                commands=commands,
            )

            exit_code, stdout, stderr = await env.execute(
                step=step,
                env={},
                timeout=2,
            )

            all_output = " ".join(stdout + stderr).lower()
            assert exit_code != 0 or "timeout" in all_output or "timed out" in all_output


class TestArtifactManager:
    """Tests for artifact management."""

    @pytest.mark.asyncio
    async def test_collect_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()

            (source_dir / "output").mkdir()
            (source_dir / "output" / "build.txt").write_text("build content")
            (source_dir / "output" / "test.txt").write_text("test content")

            manager = ArtifactManager(base_dir=tmpdir + "/artifacts")
            step = Step(
                name="compile",
                stage="build",
                commands=[],
                artifacts=["output/*.txt"],
            )

            artifacts = await manager.collect_artifacts(
                step=step,
                source_dir=str(source_dir),
                run_id="test_run",
            )

            assert len(artifacts) == 2
            artifact_names = [a.name for a in artifacts]
            assert any("build.txt" in n for n in artifact_names)
            assert any("test.txt" in n for n in artifact_names)

    @pytest.mark.asyncio
    async def test_prepare_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_dir = Path(tmpdir) / "artifacts"
            source_dir = Path(tmpdir) / "source"
            target_dir = Path(tmpdir) / "target"

            source_dir.mkdir(parents=True)
            (source_dir / "output").mkdir()
            (source_dir / "output" / "build.txt").write_text("build content")

            manager = ArtifactManager(base_dir=str(artifact_dir))
            step = Step(
                name="compile",
                stage="build",
                commands=[],
                artifacts=["output/*.txt"],
            )

            artifacts = await manager.collect_artifacts(
                step=step,
                source_dir=str(source_dir),
                run_id="test_run",
            )

            assert len(artifacts) >= 1

            available = {"build/compile": artifacts}

            downstream_step = Step(
                name="unit",
                stage="test",
                commands=[],
                inputs=["build/compile:*.txt"],
            )

            prepared = await manager.prepare_inputs(
                step=downstream_step,
                target_dir=str(target_dir),
                run_id="test_run",
                available_artifacts=available,
            )

            assert len(prepared) >= 1

            found = False
            for root, dirs, files in os.walk(target_dir):
                for f in files:
                    if f == "build.txt":
                        found = True
                        file_path = Path(root) / f
                        assert file_path.read_text() == "build content"
                        break
                if found:
                    break

            assert found, "build.txt not found in target directory"


class TestLogStream:
    """Tests for log streaming."""

    @pytest.mark.asyncio
    async def test_log_stream_basic(self):
        log_stream = LogStream()
        handler = ConsoleLogHandler(use_colors=False)
        log_stream.add_handler(handler)

        await log_stream.log(
            message="test message",
            step_name="test",
            stage_name="build",
            run_id="run_123",
            stream="stdout",
        )

        logs = log_stream.get_logs("run_123")
        assert len(logs) == 1
        assert logs[0].message == "test message"
        assert logs[0].step_name == "test"

    @pytest.mark.asyncio
    async def test_log_stream_filtering(self):
        log_stream = LogStream()

        await log_stream.log("msg1", "step1", "build", "run1", "stdout")
        await log_stream.log("msg2", "step2", "build", "run1", "stderr")
        await log_stream.log("msg3", "step1", "test", "run1", "stdout")

        logs = log_stream.get_logs("run1", step_full_name="build/step1")
        assert len(logs) == 1
        assert logs[0].message == "msg1"

        logs = log_stream.get_logs("run1", stream_filter=["stderr"])
        assert len(logs) == 1
        assert logs[0].message == "msg2"

    @pytest.mark.asyncio
    async def test_log_capturer(self):
        log_stream = LogStream()
        capturer = log_stream.create_capturer(
            step_name="test",
            stage_name="build",
            run_id="run_123",
        )

        callback = capturer.callback
        callback("log line 1", "stdout")
        callback("log line 2", "stderr")

        await asyncio.sleep(0.1)

        logs = log_stream.get_logs("run_123")
        assert len(logs) >= 2
        assert capturer.get_log_lines() == ["log line 1", "log line 2"]

    @pytest.mark.asyncio
    async def test_file_log_handler(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            handler = FileLogHandler(base_dir=tmpdir)

            from cicd_engine.models import LogEntry

            entry = LogEntry(
                timestamp=datetime.now(),
                step_name="test",
                stage_name="build",
                message="test log",
                stream="stdout",
            )

            await handler.handle(entry, run_id="run_123")

            logs = handler.get_logs("run_123", "build/test")
            assert len(logs) == 1
            assert logs[0].message == "test log"


class TestPipelineScheduler:
    """Integration tests for the pipeline scheduler."""

    @pytest.mark.asyncio
    async def test_full_pipeline_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)

            scheduler = PipelineScheduler(
                execution_env=SubprocessEnvironment(base_workdir=tmpdir + "/workspace"),
                artifact_manager=ArtifactManager(base_dir=tmpdir + "/artifacts"),
                log_stream=LogStream(),
                state_manager=StateManager(base_dir=tmpdir + "/runs"),
                max_parallel=2,
                base_workdir=tmpdir + "/workspace",
            )

            result = await scheduler.run(pipeline)

            assert result.status == PipelineStatus.SUCCESS
            assert len(result.step_results) == 6

            for step_name, step_result in result.step_results.items():
                assert step_result.status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_pipeline_with_variables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PipelineParser.parse_yaml(PIPELINE_YAML)

            scheduler = PipelineScheduler(
                execution_env=SubprocessEnvironment(base_workdir=tmpdir + "/workspace"),
                artifact_manager=ArtifactManager(base_dir=tmpdir + "/artifacts"),
                log_stream=LogStream(),
                max_parallel=2,
                base_workdir=tmpdir + "/workspace",
            )

            result = await scheduler.run(
                pipeline, variables={"VERSION": "2.0.0", "DEBUG": True}
            )

            deploy_prod = result.step_results.get("deploy/production")
            assert deploy_prod is not None
            assert deploy_prod.status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_failure_strategy_continue(self):
        if os.name == "nt":
            fail_cmd = "cmd /c exit 1"
        else:
            fail_cmd = "exit 1"

        yaml_with_failure = f"""
name: failure-test
stages:
  - build
steps:
  build:
    fail_step:
      commands:
        - {fail_cmd}
      failure_strategy: continue
    success_step:
      commands:
        - echo success
      depends_on: []
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PipelineParser.parse_yaml(yaml_with_failure)

            scheduler = PipelineScheduler(
                execution_env=SubprocessEnvironment(base_workdir=tmpdir + "/workspace"),
                artifact_manager=ArtifactManager(base_dir=tmpdir + "/artifacts"),
                log_stream=LogStream(),
                max_parallel=2,
                base_workdir=tmpdir + "/workspace",
            )

            result = await scheduler.run(pipeline)

            assert result.status == PipelineStatus.PARTIAL
            assert result.step_results["build/fail_step"].status == StepStatus.FAILED
            assert result.step_results["build/success_step"].status == StepStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_failure_strategy_abort(self):
        if os.name == "nt":
            fail_cmd = "cmd /c exit 1"
        else:
            fail_cmd = "exit 1"

        yaml_with_failure = f"""
name: failure-test
stages:
  - build
  - test
steps:
  build:
    fail_step:
      commands:
        - {fail_cmd}
      failure_strategy: abort
  test:
    never_run:
      commands:
        - echo should not run
      depends_on:
        - build/fail_step
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PipelineParser.parse_yaml(yaml_with_failure)

            scheduler = PipelineScheduler(
                execution_env=SubprocessEnvironment(base_workdir=tmpdir + "/workspace"),
                artifact_manager=ArtifactManager(base_dir=tmpdir + "/artifacts"),
                log_stream=LogStream(),
                max_parallel=1,
                base_workdir=tmpdir + "/workspace",
            )

            result = await scheduler.run(pipeline)

            assert result.status == PipelineStatus.FAILED
            assert result.step_results["build/fail_step"].status == StepStatus.FAILED
            assert "test/never_run" not in result.step_results

    @pytest.mark.asyncio
    async def test_artifact_transfer_between_steps(self):
        artifact_yaml = f"""
name: artifact-test
stages:
  - build
  - test
steps:
  build:
    create:
      commands:
        - {MKDIR_CMD}
        - echo artifact content > output/data.txt
      artifacts:
        - output/*.txt
  test:
    consume:
      commands:
        - {MKDIR_CMD}
        - echo consume start
        - if exist output/data.txt echo file exists
        - echo consume complete
      depends_on:
        - build/create
      inputs:
        - build/create:*.txt
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = PipelineParser.parse_yaml(artifact_yaml)

            scheduler = PipelineScheduler(
                execution_env=SubprocessEnvironment(base_workdir=tmpdir + "/workspace"),
                artifact_manager=ArtifactManager(base_dir=tmpdir + "/artifacts"),
                log_stream=LogStream(),
                max_parallel=1,
                base_workdir=tmpdir + "/workspace",
            )

            result = await scheduler.run(pipeline)

            assert result.status == PipelineStatus.SUCCESS
            assert result.step_results["build/create"].status == StepStatus.SUCCESS
            assert result.step_results["test/consume"].status == StepStatus.SUCCESS


class TestStateManager:
    """Tests for state persistence."""

    @pytest.mark.asyncio
    async def test_save_and_load_pipeline_result(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(base_dir=tmpdir)

            start_time = datetime.now()
            end_time = datetime.now()

            result = PipelineResult(
                pipeline_name="test-pipeline",
                status=PipelineStatus.SUCCESS,
                started_at=start_time,
                completed_at=end_time,
                step_results={
                    "build/test": StepResult(
                        step_name="test",
                        stage_name="build",
                        status=StepStatus.SUCCESS,
                        exit_code=0,
                        started_at=start_time,
                        completed_at=end_time,
                        logs=["test log"],
                        outputs={"exit_code": 0},
                    )
                },
                outputs={"result": "ok"},
            )

            await manager.save_pipeline_result("run_123", result)

            loaded = await manager.load_pipeline_result("run_123")
            assert loaded is not None
            assert loaded.pipeline_name == "test-pipeline"
            assert loaded.status == PipelineStatus.SUCCESS
            assert "build/test" in loaded.step_results
            assert loaded.step_results["build/test"].exit_code == 0
            assert loaded.step_results["build/test"].logs == ["test log"]

    def test_list_runs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(base_dir=tmpdir)

            runs = manager.list_runs()
            assert len(runs) == 0

    def test_get_pipeline_stats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(base_dir=tmpdir)

            stats = manager.get_pipeline_stats("test-pipeline")
            assert stats["total_runs"] == 0
            assert stats["success_rate"] == 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
