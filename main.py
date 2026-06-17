"""
Main entry point for the CI/CD pipeline execution engine.

Usage:
    python main.py --pipeline examples/pipeline.yaml
    python main.py --pipeline examples/pipeline.yaml --var VERSION=2.0.0
    python main.py --list-runs
    python main.py --show-run <run_id>
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from cicd_engine import (
    PipelineParser,
    PipelineScheduler,
    SubprocessEnvironment,
    ArtifactManager,
    LogStream,
    StateManager,
    ConsoleLogHandler,
    FileLogHandler,
    PipelineStatus,
)


async def run_pipeline(pipeline_path: str, variables: dict) -> None:
    """Run a pipeline from a definition file."""
    print(f"Loading pipeline from: {pipeline_path}")

    parser = PipelineParser()
    pipeline = parser.parse_file(pipeline_path)

    print(f"Pipeline: {pipeline.name}")
    print(f"Stages: {', '.join(pipeline.stages)}")
    print(f"Steps: {len(pipeline.steps)}")

    execution_levels = PipelineParser.topological_sort(pipeline)
    print(f"\nExecution levels (parallel groups):")
    for i, level in enumerate(execution_levels, 1):
        print(f"  Level {i}: {', '.join(level)}")

    base_workdir = Path("./workspace")
    base_workdir.mkdir(exist_ok=True)

    scheduler = PipelineScheduler(
        execution_env=SubprocessEnvironment(base_workdir=str(base_workdir)),
        artifact_manager=ArtifactManager(base_dir="./artifacts"),
        log_stream=LogStream(),
        state_manager=StateManager(base_dir="./runs"),
        max_parallel=4,
        base_workdir=str(base_workdir),
    )

    scheduler.log_stream.add_handler(ConsoleLogHandler(use_colors=True))
    scheduler.log_stream.add_handler(FileLogHandler(base_dir="./logs"))

    print(f"\n{'='*60}")
    print("Starting pipeline execution...")
    print(f"{'='*60}\n")

    result = await scheduler.run(pipeline, variables=variables)

    print(f"\n{'='*60}")
    print("Pipeline execution completed!")
    print(f"{'='*60}")
    print(f"Status: {result.status}")
    print(f"Duration: {result.get_duration():.2f}s")
    print(f"\nStep results:")
    for step_full_name, step_result in sorted(result.step_results.items()):
        status_icon = {
            "success": "✓",
            "failed": "✗",
            "skipped": "⊘",
            "cancelled": "⊗",
            "running": "⏳",
            "pending": "○",
        }.get(step_result.status.value, "?")
        duration = ""
        if step_result.started_at and step_result.completed_at:
            dur = (step_result.completed_at - step_result.started_at).total_seconds()
            duration = f" ({dur:.2f}s)"
        print(f"  {status_icon} {step_full_name}: {step_result.status}{duration}")

    if result.error_message:
        print(f"\nError: {result.error_message}")

    print(f"\nArtifacts stored in: ./artifacts")
    print(f"Logs stored in: ./logs")
    print(f"Results stored in: ./runs")

    sys.exit(0 if result.status == PipelineStatus.SUCCESS else 1)


def list_runs(limit: int) -> None:
    """List recent pipeline runs."""
    state_manager = StateManager(base_dir="./runs")
    runs = state_manager.list_runs(limit=limit)

    if not runs:
        print("No pipeline runs found.")
        return

    print(f"Recent pipeline runs (showing {len(runs)}):")
    print("-" * 80)
    print(f"{'Run ID':<20} {'Pipeline':<25} {'Status':<12} {'Duration':<10} {'Steps':<6}")
    print("-" * 80)

    for run in runs:
        duration = f"{run.get('duration_seconds', 0):.1f}s" if run.get('duration_seconds') else "N/A"
        steps = f"{run.get('failed_steps', 0)}/{run.get('step_count', 0)}"
        print(
            f"{run.get('run_id', 'N/A'):<20} "
            f"{run.get('pipeline_name', 'N/A'):<25} "
            f"{run.get('status', 'N/A'):<12} "
            f"{duration:<10} "
            f"{steps:<6}"
        )


def show_run(run_id: str) -> None:
    """Show details of a specific pipeline run."""
    state_manager = StateManager(base_dir="./runs")

    result = asyncio.run(state_manager.load_pipeline_result(run_id))
    if not result:
        print(f"Run not found: {run_id}")
        return

    print(f"Pipeline: {result.pipeline_name}")
    print(f"Run ID: {run_id}")
    print(f"Status: {result.status}")
    print(f"Started: {result.started_at}")
    print(f"Completed: {result.completed_at}")
    print(f"Duration: {result.get_duration():.2f}s")

    if result.error_message:
        print(f"\nError: {result.error_message}")

    print(f"\nStep results:")
    print("-" * 80)
    for step_full_name, step_result in sorted(result.step_results.items()):
        print(f"\n{step_full_name}: {step_result.status}")
        if step_result.started_at:
            print(f"  Started: {step_result.started_at}")
        if step_result.completed_at:
            print(f"  Completed: {step_result.completed_at}")
        if step_result.exit_code is not None:
            print(f"  Exit code: {step_result.exit_code}")
        if step_result.error_message:
            print(f"  Error: {step_result.error_message}")


def parse_variables(var_args: list) -> dict:
    """Parse variable arguments in the format KEY=VALUE."""
    variables = {}
    for arg in var_args:
        if "=" in arg:
            key, value = arg.split("=", 1)
            if value.lower() == "true":
                variables[key] = True
            elif value.lower() == "false":
                variables[key] = False
            elif value.isdigit():
                variables[key] = int(value)
            else:
                try:
                    variables[key] = float(value)
                except ValueError:
                    variables[key] = value
    return variables


def main():
    parser = argparse.ArgumentParser(
        description="CI/CD Pipeline Execution Engine"
    )

    parser.add_argument(
        "--pipeline",
        type=str,
        help="Path to pipeline definition file (YAML/JSON)",
    )
    parser.add_argument(
        "--var",
        action="append",
        default=[],
        help="Set variable: KEY=VALUE (can be used multiple times)",
    )
    parser.add_argument(
        "--list-runs",
        action="store_true",
        help="List recent pipeline runs",
    )
    parser.add_argument(
        "--show-run",
        type=str,
        metavar="RUN_ID",
        help="Show details of a specific run",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of runs to list (default: 20)",
    )

    args = parser.parse_args()

    if args.list_runs:
        list_runs(args.limit)
        return

    if args.show_run:
        show_run(args.show_run)
        return

    if args.pipeline:
        variables = parse_variables(args.var)
        asyncio.run(run_pipeline(args.pipeline, variables))
        return

    parser.print_help()


if __name__ == "__main__":
    main()
