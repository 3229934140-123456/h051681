"""
Pipeline Parser Module
Parses pipeline definitions from YAML/JSON and constructs dependency graphs.

How it works:
1. Pipeline definition consists of stages, each containing steps
2. Each step can declare dependencies on other steps (depends_on)
3. The parser builds a directed acyclic graph (DAG) of step dependencies
4. Steps without dependencies can run in parallel
5. Cross-stage dependencies are supported via full step names (stage/step)
"""

import yaml
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

from .models import Step, Pipeline, FailureStrategy


class PipelineParserError(Exception):
    pass


class PipelineParser:
    """
    Parses pipeline definitions and builds dependency graphs.

    Pipeline Definition Structure:
    ```yaml
    name: my-pipeline
    variables:
      VERSION: "1.0.0"
    environment:
      APP_ENV: production
    stages:
      - build
      - test
      - deploy
    steps:
      build:
        compile:
          commands:
            - npm install
            - npm run build
          artifacts:
            - dist/**/*
        lint:
          commands:
            - npm run lint
          depends_on: []  # Can run in parallel with compile

      test:
        unit:
          commands:
            - npm run test:unit
          depends_on:
            - build/compile
        integration:
          commands:
            - npm run test:integration
          depends_on:
            - build/compile
          failure_strategy: continue  # Don't abort pipeline if this fails

      deploy:
        production:
          commands:
            - npm run deploy
          depends_on:
            - test/unit
            - test/integration
          condition: "success"  # Only run if all dependencies succeeded
    ```

    Dependency Graph Construction:
    - Nodes: Steps (identified by "stage/step_name")
    - Edges: depends_on relationships
    - Parallel execution: Steps with no pending dependencies run concurrently
    """

    @classmethod
    def parse_file(cls, file_path: str) -> Pipeline:
        path = Path(file_path)
        if not path.exists():
            raise PipelineParserError(f"Pipeline file not found: {file_path}")

        content = path.read_text(encoding="utf-8")
        if path.suffix in (".yaml", ".yml"):
            return cls.parse_yaml(content)
        elif path.suffix == ".json":
            return cls.parse_json(content)
        else:
            raise PipelineParserError(
                f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json"
            )

    @classmethod
    def parse_yaml(cls, yaml_content: str) -> Pipeline:
        try:
            data = yaml.safe_load(yaml_content)
            return cls._parse_dict(data)
        except yaml.YAMLError as e:
            raise PipelineParserError(f"YAML parsing error: {e}")

    @classmethod
    def parse_json(cls, json_content: str) -> Pipeline:
        try:
            data = json.loads(json_content)
            return cls._parse_dict(data)
        except json.JSONDecodeError as e:
            raise PipelineParserError(f"JSON parsing error: {e}")

    @classmethod
    def _parse_dict(cls, data: Dict[str, Any]) -> Pipeline:
        if not data:
            raise PipelineParserError("Empty pipeline definition")

        name = data.get("name", "unnamed-pipeline")
        stages = data.get("stages", [])
        if not stages:
            raise PipelineParserError("Pipeline must define at least one stage")

        variables = data.get("variables", {})
        environment = data.get("environment", {})
        steps_data = data.get("steps", {})

        steps: Dict[str, Step] = {}

        for stage_name in stages:
            if stage_name not in steps_data:
                raise PipelineParserError(
                    f"Stage '{stage_name}' defined but has no steps"
                )

            stage_steps = steps_data[stage_name]
            if not isinstance(stage_steps, dict):
                raise PipelineParserError(
                    f"Stage '{stage_name}' steps must be a dictionary"
                )

            for step_name, step_config in stage_steps.items():
                step = cls._parse_step(
                    stage_name, step_name, step_config, stages
                )
                steps[step.get_full_name()] = step

        cls._validate_dependencies(steps, stages)
        cls._detect_cycles(steps)

        return Pipeline(
            name=name,
            stages=stages,
            steps=steps,
            variables=variables,
            environment=environment,
        )

    @classmethod
    def _parse_step(
        cls,
        stage_name: str,
        step_name: str,
        config: Dict[str, Any],
        stages: List[str],
    ) -> Step:
        if "commands" not in config:
            raise PipelineParserError(
                f"Step '{stage_name}/{step_name}' missing 'commands'"
            )

        commands = config["commands"]
        if not isinstance(commands, list):
            commands = [commands]

        cls._validate_commands(commands, stage_name, step_name)

        depends_on_raw = config.get("depends_on", [])
        if not isinstance(depends_on_raw, list):
            depends_on_raw = [depends_on_raw]

        depends_on = cls._normalize_dependencies(
            depends_on_raw, stage_name, stages
        )

        failure_strategy_str = config.get("failure_strategy", "abort")
        try:
            failure_strategy = FailureStrategy(failure_strategy_str)
        except ValueError:
            raise PipelineParserError(
                f"Invalid failure_strategy '{failure_strategy_str}' "
                f"in step '{stage_name}/{step_name}'. "
                f"Use: abort, continue, rollback"
            )

        return Step(
            name=step_name,
            stage=stage_name,
            commands=commands,
            depends_on=depends_on,
            environment=config.get("environment", {}),
            artifacts=config.get("artifacts", []),
            inputs=config.get("inputs", []),
            condition=config.get("condition"),
            failure_strategy=failure_strategy,
            timeout=config.get("timeout", 3600),
            working_dir=config.get("working_dir"),
        )

    @classmethod
    def _normalize_dependencies(
        cls,
        dependencies: List[str],
        current_stage: str,
        all_stages: List[str],
    ) -> List[str]:
        normalized = []
        for dep in dependencies:
            if "/" in dep:
                normalized.append(dep)
            else:
                normalized.append(f"{current_stage}/{dep}")
        return normalized

    @classmethod
    def _validate_dependencies(
        cls, steps: Dict[str, Step], stages: List[str]
    ) -> None:
        all_step_names = set(steps.keys())

        for step_full_name, step in steps.items():
            for dep in step.depends_on:
                if dep not in all_step_names:
                    raise PipelineParserError(
                        f"Step '{step_full_name}' depends on "
                        f"non-existent step '{dep}'"
                    )

                dep_stage = dep.split("/")[0]
                dep_stage_idx = stages.index(dep_stage)
                current_stage_idx = stages.index(step.stage)
                if dep_stage_idx > current_stage_idx:
                    raise PipelineParserError(
                        f"Step '{step_full_name}' cannot depend on "
                        f"'{dep}' which is in a later stage"
                    )

    @classmethod
    def _validate_commands(
        cls, commands: List[Any], stage_name: str, step_name: str
    ) -> None:
        """Validate that all commands are strings.

        This catches common YAML parsing issues where commands containing
        colons are incorrectly parsed as dictionaries.

        Args:
            commands: List of commands to validate
            stage_name: Name of the stage containing the step
            step_name: Name of the step containing the commands

        Raises:
            PipelineParserError: If any command is not a string
        """
        for i, cmd in enumerate(commands):
            if not isinstance(cmd, str):
                if isinstance(cmd, dict):
                    cmd_repr = str(cmd)
                    example_fix = None
                    for k, v in cmd.items():
                        example_fix = f'"{k}: {v}"'
                        break
                    raise PipelineParserError(
                        f"Invalid command in step '{stage_name}/{step_name}' at index {i}:\n"
                        f"  Command was parsed as a dictionary: {cmd_repr}\n"
                        f"  This usually happens when a command contains a colon ':' without quotes.\n"
                        f"  To fix this, wrap the command in quotes, e.g.: {example_fix}"
                    )
                else:
                    raise PipelineParserError(
                        f"Invalid command in step '{stage_name}/{step_name}' at index {i}:\n"
                        f"  Expected string, got {type(cmd).__name__}: {cmd!r}\n"
                        f"  Commands must be strings."
                    )

    @classmethod
    def _detect_cycles(cls, steps: Dict[str, Step]) -> None:
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {name: WHITE for name in steps}

        def dfs(node: str, path: List[str]) -> None:
            color[node] = GRAY
            path.append(node)

            for dep in steps[node].depends_on:
                if color[dep] == GRAY:
                    cycle_path = path[path.index(dep):] + [dep]
                    raise PipelineParserError(
                        f"Circular dependency detected: "
                        f"{' -> '.join(cycle_path)}"
                    )
                if color[dep] == WHITE:
                    dfs(dep, path)

            color[node] = BLACK
            path.pop()

        for step_name in steps:
            if color[step_name] == WHITE:
                dfs(step_name, [])

    @classmethod
    def build_dependency_graph(
        cls, pipeline: Pipeline
    ) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        Build forward and reverse dependency graphs.

        Returns:
            (forward_deps, reverse_deps):
            - forward_deps: step -> set of steps that depend on it
            - reverse_deps: step -> set of steps it depends on
        """
        forward: Dict[str, Set[str]] = defaultdict(set)
        reverse: Dict[str, Set[str]] = defaultdict(set)

        for step_name in pipeline.get_all_step_names():
            step = pipeline.get_step(step_name)
            reverse[step_name] = set(step.depends_on)
            for dep in step.depends_on:
                forward[dep].add(step_name)

        return dict(forward), dict(reverse)

    @classmethod
    def topological_sort(
        cls, pipeline: Pipeline
    ) -> List[List[str]]:
        """
        Returns steps grouped by execution level.
        Each level contains steps that can run in parallel.

        Example:
            [
                ["build/compile", "build/lint"],  # Level 0: parallel
                ["test/unit", "test/integration"],  # Level 1: parallel
                ["deploy/production"]  # Level 2
            ]
        """
        forward, reverse = cls.build_dependency_graph(pipeline)
        in_degree = {
            name: len(reverse.get(name, set()))
            for name in pipeline.get_all_step_names()
        }

        levels = []
        visited = set()

        while len(visited) < len(pipeline.steps):
            current_level = [
                name
                for name in pipeline.get_all_step_names()
                if name not in visited and in_degree[name] == 0
            ]

            if not current_level:
                raise PipelineParserError(
                    "Cannot schedule steps: possible cycle detected"
                )

            levels.append(current_level)
            visited.update(current_level)

            for step_name in current_level:
                for dependent in forward.get(step_name, set()):
                    in_degree[dependent] -= 1

        return levels
