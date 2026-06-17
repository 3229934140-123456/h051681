"""
Artifact Manager Module
Handles collection, storage, and transfer of artifacts between pipeline steps.

How it works:
1. Each step can declare artifacts (files/directories) to preserve
2. After a step completes, artifacts are collected and stored
3. Downstream steps can declare inputs to receive specific artifacts
4. Artifacts are transferred from upstream to downstream workspaces
5. Supports glob patterns for flexible artifact matching
6. Metadata (size, timestamp, origin) is tracked for each artifact

Artifact Flow:
    [Step A] → produces artifacts → [Artifact Store]
                                          ↓
    [Step B] → declares inputs → [Artifact Manager] → copies to Step B workspace
"""

import fnmatch
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .models import Artifact, Step


class ArtifactManagerError(Exception):
    pass


class ArtifactManager:
    """
    Manages artifacts produced and consumed by pipeline steps.

    Storage Layout:
        artifacts/
            {pipeline_run_id}/
                {stage_name}/
                    {step_name}/
                        {artifact_name}/
                            {files...}
                metadata.json  # Index of all artifacts

    Example:
        manager = ArtifactManager(base_dir="./artifacts")

        # After step completes, collect its artifacts
        artifacts = await manager.collect_artifacts(
            step=step,
            source_dir="./workspace/build/compile",
            run_id="run_123"
        )

        # Before downstream step runs, prepare its inputs
        await manager.prepare_inputs(
            step=downstream_step,
            target_dir="./workspace/test/unit",
            run_id="run_123",
            available_artifacts=all_artifacts
        )
    """

    def __init__(self, base_dir: str = "./artifacts"):
        """
        Args:
            base_dir: Base directory for artifact storage
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_run_dir(self, run_id: str) -> Path:
        run_dir = self.base_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _get_step_artifact_dir(
        self, run_id: str, stage_name: str, step_name: str
    ) -> Path:
        step_dir = self._get_run_dir(run_id) / stage_name / step_name
        step_dir.mkdir(parents=True, exist_ok=True)
        return step_dir

    async def collect_artifacts(
        self,
        step: Step,
        source_dir: str,
        run_id: str,
    ) -> List[Artifact]:
        """
        Collect artifacts produced by a step.

        Args:
            step: The step that produced the artifacts
            source_dir: The working directory of the step
            run_id: Unique identifier for this pipeline run

        Returns:
            List of collected Artifact objects

        Example artifact patterns:
            - "dist/**/*.js"  # All JS files in dist recursively
            - "build/*.tar.gz"  # Tarballs in build dir
            - "config.yaml"  # Specific file
            - "logs/"  # Entire directory
        """
        collected: List[Artifact] = []
        source_path = Path(source_dir)

        if not source_path.exists():
            return collected

        for pattern in step.artifacts:
            artifacts = self._find_matching_files(source_path, pattern)

            for file_path in artifacts:
                artifact = await self._store_artifact(
                    file_path=file_path,
                    source_dir=source_path,
                    pattern=pattern,
                    step=step,
                    run_id=run_id,
                )
                collected.append(artifact)

        return collected

    def _find_matching_files(
        self, source_dir: Path, pattern: str
    ) -> List[Path]:
        """Find all files matching the glob pattern."""
        matches: List[Path] = []

        pattern_path = Path(pattern)
        if pattern.endswith("/") or pattern.endswith("\\"):
            pattern = pattern.rstrip("/\\") + "/**/*"

        if "**" in pattern:
            base_pattern = pattern.split("**")[0].rstrip("/\\") or "."
            recursive = True
        else:
            base_pattern = str(Path(pattern).parent)
            if base_pattern == ".":
                base_pattern = ""
            recursive = False

        search_root = source_dir / base_pattern if base_pattern else source_dir

        if not search_root.exists():
            return []

        for root, dirs, files in os.walk(search_root):
            root_path = Path(root)
            for filename in files:
                file_path = root_path / filename
                rel_path = file_path.relative_to(source_dir)
                rel_str = str(rel_path).replace("\\", "/")

                if fnmatch.fnmatch(rel_str, pattern) or fnmatch.fnmatch(
                    filename, pattern
                ):
                    matches.append(file_path)

            if not recursive:
                break

        return matches

    async def _store_artifact(
        self,
        file_path: Path,
        source_dir: Path,
        pattern: str,
        step: Step,
        run_id: str,
    ) -> Artifact:
        """Store a single artifact to the artifact store."""
        rel_path = file_path.relative_to(source_dir)
        artifact_name = f"{pattern}:{rel_path}"

        target_dir = self._get_step_artifact_dir(run_id, step.stage, step.name)
        target_path = target_dir / rel_path
        target_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(file_path, target_path)

        file_size = target_path.stat().st_size

        artifact = Artifact(
            name=artifact_name,
            path=str(target_path),
            step_name=step.name,
            stage_name=step.stage,
            source_rel_path=str(rel_path),
            size=file_size,
            created_at=datetime.now(),
        )

        return artifact

    async def prepare_inputs(
        self,
        step: Step,
        target_dir: str,
        run_id: str,
        available_artifacts: Dict[str, List[Artifact]],
    ) -> List[Artifact]:
        """
        Prepare inputs for a step by copying required artifacts.

        Args:
            step: The step that needs the inputs
            target_dir: The working directory of the step
            run_id: Unique identifier for this pipeline run
            available_artifacts: Dict of step_full_name -> List[Artifact]

        Returns:
            List of Artifact objects that were prepared

        Example input patterns:
            - "build/compile:dist/**/*"  # All dist artifacts from build/compile
            - "build/compile:*.tar.gz"   # Specific file type
            - "build/*:config.yaml"      # From any step in build stage
        """
        prepared: List[Artifact] = []
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)

        for input_pattern in step.inputs:
            matched_artifacts = self._match_input_pattern(
                input_pattern, available_artifacts
            )

            for artifact in matched_artifacts:
                dest_path = await self._copy_artifact_to_workspace(
                    artifact=artifact,
                    target_dir=target_path,
                    input_pattern=input_pattern,
                )
                prepared.append(artifact)

        return prepared

    def _match_input_pattern(
        self,
        input_pattern: str,
        available_artifacts: Dict[str, List[Artifact]],
    ) -> List[Artifact]:
        """
        Match an input pattern against available artifacts.

        Pattern format:
            "step_pattern:file_pattern"
            - step_pattern: Glob pattern matching "stage/step"
            - file_pattern: Glob pattern matching artifact file paths

        Examples:
            "build/compile:dist/**/*"
            "build/*:*.log"
            "*:result.txt"
        """
        if ":" not in input_pattern:
            step_pattern = "*"
            file_pattern = input_pattern
        else:
            step_pattern, file_pattern = input_pattern.split(":", 1)

        matches: List[Artifact] = []

        for step_full_name, artifacts in available_artifacts.items():
            if not fnmatch.fnmatch(step_full_name, step_pattern):
                continue

            for artifact in artifacts:
                artifact_rel = artifact.name.split(":", 1)[-1] if ":" in artifact.name else artifact.name

                if fnmatch.fnmatch(artifact_rel, file_pattern):
                    matches.append(artifact)
                else:
                    artifact_path = Path(artifact.path)
                    if fnmatch.fnmatch(artifact_path.name, file_pattern):
                        matches.append(artifact)

        return matches

    async def _copy_artifact_to_workspace(
        self,
        artifact: Artifact,
        target_dir: Path,
        input_pattern: str,
    ) -> Path:
        """Copy an artifact from storage to a step's workspace."""
        source_path = Path(artifact.path)

        if artifact.source_rel_path:
            rel_part = artifact.source_rel_path
        elif ":" in artifact.name:
            rel_part = artifact.name.split(":", 1)[-1]
        else:
            rel_part = Path(artifact.path).name

        dest_path = target_dir / rel_part
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if source_path.is_file():
            shutil.copy2(source_path, dest_path)

        return dest_path

    def list_artifacts(
        self, run_id: str, step_full_name: Optional[str] = None
    ) -> List[Artifact]:
        """List all artifacts for a run, optionally filtered by step."""
        artifacts: List[Artifact] = []
        run_dir = self._get_run_dir(run_id)

        for stage_dir in run_dir.iterdir():
            if not stage_dir.is_dir():
                continue
            for step_dir in stage_dir.iterdir():
                if not step_dir.is_dir():
                    continue

                current_step = f"{stage_dir.name}/{step_dir.name}"
                if step_full_name and current_step != step_full_name:
                    continue

                for root, _, files in os.walk(step_dir):
                    for filename in files:
                        file_path = Path(root) / filename
                        rel_path = file_path.relative_to(step_dir)
                        artifacts.append(
                            Artifact(
                                name=str(rel_path),
                                path=str(file_path),
                                step_name=step_dir.name,
                                stage_name=stage_dir.name,
                                size=file_path.stat().st_size,
                                created_at=datetime.fromtimestamp(
                                    file_path.stat().st_mtime
                                ),
                            )
                        )

        return artifacts

    def get_artifact_total_size(self, run_id: str) -> int:
        """Get total size of all artifacts in a run."""
        artifacts = self.list_artifacts(run_id)
        return sum(a.size for a in artifacts)

    def cleanup_run(self, run_id: str) -> None:
        """Remove all artifacts for a run."""
        run_dir = self._get_run_dir(run_id)
        if run_dir.exists():
            shutil.rmtree(run_dir)
