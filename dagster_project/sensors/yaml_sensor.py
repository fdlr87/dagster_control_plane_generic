"""YAML Dataset Sensor — detects new dataset YAML files and triggers onboarding.

This sensor scans the datasets/ directory periodically and when a new YAML
is detected (not yet materialized), it requests a run to materialize all
4 onboarding assets for that dataset.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import dagster as dg
from dagster import SensorEvaluationContext
import yaml

from dagster_project.models.dataset_config import DatasetYAML


def _compute_yaml_hash(filepath: Path) -> str:
    """Compute MD5 hash of a YAML file for change detection."""
    content = filepath.read_bytes()
    return hashlib.md5(content).hexdigest()


@dg.sensor(
    name="dataset_yaml_sensor",
    description=(
        "Monitors the datasets/ directory for new or modified YAML files. "
        "When a new dataset is detected, triggers materialization of its "
        "onboarding assets (Kafka topic, Debezium connector, ClickHouse tables, "
        "and historical load)."
    ),
    minimum_interval_seconds=30,
)
def dataset_yaml_sensor(context: SensorEvaluationContext):
    """Sensor that watches for new dataset YAML files."""
    datasets_dir = os.environ.get(
        "DATASETS_DIR",
        str(Path(__file__).parent.parent.parent / "datasets"),
    )
    datasets_path = Path(datasets_dir)

    if not datasets_path.exists():
        context.log.info(f"Datasets directory not found: {datasets_dir}")
        return

    # Load previous state (dict of filename → hash)
    previous_state: dict = {}
    if context.cursor:
        try:
            import json
            previous_state = json.loads(context.cursor)
        except Exception:
            previous_state = {}

    current_state: dict = {}
    new_datasets: list[str] = []

    for yaml_file in sorted(datasets_path.glob("*.yaml")):
        file_hash = _compute_yaml_hash(yaml_file)
        current_state[yaml_file.name] = file_hash

        # Detect new or modified files
        if yaml_file.name not in previous_state or previous_state[yaml_file.name] != file_hash:
            try:
                with open(yaml_file) as f:
                    raw = yaml.safe_load(f)
                if raw and "dataset" in raw:
                    parsed = DatasetYAML.model_validate(raw)
                    new_datasets.append(parsed.dataset.name)
                    context.log.info(
                        f"🆕 Detected new/modified dataset: {parsed.dataset.name} "
                        f"({yaml_file.name})"
                    )
            except Exception as e:
                context.log.error(f"Error parsing {yaml_file.name}: {e}")

    # Yield run requests for new datasets
    for dataset_name in new_datasets:
        asset_keys = [
            dg.AssetKey(f"kafka_topic_{dataset_name}"),
            dg.AssetKey(f"debezium_connector_{dataset_name}"),
            dg.AssetKey(f"clickhouse_tables_{dataset_name}"),
            dg.AssetKey(f"historical_load_{dataset_name}"),
        ]
        yield dg.RunRequest(
            run_key=f"onboard_{dataset_name}_{current_state.get(f'{dataset_name}.yaml', 'new')}",
            asset_selection=asset_keys,
        )

    # Update cursor with current state
    import json
    context.update_cursor(json.dumps(current_state))
