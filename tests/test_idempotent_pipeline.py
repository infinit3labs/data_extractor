import importlib.util
import random
import sys
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "data_extractor" / "idempotent_pipeline.py"
)
SPEC = importlib.util.spec_from_file_location("idempotent_pipeline", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
assert SPEC.loader
SPEC.loader.exec_module(module)
DataPipelineOrchestrator = module.DataPipelineOrchestrator


def test_idempotent_run(tmp_path: Path) -> None:
    random.seed(0)
    orchestrator = DataPipelineOrchestrator(tmp_path)
    tables = ["customers"]
    date = "2024-07-25"
    run1 = orchestrator.run_pipeline("demo", date, tables)
    run2 = orchestrator.run_pipeline("demo", date, tables)
    assert run1 != "no_changes_needed"
    assert run2 == "no_changes_needed"
