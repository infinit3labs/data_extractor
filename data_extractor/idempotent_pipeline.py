#!/usr/bin/env python3
"""Idempotent data pipeline prototype."""

from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


class PipelineStatus(Enum):
    """Possible pipeline run states."""

    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


@dataclass
class PipelineRunRecord:
    pipeline_name: str
    business_date: str
    run_id: str
    status: PipelineStatus
    tables_attempted: List[str]
    tables_succeeded: List[str]
    tables_failed: List[str]
    output_path: str
    is_active: bool
    start_ts: str
    end_ts: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class TableStateRecord:
    table_name: str
    business_date: str
    successful_run_id: Optional[str]
    extraction_path: Optional[str]
    schema_hash: Optional[str]
    data_checksum: Optional[str]
    last_updated: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AtomicFileWriter:
    """Atomic write helper."""

    @staticmethod
    def write_json(path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump(data, fh, indent=2)
        tmp.rename(path)


class SchemaManager:
    """Schema validation."""

    @staticmethod
    def schema_hash(schema: Dict[str, str]) -> str:
        return json.dumps(schema, sort_keys=True)

    @staticmethod
    def validate(old: Dict[str, str], new: Dict[str, str]) -> None:
        for col, typ in old.items():
            if col in new and new[col] != typ:
                raise ValueError(f"column {col} type change from {typ} to {new[col]}")


class DataIntegrityValidator:
    """Checksum calculation."""

    @staticmethod
    def checksum(rows: List[Dict[str, Any]]) -> str:
        return json.dumps(rows, sort_keys=True)


class MetadataManager:
    """Simple metadata manager storing JSON files."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.meta = base / "metadata"
        self.run_log = self.meta / "pipeline_run_log.json"
        self.table_state = self.meta / "table_state.json"
        self.meta.mkdir(parents=True, exist_ok=True)
        if not self.run_log.exists():
            AtomicFileWriter.write_json(self.run_log, [])
        if not self.table_state.exists():
            AtomicFileWriter.write_json(self.table_state, [])

    def append_run(self, rec: PipelineRunRecord) -> None:
        runs = json.loads(self.run_log.read_text())
        runs.append(rec.to_dict())
        AtomicFileWriter.write_json(self.run_log, runs)

    def update_run(self, run_id: str, updates: Dict[str, Any]) -> None:
        runs = json.loads(self.run_log.read_text())
        for rec in runs:
            if rec["run_id"] == run_id:
                rec.update(updates)
                break
        AtomicFileWriter.write_json(self.run_log, runs)

    def get_state(self, table: str, date: str) -> Optional[TableStateRecord]:
        states = json.loads(self.table_state.read_text())
        for s in states:
            if s["table_name"] == table and s["business_date"] == date:
                return TableStateRecord(**s)
        return None

    def update_state(self, rec: TableStateRecord) -> None:
        states = json.loads(self.table_state.read_text())
        states = [
            s
            for s in states
            if not (
                s["table_name"] == rec.table_name
                and s["business_date"] == rec.business_date
            )
        ]
        states.append(rec.to_dict())
        AtomicFileWriter.write_json(self.table_state, states)

    def completed_tables(self, date: str) -> Set[str]:
        states = json.loads(self.table_state.read_text())
        return {
            s["table_name"]
            for s in states
            if s["business_date"] == date and s["successful_run_id"] is not None
        }


class MockDataExtractor:
    """Fake data and schema source."""

    SCHEMAS = {
        "customers": {"id": "int", "name": "string", "email": "string"},
        "orders": {
            "id": "int",
            "customer_id": "int",
            "amount": "decimal",
            "order_date": "date",
        },
    }

    DATA = {
        "customers": [
            {"id": 1, "name": "Alice", "email": "a@example.com"},
            {"id": 2, "name": "Bob", "email": "b@example.com"},
        ],
        "orders": [
            {
                "id": 101,
                "customer_id": 1,
                "amount": 50.0,
                "order_date": "2024-07-25",
            },
            {
                "id": 102,
                "customer_id": 2,
                "amount": 75.5,
                "order_date": "2024-07-25",
            },
        ],
    }

    @classmethod
    def schema(cls, table: str) -> Dict[str, str]:
        schema = dict(cls.SCHEMAS.get(table, {}))
        if table == "customers" and random.random() > 0.5:
            schema["phone"] = "string"
        return schema

    @classmethod
    def data(cls, table: str) -> List[Dict[str, Any]]:
        if random.random() < 0.1:
            raise ConnectionError("simulated network error")
        return cls.DATA.get(table, [])


class DataPipelineOrchestrator:
    """Run the pipeline in an idempotent manner."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.meta = MetadataManager(base)
        self.extractor = MockDataExtractor()

    def _run_id(self) -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _path(self, source: str, date: str, run: str, table: str) -> Path:
        return (
            self.base / "raw" / source / f"batch_date={date}" / f"run_id={run}" / table
        )

    def _needs_processing(self, table: str, date: str) -> bool:
        state = self.meta.get_state(table, date)
        if not state or not state.successful_run_id:
            return True
        existing = Path(state.extraction_path)
        if not (existing / "data.json").exists():
            return True
        data = json.loads((existing / "data.json").read_text())
        return DataIntegrityValidator.checksum(data) != state.data_checksum

    def _process_table(self, table: str, source: str, date: str, run: str) -> bool:
        try:
            schema = self.extractor.schema(table)
            data = self.extractor.data(table)
            state = self.meta.get_state(table, date)
            if state and state.schema_hash:
                old = json.loads(
                    (Path(state.extraction_path) / "schema.json").read_text()
                )
                SchemaManager.validate(old, schema)
            sch_hash = SchemaManager.schema_hash(schema)
            checksum = DataIntegrityValidator.checksum(data)
            out = self._path(source, date, run, table)
            AtomicFileWriter.write_json(out / "schema.json", schema)
            AtomicFileWriter.write_json(out / "data.json", data)
            rec = TableStateRecord(
                table_name=table,
                business_date=date,
                successful_run_id=run,
                extraction_path=str(out),
                schema_hash=sch_hash,
                data_checksum=checksum,
                last_updated=datetime.now().isoformat(),
            )
            self.meta.update_state(rec)
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"error processing {table}: {exc}")
            return False

    def run_pipeline(
        self, pipeline: str, date: str, tables: List[str], force_rerun: bool = False
    ) -> str | None:
        run_id = self._run_id()
        start = datetime.now().isoformat()
        if not force_rerun:
            todo = [t for t in tables if self._needs_processing(t, date)]
        else:
            todo = list(tables)
        if not todo:
            print("no changes needed")
            return "no_changes_needed"
        rec = PipelineRunRecord(
            pipeline_name=pipeline,
            business_date=date,
            run_id=run_id,
            status=PipelineStatus.STARTED,
            tables_attempted=todo,
            tables_succeeded=[],
            tables_failed=[],
            output_path=str(self.base / "raw" / pipeline / f"batch_date={date}"),
            is_active=True,
            start_ts=start,
        )
        self.meta.append_run(rec)
        ok: List[str] = []
        fail: List[str] = []
        for table in todo:
            if self._process_table(table, pipeline, date, run_id):
                ok.append(table)
            else:
                fail.append(table)
        status = (
            PipelineStatus.SUCCESS
            if not fail
            else PipelineStatus.PARTIAL if ok else PipelineStatus.FAILED
        )
        self.meta.update_run(
            run_id,
            {
                "status": status.value,
                "tables_succeeded": ok,
                "tables_failed": fail,
                "is_active": False,
                "end_ts": datetime.now().isoformat(),
            },
        )
        print(f"run {run_id} complete: {status.value}")
        return run_id

    def complete_dataset(self, date: str) -> Dict[str, str]:
        states = json.loads(self.meta.table_state.read_text())
        return {
            s["table_name"]: s["extraction_path"]
            for s in states
            if s["business_date"] == date and s["successful_run_id"] is not None
        }


def demo_pipeline() -> None:
    base = Path("/tmp/pipeline_demo")
    orchestrator = DataPipelineOrchestrator(base)
    tables = ["customers", "orders"]
    date = "2024-07-25"
    print("initial run")
    orchestrator.run_pipeline("oracle", date, tables)
    print("replay")
    orchestrator.run_pipeline("oracle", date, tables)
    print("forced")
    orchestrator.run_pipeline("oracle", date, tables, force_rerun=True)
    print("dataset", orchestrator.complete_dataset(date))


if __name__ == "__main__":
    demo_pipeline()
