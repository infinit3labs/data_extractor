"""
State management module for tracking pipeline progress and enabling restarts.
Provides idempotent operations and persistent state tracking.
"""

import json
import logging
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import fcntl


class ExtractionStatus(Enum):
    """Status of an extraction operation."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class PipelineStatus(Enum):
    """Overall pipeline status."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


@dataclass
class ExtractionState:
    """State information for a single table extraction."""
    
    # Identification
    source_name: str
    table_name: str
    schema_name: Optional[str] = None
    
    # Extraction configuration
    incremental_column: Optional[str] = None
    is_full_extract: bool = False
    custom_query: Optional[str] = None
    
    # State tracking
    status: ExtractionStatus = ExtractionStatus.PENDING
    run_id: Optional[str] = None
    extraction_date: Optional[Union[datetime, str]] = None
    
    # Progress tracking
    start_time: Optional[Union[datetime, str]] = None
    end_time: Optional[Union[datetime, str]] = None
    record_count: int = 0
    output_path: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    
    # Metadata
    thread_name: Optional[str] = None
    spark_session_id: Optional[str] = None
    checksum: Optional[str] = None
    file_size_bytes: int = 0
    
    def __post_init__(self):
        """Post-initialization processing."""
        # Convert string timestamps to datetime objects
        if isinstance(self.extraction_date, str) and self.extraction_date:
            self.extraction_date = datetime.fromisoformat(self.extraction_date.replace('Z', '+00:00'))
        if isinstance(self.start_time, str) and self.start_time:
            self.start_time = datetime.fromisoformat(self.start_time.replace('Z', '+00:00'))
        if isinstance(self.end_time, str) and self.end_time:
            self.end_time = datetime.fromisoformat(self.end_time.replace('Z', '+00:00'))

    @property
    def table_key(self) -> str:
        """Generate unique key for this table extraction."""
        if self.schema_name:
            return f"{self.source_name}.{self.schema_name}.{self.table_name}"
        return f"{self.source_name}.{self.table_name}"
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate extraction duration in seconds."""
        if self.start_time and self.end_time and isinstance(self.start_time, datetime) and isinstance(self.end_time, datetime):
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def is_complete(self) -> bool:
        """Check if extraction is complete (successful or failed)."""
        return self.status in (ExtractionStatus.COMPLETED, ExtractionStatus.FAILED, ExtractionStatus.SKIPPED)
    
    @property
    def is_running(self) -> bool:
        """Check if extraction is currently running."""
        return self.status == ExtractionStatus.RUNNING


@dataclass
class PipelineState:
    """State information for the entire pipeline run."""
    
    # Pipeline identification
    pipeline_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    
    # Pipeline configuration
    extraction_date: Union[datetime, str] = field(default_factory=lambda: datetime.now() - timedelta(days=1))
    output_base_path: str = "data"
    max_workers: int = 1
    
    # Pipeline state
    status: PipelineStatus = PipelineStatus.IDLE
    start_time: Optional[Union[datetime, str]] = None
    end_time: Optional[Union[datetime, str]] = None
    
    # Progress tracking
    total_tables: int = 0
    completed_tables: int = 0
    failed_tables: int = 0
    skipped_tables: int = 0
    
    # Metadata
    config_checksum: Optional[str] = None
    restart_count: int = 0
    last_checkpoint: Optional[Union[datetime, str]] = None
    
    def __post_init__(self):
        """Post-initialization processing."""
        # Convert string timestamps to datetime objects
        if isinstance(self.extraction_date, str) and self.extraction_date:
            self.extraction_date = datetime.fromisoformat(self.extraction_date.replace('Z', '+00:00'))
        if isinstance(self.start_time, str) and self.start_time:
            self.start_time = datetime.fromisoformat(self.start_time.replace('Z', '+00:00'))
        if isinstance(self.end_time, str) and self.end_time:
            self.end_time = datetime.fromisoformat(self.end_time.replace('Z', '+00:00'))
        if isinstance(self.last_checkpoint, str) and self.last_checkpoint:
            self.last_checkpoint = datetime.fromisoformat(self.last_checkpoint.replace('Z', '+00:00'))

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate pipeline duration in seconds."""
        if (self.start_time and self.end_time and 
            isinstance(self.start_time, datetime) and isinstance(self.end_time, datetime)):
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def completion_rate(self) -> float:
        """Calculate completion rate as percentage."""
        if self.total_tables == 0:
            return 0.0
        return (self.completed_tables + self.skipped_tables) / self.total_tables * 100
    
    @property
    def is_complete(self) -> bool:
        """Check if pipeline is complete."""
        return self.status in (PipelineStatus.COMPLETED, PipelineStatus.FAILED, PipelineStatus.CANCELLED)


class StateManager:
    """
    Manages pipeline and extraction state with persistence and restart capabilities.
    
    Features:
    - Persistent state storage in JSON format
    - Thread-safe operations
    - Idempotent extraction checks
    - Progress tracking and reporting
    - Restart capabilities with state recovery
    - Checkpoint-based recovery
    """
    
    def __init__(
        self,
        state_dir: str = "state",
        enable_checkpoints: bool = True,
        checkpoint_interval_seconds: int = 30,
        max_retry_attempts: int = 3,
        cleanup_after_days: int = 7
    ):
        """
        Initialize StateManager.
        
        Args:
            state_dir: Directory to store state files
            enable_checkpoints: Whether to enable periodic checkpoints
            checkpoint_interval_seconds: Interval between automatic checkpoints
            max_retry_attempts: Maximum retry attempts for failed extractions
            cleanup_after_days: Days after which to clean up old state files
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        self.enable_checkpoints = enable_checkpoints
        self.checkpoint_interval_seconds = checkpoint_interval_seconds
        self.max_retry_attempts = max_retry_attempts
        self.cleanup_after_days = cleanup_after_days
        
        # Thread-safe state storage
        self._lock = threading.RLock()
        self._pipeline_state: Optional[PipelineState] = None
        self._extraction_states: Dict[str, ExtractionState] = {}
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Checkpoint timer
        self._checkpoint_timer: Optional[threading.Timer] = None
    
    def _generate_checksum(self, data: Any) -> str:
        """Generate checksum for data consistency checks."""
        import hashlib
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _verify_extraction_integrity(self, state: ExtractionState) -> bool:
        """
        Verify the integrity of an extraction by checking file existence and optional checksum.
        
        Args:
            state: ExtractionState to verify
            
        Returns:
            bool: True if extraction is valid, False otherwise
        """
        if not state.output_path:
            return False
        
        # Check if file exists
        if not Path(state.output_path).exists():
            return False
        
        # Check if it's a directory (Parquet output)
        if Path(state.output_path).is_dir():
            # Verify parquet files exist in directory
            parquet_files = list(Path(state.output_path).rglob("*.parquet"))
            if not parquet_files:
                return False
        
        # Verify file size if recorded
        if state.file_size_bytes > 0:
            try:
                actual_size = self._get_path_size(state.output_path)
                # Allow 5% variance in file size
                if abs(actual_size - state.file_size_bytes) / state.file_size_bytes > 0.05:
                    self.logger.warning(f"File size mismatch for {state.output_path}: expected {state.file_size_bytes}, got {actual_size}")
                    return False
            except Exception as e:
                self.logger.warning(f"Could not verify file size for {state.output_path}: {e}")
        
        # Verify checksum if available
        if state.checksum:
            try:
                actual_checksum = self._calculate_path_checksum(state.output_path)
                if actual_checksum != state.checksum:
                    self.logger.warning(f"Checksum mismatch for {state.output_path}")
                    return False
            except Exception as e:
                self.logger.warning(f"Could not verify checksum for {state.output_path}: {e}")
        
        return True
    
    def _is_extraction_window_current(self, state: ExtractionState) -> bool:
        """
        Check if the extraction window for this state matches the current pipeline's expected window.
        
        For incremental extractions with 24-hour windows, this ensures we're not reusing
        data from a different day's extraction.
        
        Args:
            state: ExtractionState to check
            
        Returns:
            bool: True if the extraction window is current, False otherwise
        """
        if not self._pipeline_state or not state.extraction_date:
            return True  # Cannot determine, assume current
        
        # Convert to datetime if string
        pipeline_date = self._pipeline_state.extraction_date
        if isinstance(pipeline_date, str):
            pipeline_date = datetime.fromisoformat(pipeline_date.replace('Z', '+00:00'))
        
        state_date = state.extraction_date
        if isinstance(state_date, str):
            state_date = datetime.fromisoformat(state_date.replace('Z', '+00:00'))
        
        # For the current 24-hour window implementation, check if dates match (same day)
        if isinstance(pipeline_date, datetime) and isinstance(state_date, datetime):
            return pipeline_date.date() == state_date.date()
        
        return True  # Default to current if cannot determine
    
    def _get_path_size(self, path: str) -> int:
        """Get total size of a file or directory in bytes."""
        path_obj = Path(path)
        if path_obj.is_file():
            return path_obj.stat().st_size
        elif path_obj.is_dir():
            return sum(f.stat().st_size for f in path_obj.rglob('*') if f.is_file())
        return 0
    
    def _calculate_path_checksum(self, path: str) -> str:
        """Calculate checksum for a file or directory."""
        import hashlib
        hash_md5 = hashlib.md5()
        
        path_obj = Path(path)
        if path_obj.is_file():
            with open(path_obj, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
        elif path_obj.is_dir():
            # For directories, create checksum from all file contents
            for file_path in sorted(path_obj.rglob('*')):
                if file_path.is_file():
                    # Include relative path in checksum
                    hash_md5.update(str(file_path.relative_to(path_obj)).encode())
                    with open(file_path, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            hash_md5.update(chunk)
        
        return hash_md5.hexdigest()
    
    def _get_state_file_path(self, pipeline_id: str) -> Path:
        """Get path for pipeline state file."""
        return self.state_dir / f"pipeline_{pipeline_id}.json"
    
    def _save_state_to_file(self, pipeline_state: PipelineState, extraction_states: Dict[str, ExtractionState]) -> None:
        """Save current state to file with file locking."""
        state_file = self._get_state_file_path(pipeline_state.pipeline_id)
        
        # Prepare state data for serialization, ensuring no circular references
        def serialize_state(state_obj):
            """Serialize state object to dict, handling datetime objects and enums."""
            data = asdict(state_obj)
            # Convert datetime objects to ISO strings and enums to their values
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.isoformat()
                elif hasattr(value, 'value'):  # Handle enums
                    data[key] = value.value
                elif hasattr(value, 'name'):  # Handle enums (alternative)
                    data[key] = value.name
            return data
        
        state_data = {
            "pipeline": serialize_state(pipeline_state),
            "extractions": {key: serialize_state(state) for key, state in extraction_states.items()},
            "metadata": {
                "saved_at": datetime.now().isoformat(),
                "version": "1.0"
            }
        }
        
        # Write to temporary file first, then rename for atomic operation
        temp_file = state_file.with_suffix(".tmp")
        try:
            with open(temp_file, 'w') as f:
                # Apply file lock for exclusive access
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(state_data, f, indent=2)
            
            # Atomic rename
            temp_file.rename(state_file)
            self.logger.debug(f"State saved to {state_file}")
            
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise e
    
    def _load_state_from_file(self, pipeline_id: str) -> tuple[Optional[PipelineState], Dict[str, ExtractionState]]:
        """Load state from file."""
        state_file = self._get_state_file_path(pipeline_id)
        
        if not state_file.exists():
            return None, {}
        
        try:
            with open(state_file, 'r') as f:
                # Apply file lock for shared access
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                state_data = json.load(f)
            
            # Helper function to convert data back to proper types
            def deserialize_state_data(data, state_type):
                """Convert loaded JSON data back to proper state object."""
                converted_data = {}
                for key, value in data.items():
                    if key == 'status':
                        if state_type == 'pipeline':
                            converted_data[key] = PipelineStatus(value)
                        elif state_type == 'extraction':
                            converted_data[key] = ExtractionStatus(value)
                        else:
                            converted_data[key] = value
                    elif key in ['created_at', 'updated_at', 'start_time', 'end_time', 'extraction_start', 'extraction_end']:
                        # Convert ISO string back to datetime
                        if value:
                            converted_data[key] = datetime.fromisoformat(value)
                        else:
                            converted_data[key] = value
                    else:
                        converted_data[key] = value
                return converted_data
            
            # Reconstruct pipeline state
            pipeline_data = state_data.get("pipeline", {})
            pipeline_converted = deserialize_state_data(pipeline_data, 'pipeline')
            pipeline_state = PipelineState(**pipeline_converted)
            
            # Reconstruct extraction states
            extraction_states = {}
            for key, extraction_data in state_data.get("extractions", {}).items():
                extraction_converted = deserialize_state_data(extraction_data, 'extraction')
                extraction_states[key] = ExtractionState(**extraction_converted)
            
            self.logger.debug(f"State loaded from {state_file}")
            return pipeline_state, extraction_states
            
        except Exception as e:
            self.logger.error(f"Failed to load state from {state_file}: {e}")
            return None, {}
    
    def start_pipeline(
        self,
        table_configs: List[Dict[str, Any]],
        run_id: Optional[str] = None,
        extraction_date: Optional[datetime] = None,
        output_base_path: str = "data",
        max_workers: int = 1,
        resume_existing: bool = True
    ) -> str:
        """
        Start a new pipeline or resume existing one.
        
        Args:
            table_configs: List of table configuration dictionaries
            run_id: Optional run ID (generated if not provided)
            extraction_date: Date for extraction (yesterday if not provided)
            output_base_path: Base path for output files
            max_workers: Maximum number of worker threads
            resume_existing: Whether to resume existing pipeline if found
        
        Returns:
            str: Pipeline ID
        """
        with self._lock:
            # Generate pipeline identifiers
            if not run_id:
                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if not extraction_date:
                extraction_date = datetime.now() - timedelta(days=1)
            
            # Check for existing pipeline with same run_id
            pipeline_id = None
            existing_pipeline = None
            existing_extractions = {}
            
            if resume_existing:
                # Look for existing pipeline with same run_id
                for state_file in self.state_dir.glob("pipeline_*.json"):
                    try:
                        pipeline_state, extraction_states = self._load_state_from_file(
                            state_file.stem.replace("pipeline_", "")
                        )
                        if pipeline_state and pipeline_state.run_id == run_id:
                            pipeline_id = pipeline_state.pipeline_id
                            existing_pipeline = pipeline_state
                            existing_extractions = extraction_states
                            break
                    except Exception:
                        continue
            
            # Create new pipeline if none found
            if not pipeline_id:
                pipeline_id = str(uuid.uuid4())
                
                self._pipeline_state = PipelineState(
                    pipeline_id=pipeline_id,
                    run_id=run_id,
                    extraction_date=extraction_date,
                    output_base_path=output_base_path,
                    max_workers=max_workers,
                    total_tables=len(table_configs),
                    status=PipelineStatus.RUNNING,
                    start_time=datetime.now()
                )
                
                # Initialize extraction states
                self._extraction_states = {}
                for config in table_configs:
                    extraction_state = ExtractionState(
                        source_name=config.get("source_name", ""),
                        table_name=config.get("table_name", ""),
                        schema_name=config.get("schema_name"),
                        incremental_column=config.get("incremental_column"),
                        is_full_extract=config.get("is_full_extract", False),
                        custom_query=config.get("custom_query"),
                        run_id=run_id,
                        extraction_date=extraction_date
                    )
                    self._extraction_states[extraction_state.table_key] = extraction_state
                
                self.logger.info(f"Started new pipeline {pipeline_id} with run_id {run_id}")
                
            else:
                # Resume existing pipeline (we know existing_pipeline is not None here)
                assert existing_pipeline is not None  # Type hint for mypy
                self._pipeline_state = existing_pipeline
                self._extraction_states = existing_extractions
                
                # Update pipeline state for restart
                self._pipeline_state.status = PipelineStatus.RUNNING
                self._pipeline_state.restart_count += 1
                
                # Add any new tables from config
                existing_keys = set(self._extraction_states.keys())
                config_keys = set()
                
                for config in table_configs:
                    extraction_state = ExtractionState(
                        source_name=config.get("source_name", ""),
                        table_name=config.get("table_name", ""),
                        schema_name=config.get("schema_name"),
                        incremental_column=config.get("incremental_column"),
                        is_full_extract=config.get("is_full_extract", False),
                        custom_query=config.get("custom_query"),
                        run_id=run_id,
                        extraction_date=extraction_date
                    )
                    config_keys.add(extraction_state.table_key)
                    
                    if extraction_state.table_key not in existing_keys:
                        self._extraction_states[extraction_state.table_key] = extraction_state
                
                # Update total count
                self._pipeline_state.total_tables = len(self._extraction_states)
                
                self.logger.info(f"Resumed pipeline {pipeline_id} with run_id {run_id} (restart #{self._pipeline_state.restart_count})")
            
            # Save initial state (we know _pipeline_state is not None here)
            assert self._pipeline_state is not None  # Type hint for mypy
            self._save_state_to_file(self._pipeline_state, self._extraction_states)
            
            # Start checkpoint timer if enabled
            if self.enable_checkpoints:
                self._start_checkpoint_timer()
            
            return pipeline_id
    
    def _start_checkpoint_timer(self) -> None:
        """Start periodic checkpoint timer."""
        if self._checkpoint_timer:
            self._checkpoint_timer.cancel()
        
        def checkpoint():
            try:
                self.checkpoint()
                self._start_checkpoint_timer()  # Schedule next checkpoint
            except Exception as e:
                self.logger.error(f"Checkpoint failed: {e}")
        
        self._checkpoint_timer = threading.Timer(self.checkpoint_interval_seconds, checkpoint)
        self._checkpoint_timer.daemon = True
        self._checkpoint_timer.start()
    
    def checkpoint(self) -> None:
        """Save current state as checkpoint."""
        with self._lock:
            if self._pipeline_state:
                self._pipeline_state.last_checkpoint = datetime.now()
                self._save_state_to_file(self._pipeline_state, self._extraction_states)
                self.logger.debug("Checkpoint saved")
    
    def is_extraction_needed(self, table_key: str, force_reprocess: bool = False) -> bool:
        """
        Check if extraction is needed for a table (idempotent check).
        
        Args:
            table_key: Unique table identifier
            force_reprocess: Force reprocessing even if already completed
            
        Returns:
            bool: True if extraction is needed, False if already completed
        """
        with self._lock:
            if table_key not in self._extraction_states:
                return True
            
            state = self._extraction_states[table_key]
            
            # Force reprocessing if requested
            if force_reprocess:
                self.logger.info(f"Force reprocessing requested for {table_key}")
                state.status = ExtractionStatus.PENDING
                return True
            
            # Always need extraction if not completed
            if not state.is_complete:
                return True
            
            # Enhanced idempotent checks for completed extractions
            if state.status == ExtractionStatus.COMPLETED:
                # Verify output file still exists and is valid
                if state.output_path and self._verify_extraction_integrity(state):
                    # Additional check: verify the extraction window matches current run
                    if self._is_extraction_window_current(state):
                        self.logger.debug(f"Table {table_key} already extracted successfully for current window")
                        return False
                    else:
                        self.logger.info(f"Table {table_key} extraction window outdated, will re-extract")
                        state.status = ExtractionStatus.PENDING
                        return True
                else:
                    # Output file missing or corrupted, need to re-extract
                    self.logger.warning(f"Output file missing or corrupted for {table_key}, will re-extract")
                    state.status = ExtractionStatus.PENDING
                    return True
            
            # Check if failed and retry attempts not exhausted
            if state.status == ExtractionStatus.FAILED:
                if state.retry_count < self.max_retry_attempts:
                    self.logger.info(f"Table {table_key} failed, will retry (attempt {state.retry_count + 1}/{self.max_retry_attempts})")
                    return True
                else:
                    self.logger.warning(f"Table {table_key} failed max retry attempts, skipping")
                    state.status = ExtractionStatus.SKIPPED
                    return False
            
            return True
    
    def start_extraction(self, table_key: str, thread_name: str) -> bool:
        """
        Mark extraction as started for a table.
        
        Args:
            table_key: Unique table identifier
            thread_name: Name of the thread performing extraction
            
        Returns:
            bool: True if extraction started, False if already running/completed
        """
        with self._lock:
            if table_key not in self._extraction_states:
                self.logger.error(f"Unknown table key: {table_key}")
                return False
            
            state = self._extraction_states[table_key]
            
            # Check if already running
            if state.is_running:
                self.logger.warning(f"Table {table_key} is already running")
                return False
            
            # Check if already completed successfully
            if state.status == ExtractionStatus.COMPLETED and state.output_path and Path(state.output_path).exists():
                self.logger.info(f"Table {table_key} already completed successfully")
                return False
            
            # Update state
            state.status = ExtractionStatus.RUNNING
            state.start_time = datetime.now()
            state.thread_name = thread_name
            
            # Increment retry count if this is a retry
            if state.end_time:  # Has been attempted before
                state.retry_count += 1
                state.status = ExtractionStatus.RETRYING
            
            self.logger.info(f"Started extraction for {table_key} on thread {thread_name}")
            return True
    
    def complete_extraction(
        self,
        table_key: str,
        success: bool,
        record_count: int = 0,
        output_path: Optional[str] = None,
        error_message: Optional[str] = None,
        file_size_bytes: int = 0,
        checksum: Optional[str] = None
    ) -> None:
        """
        Mark extraction as completed for a table.
        
        Args:
            table_key: Unique table identifier
            success: Whether extraction was successful
            record_count: Number of records extracted
            output_path: Path to output file
            error_message: Error message if failed
            file_size_bytes: Size of output file in bytes
            checksum: Checksum of output file for verification
        """
        with self._lock:
            if table_key not in self._extraction_states:
                self.logger.error(f"Unknown table key: {table_key}")
                return
            
            state = self._extraction_states[table_key]
            
            # Update state
            state.end_time = datetime.now()
            state.record_count = record_count
            state.output_path = output_path
            state.file_size_bytes = file_size_bytes
            state.checksum = checksum
            
            if success:
                state.status = ExtractionStatus.COMPLETED
                state.error_message = None
                self.logger.info(f"Completed extraction for {table_key}: {record_count} records")
            else:
                state.status = ExtractionStatus.FAILED
                state.error_message = error_message
                self.logger.error(f"Failed extraction for {table_key}: {error_message}")
            
            # Update pipeline progress
            if self._pipeline_state:
                if success:
                    self._pipeline_state.completed_tables += 1
                else:
                    self._pipeline_state.failed_tables += 1
    
    def get_pending_extractions(self, include_failed_retries: bool = True) -> List[str]:
        """
        Get list of table keys that need extraction.
        
        Args:
            include_failed_retries: Whether to include failed extractions that can be retried
            
        Returns:
            List[str]: Table keys that need extraction
        """
        with self._lock:
            pending = []
            for table_key in self._extraction_states:
                if self.is_extraction_needed(table_key):
                    state = self._extraction_states[table_key]
                    
                    # Skip failed extractions if retries not wanted
                    if not include_failed_retries and state.status == ExtractionStatus.FAILED:
                        continue
                    
                    pending.append(table_key)
            return pending
    
    def force_reprocess_table(self, table_key: str) -> bool:
        """
        Force reprocessing of a specific table, regardless of current status.
        
        Args:
            table_key: Table key to force reprocess
            
        Returns:
            bool: True if table was marked for reprocessing
        """
        with self._lock:
            if table_key not in self._extraction_states:
                self.logger.warning(f"Cannot force reprocess unknown table: {table_key}")
                return False
            
            state = self._extraction_states[table_key]
            state.status = ExtractionStatus.PENDING
            state.retry_count = 0
            state.error_message = None
            state.start_time = None
            state.end_time = None
            
            self.logger.info(f"Forced reprocessing for table: {table_key}")
            return True
    
    def reset_failed_extractions(self) -> int:
        """
        Reset all failed extractions to pending status for retry.
        
        Returns:
            int: Number of failed extractions reset
        """
        with self._lock:
            reset_count = 0
            for state in self._extraction_states.values():
                if state.status == ExtractionStatus.FAILED and state.retry_count < self.max_retry_attempts:
                    state.status = ExtractionStatus.PENDING
                    state.error_message = None
                    reset_count += 1
            
            if reset_count > 0:
                self.logger.info(f"Reset {reset_count} failed extractions to pending status")
            
            return reset_count
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """
        Get detailed summary of extractions by status.
        
        Returns:
            Dict containing extraction summary statistics
        """
        with self._lock:
            summary: Dict[str, Any] = {
                "total_extractions": len(self._extraction_states),
                "by_status": {},
                "completed_tables": [],
                "failed_tables": [],
                "pending_tables": [],
                "running_tables": [],
                "total_records_extracted": 0,
                "total_file_size_bytes": 0,
                "average_duration_seconds": 0,
                "fastest_extraction": None,
                "slowest_extraction": None
            }
            
            durations = []
            
            for status in ExtractionStatus:
                summary["by_status"][status.value] = 0
            
            for table_key, state in self._extraction_states.items():
                summary["by_status"][state.status.value] += 1
                
                if state.status == ExtractionStatus.COMPLETED:
                    summary["completed_tables"].append({
                        "table_key": table_key,
                        "records": state.record_count,
                        "duration_seconds": state.duration_seconds,
                        "file_size_bytes": state.file_size_bytes
                    })
                    summary["total_records_extracted"] += state.record_count
                    summary["total_file_size_bytes"] += state.file_size_bytes
                    
                    if state.duration_seconds:
                        durations.append(state.duration_seconds)
                        
                elif state.status == ExtractionStatus.FAILED:
                    summary["failed_tables"].append({
                        "table_key": table_key,
                        "error": state.error_message,
                        "retry_count": state.retry_count
                    })
                elif state.status == ExtractionStatus.PENDING:
                    summary["pending_tables"].append(table_key)
                elif state.status == ExtractionStatus.RUNNING:
                    summary["running_tables"].append({
                        "table_key": table_key,
                        "thread": state.thread_name,
                        "start_time": self._serialize_datetime(state.start_time)
                    })
            
            # Calculate duration statistics
            if durations:
                summary["average_duration_seconds"] = sum(durations) / len(durations)
                summary["fastest_extraction"] = min(durations)
                summary["slowest_extraction"] = max(durations)
            
            return summary
    
    def _serialize_datetime(self, dt: Optional[Union[datetime, str]]) -> Optional[str]:
        """Safely serialize datetime to ISO format string."""
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat()
        return dt  # Already a string
    
    def get_pipeline_progress(self) -> Dict[str, Any]:
        """Get current pipeline progress information."""
        with self._lock:
            if not self._pipeline_state:
                return {}
            
            # Count current status
            status_counts = {}
            for status in ExtractionStatus:
                status_counts[status.value] = 0
            
            for state in self._extraction_states.values():
                status_counts[state.status.value] += 1
            
            return {
                "pipeline_id": self._pipeline_state.pipeline_id,
                "run_id": self._pipeline_state.run_id,
                "status": self._pipeline_state.status.value,
                "start_time": self._serialize_datetime(self._pipeline_state.start_time),
                "end_time": self._serialize_datetime(self._pipeline_state.end_time),
                "duration_seconds": self._pipeline_state.duration_seconds,
                "total_tables": self._pipeline_state.total_tables,
                "completed_tables": self._pipeline_state.completed_tables,
                "failed_tables": self._pipeline_state.failed_tables,
                "skipped_tables": self._pipeline_state.skipped_tables,
                "completion_rate": self._pipeline_state.completion_rate,
                "restart_count": self._pipeline_state.restart_count,
                "last_checkpoint": self._serialize_datetime(self._pipeline_state.last_checkpoint),
                "status_breakdown": status_counts
            }
    
    def finish_pipeline(self, success: bool = True) -> None:
        """
        Mark pipeline as finished.
        
        Args:
            success: Whether pipeline completed successfully
        """
        with self._lock:
            if not self._pipeline_state:
                return
            
            self._pipeline_state.end_time = datetime.now()
            
            if success:
                self._pipeline_state.status = PipelineStatus.COMPLETED
            else:
                self._pipeline_state.status = PipelineStatus.FAILED
            
            # Cancel checkpoint timer
            if self._checkpoint_timer:
                self._checkpoint_timer.cancel()
                self._checkpoint_timer = None
            
            # Final save
            self._save_state_to_file(self._pipeline_state, self._extraction_states)
            
            self.logger.info(f"Pipeline {self._pipeline_state.pipeline_id} finished with status: {self._pipeline_state.status.value}")
    
    def cleanup_old_state_files(self) -> int:
        """
        Clean up old state files.
        
        Returns:
            int: Number of files cleaned up
        """
        cutoff_date = datetime.now() - timedelta(days=self.cleanup_after_days)
        cleaned_count = 0
        
        for state_file in self.state_dir.glob("pipeline_*.json"):
            try:
                file_mtime = datetime.fromtimestamp(state_file.stat().st_mtime)
                if file_mtime < cutoff_date:
                    state_file.unlink()
                    cleaned_count += 1
                    self.logger.debug(f"Cleaned up old state file: {state_file}")
            except Exception as e:
                self.logger.error(f"Failed to clean up {state_file}: {e}")
        
        if cleaned_count > 0:
            self.logger.info(f"Cleaned up {cleaned_count} old state files")
        
        return cleaned_count
    
    def get_extraction_state(self, table_key: str) -> Optional[ExtractionState]:
        """Get extraction state for a specific table."""
        with self._lock:
            return self._extraction_states.get(table_key)
    
    def list_recent_pipelines(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List recent pipeline runs.
        
        Args:
            limit: Maximum number of pipelines to return
            
        Returns:
            List of pipeline information dictionaries
        """
        pipelines = []
        
        for state_file in sorted(self.state_dir.glob("pipeline_*.json"), key=lambda f: f.stat().st_mtime, reverse=True):
            if len(pipelines) >= limit:
                break
                
            try:
                pipeline_state, _ = self._load_state_from_file(
                    state_file.stem.replace("pipeline_", "")
                )
                if pipeline_state:
                    pipelines.append({
                        "pipeline_id": pipeline_state.pipeline_id,
                        "run_id": pipeline_state.run_id,
                        "status": pipeline_state.status.value,
                        "start_time": self._serialize_datetime(pipeline_state.start_time),
                        "end_time": self._serialize_datetime(pipeline_state.end_time),
                        "duration_seconds": pipeline_state.duration_seconds,
                        "total_tables": pipeline_state.total_tables,
                        "completed_tables": pipeline_state.completed_tables,
                        "failed_tables": pipeline_state.failed_tables,
                        "completion_rate": pipeline_state.completion_rate,
                        "restart_count": pipeline_state.restart_count
                    })
            except Exception as e:
                self.logger.error(f"Failed to load pipeline info from {state_file}: {e}")
        
        return pipelines
    
    def set_extraction_window(self, extraction_date: datetime, window_hours: int = 24) -> None:
        """
        Set the extraction window for the current pipeline.
        
        Args:
            extraction_date: Base date for extraction
            window_hours: Size of extraction window in hours (default: 24)
        """
        with self._lock:
            if self._pipeline_state:
                self._pipeline_state.extraction_date = extraction_date
                # Store window information in metadata
                if not hasattr(self._pipeline_state, 'window_hours'):
                    # Add window_hours as dynamic attribute for tracking
                    setattr(self._pipeline_state, 'window_hours', window_hours)
                
                self.logger.info(f"Set extraction window: {extraction_date} ({window_hours}h window)")
    
    def get_extraction_window(self) -> tuple[Optional[datetime], Optional[datetime]]:
        """
        Get the current extraction window (start and end times).
        
        Returns:
            Tuple of (start_time, end_time) for the extraction window
        """
        with self._lock:
            if not self._pipeline_state or not self._pipeline_state.extraction_date:
                return None, None
            
            extraction_date = self._pipeline_state.extraction_date
            if isinstance(extraction_date, str):
                extraction_date = datetime.fromisoformat(extraction_date.replace('Z', '+00:00'))
            
            # Default 24-hour window from start of day
            start_date = extraction_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            
            return start_date, end_date
    
    def validate_extraction_window_consistency(self) -> Dict[str, Any]:
        """
        Validate that all extractions are using consistent time windows.
        
        Returns:
            Dict with validation results and any inconsistencies found
        """
        with self._lock:
            validation_result: Dict[str, Any] = {
                "is_consistent": True,
                "pipeline_window": None,
                "inconsistent_extractions": [],
                "total_extractions": len(self._extraction_states),
                "checked_extractions": 0
            }
            
            start_time, end_time = self.get_extraction_window()
            if start_time and end_time:
                validation_result["pipeline_window"] = {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat()
                }
            
            for table_key, state in self._extraction_states.items():
                if not state.extraction_date:
                    continue
                
                validation_result["checked_extractions"] += 1
                
                state_date = state.extraction_date
                if isinstance(state_date, str):
                    state_date = datetime.fromisoformat(state_date.replace('Z', '+00:00'))
                
                # Check if extraction date falls within the expected window
                if start_time and end_time and isinstance(state_date, datetime):
                    if not (start_time <= state_date < end_time):
                        validation_result["is_consistent"] = False
                        validation_result["inconsistent_extractions"].append({
                            "table_key": table_key,
                            "extraction_date": state_date.isoformat(),
                            "expected_window": f"{start_time.isoformat()} to {end_time.isoformat()}"
                        })
            
            return validation_result
    
    def create_extraction_report(self) -> Dict[str, Any]:
        """
        Create a comprehensive extraction report.
        
        Returns:
            Dict containing detailed extraction report
        """
        with self._lock:
            pipeline_progress = self.get_pipeline_progress()
            extraction_summary = self.get_extraction_summary()
            window_validation = self.validate_extraction_window_consistency()
            
            recommendations = []
            
            # Add recommendations based on analysis
            if not window_validation["is_consistent"]:
                recommendations.append({
                    "type": "window_inconsistency",
                    "message": f"Found {len(window_validation['inconsistent_extractions'])} extractions with inconsistent time windows",
                    "action": "Review extraction configurations and ensure consistent time windows"
                })
            
            failed_count = extraction_summary["by_status"].get("failed", 0)
            if failed_count > 0:
                recommendations.append({
                    "type": "failed_extractions",
                    "message": f"Found {failed_count} failed extractions",
                    "action": "Review failed extractions and consider using reset_failed_extractions() to retry"
                })
            
            pending_count = extraction_summary["by_status"].get("pending", 0)
            if pending_count > 0:
                recommendations.append({
                    "type": "pending_extractions",
                    "message": f"Found {pending_count} pending extractions",
                    "action": "Continue pipeline execution to process pending extractions"
                })
            
            report: Dict[str, Any] = {
                "pipeline_info": pipeline_progress,
                "extraction_summary": extraction_summary,
                "window_validation": window_validation,
                "recommendations": recommendations
            }
            
            return report

    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        # Cancel checkpoint timer
        if self._checkpoint_timer:
            self._checkpoint_timer.cancel()
        
        # Final checkpoint
        if self._pipeline_state and not self._pipeline_state.is_complete:
            try:
                self.checkpoint()
            except Exception as e:
                self.logger.error(f"Failed to save final checkpoint: {e}")
