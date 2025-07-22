"""
Health check functionality for monitoring application status and dependencies.
Provides comprehensive health checks for database connections, file system, and system resources.
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import psutil

try:
    import cx_Oracle

    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False


class HealthStatus(Enum):
    """Health check status enumeration."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""

    name: str
    status: HealthStatus
    message: str
    duration_ms: float
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None


class HealthChecker:
    """
    Comprehensive health checker for data extractor application.
    Monitors database connectivity, system resources, and file system health.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize health checker.

        Args:
            logger: Logger instance for health check logging
        """
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = datetime.now()

    def check_database_connection(
        self,
        host: str,
        port: int,
        service: str,
        user: str,
        password: str,
        timeout: int = 5,
    ) -> HealthCheckResult:
        """
        Check Oracle database connectivity.

        Args:
            host: Database host
            port: Database port
            service: Database service name
            user: Database username
            password: Database password
            timeout: Connection timeout in seconds

        Returns:
            HealthCheckResult with connection status
        """
        start_time = time.time()

        if not ORACLE_AVAILABLE:
            return HealthCheckResult(
                name="database_connection",
                status=HealthStatus.UNHEALTHY,
                message="cx_Oracle library not available",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={"error": "Missing cx_Oracle dependency"},
            )

        try:
            # Create connection string
            dsn = cx_Oracle.makedsn(host, port, service_name=service)

            # Attempt connection with timeout
            connection = cx_Oracle.connect(
                user=user, password=password, dsn=dsn, timeout=timeout
            )

            # Test with a simple query
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()

            cursor.close()
            connection.close()

            duration_ms = (time.time() - start_time) * 1000

            if result and result[0] == 1:
                return HealthCheckResult(
                    name="database_connection",
                    status=HealthStatus.HEALTHY,
                    message="Database connection successful",
                    duration_ms=duration_ms,
                    timestamp=datetime.now(),
                    details={
                        "host": host,
                        "port": port,
                        "service": service,
                        "response_time_ms": duration_ms,
                    },
                )
            else:
                return HealthCheckResult(
                    name="database_connection",
                    status=HealthStatus.UNHEALTHY,
                    message="Database query returned unexpected result",
                    duration_ms=duration_ms,
                    timestamp=datetime.now(),
                )

        except cx_Oracle.DatabaseError as e:
            (error_obj,) = e.args
            return HealthCheckResult(
                name="database_connection",
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {error_obj.message}",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={
                    "error_code": error_obj.code,
                    "error_message": error_obj.message,
                },
            )
        except Exception as e:
            return HealthCheckResult(
                name="database_connection",
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_file_system(self, path: str) -> HealthCheckResult:
        """
        Check file system accessibility and disk space.

        Args:
            path: Path to check

        Returns:
            HealthCheckResult with file system status
        """
        start_time = time.time()

        try:
            # Check if path exists and is accessible
            if not os.path.exists(path):
                # Try to create the directory
                os.makedirs(path, exist_ok=True)

            # Check write permissions
            test_file = os.path.join(path, f".health_check_{int(time.time())}")
            with open(test_file, "w") as f:
                f.write("health_check")

            # Clean up test file
            os.remove(test_file)

            # Get disk usage
            usage = psutil.disk_usage(path)
            free_space_gb = usage.free / (1024**3)
            total_space_gb = usage.total / (1024**3)
            usage_percent = (usage.used / usage.total) * 100

            duration_ms = (time.time() - start_time) * 1000

            # Determine status based on available space
            if free_space_gb < 1:  # Less than 1GB free
                status = HealthStatus.UNHEALTHY
                message = f"Critical: Low disk space ({free_space_gb:.2f} GB free)"
            elif free_space_gb < 5:  # Less than 5GB free
                status = HealthStatus.DEGRADED
                message = f"Warning: Low disk space ({free_space_gb:.2f} GB free)"
            else:
                status = HealthStatus.HEALTHY
                message = f"File system accessible ({free_space_gb:.2f} GB free)"

            return HealthCheckResult(
                name="file_system",
                status=status,
                message=message,
                duration_ms=duration_ms,
                timestamp=datetime.now(),
                details={
                    "path": path,
                    "free_space_gb": round(free_space_gb, 2),
                    "total_space_gb": round(total_space_gb, 2),
                    "usage_percent": round(usage_percent, 2),
                },
            )

        except PermissionError:
            return HealthCheckResult(
                name="file_system",
                status=HealthStatus.UNHEALTHY,
                message=f"Permission denied accessing path: {path}",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={"error": "Permission denied"},
            )
        except Exception as e:
            return HealthCheckResult(
                name="file_system",
                status=HealthStatus.UNHEALTHY,
                message=f"File system check failed: {str(e)}",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_system_resources(self) -> HealthCheckResult:
        """
        Check system resource utilization (CPU, memory, disk I/O).

        Returns:
            HealthCheckResult with system resource status
        """
        start_time = time.time()

        try:
            # Get system metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent

            # Get load average (Unix-like systems)
            try:
                load_avg = os.getloadavg()
                load_avg_5min = load_avg[1]
            except (OSError, AttributeError):
                # Windows doesn't support getloadavg
                load_avg_5min = cpu_percent / 100.0

            duration_ms = (time.time() - start_time) * 1000

            # Determine status based on resource utilization
            cpu_count = psutil.cpu_count() or 1
            if cpu_percent > 90 or memory_percent > 90 or load_avg_5min > cpu_count:
                status = HealthStatus.UNHEALTHY
                message = "Critical: High resource utilization"
            elif (
                cpu_percent > 70
                or memory_percent > 70
                or load_avg_5min > cpu_count * 0.7
            ):
                status = HealthStatus.DEGRADED
                message = "Warning: Elevated resource utilization"
            else:
                status = HealthStatus.HEALTHY
                message = "System resources within normal limits"

            return HealthCheckResult(
                name="system_resources",
                status=status,
                message=message,
                duration_ms=duration_ms,
                timestamp=datetime.now(),
                details={
                    "cpu_percent": round(cpu_percent, 2),
                    "memory_percent": round(memory_percent, 2),
                    "memory_available_gb": round(memory.available / (1024**3), 2),
                    "load_avg_5min": round(load_avg_5min, 2),
                    "cpu_count": psutil.cpu_count(),
                },
            )

        except Exception as e:
            return HealthCheckResult(
                name="system_resources",
                status=HealthStatus.UNHEALTHY,
                message=f"System resource check failed: {str(e)}",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_application_status(self) -> HealthCheckResult:
        """
        Check general application health and uptime.

        Returns:
            HealthCheckResult with application status
        """
        start_time = time.time()

        try:
            uptime = datetime.now() - self.start_time
            uptime_seconds = uptime.total_seconds()

            # Check if application has been running for a reasonable time
            if uptime_seconds < 10:  # Less than 10 seconds
                status = HealthStatus.DEGRADED
                message = "Application recently started"
            else:
                status = HealthStatus.HEALTHY
                message = f"Application healthy (uptime: {self._format_uptime(uptime)})"

            return HealthCheckResult(
                name="application_status",
                status=status,
                message=message,
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={
                    "start_time": self.start_time.isoformat(),
                    "uptime_seconds": round(uptime_seconds, 2),
                    "uptime_formatted": self._format_uptime(uptime),
                    "python_version": f"{__import__('sys').version_info.major}.{__import__('sys').version_info.minor}.{__import__('sys').version_info.micro}",
                    "pid": os.getpid(),
                },
            )

        except Exception as e:
            return HealthCheckResult(
                name="application_status",
                status=HealthStatus.UNHEALTHY,
                message=f"Application status check failed: {str(e)}",
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def run_all_checks(
        self,
        db_config: Optional[Dict[str, str]] = None,
        output_path: Optional[str] = None,
    ) -> Tuple[Dict[str, HealthCheckResult], HealthStatus]:
        """
        Run all health checks and return comprehensive status.

        Args:
            db_config: Database configuration for connection check
            output_path: Output path for file system check

        Returns:
            Tuple of (health check results, overall status)
        """
        results = {}

        # Always check application status and system resources
        results["application"] = self.check_application_status()
        results["system_resources"] = self.check_system_resources()

        # Check database connection if config provided
        if db_config:
            results["database"] = self.check_database_connection(
                host=db_config.get("oracle_host", ""),
                port=int(db_config.get("oracle_port", "1521")),
                service=db_config.get("oracle_service", ""),
                user=db_config.get("oracle_user", ""),
                password=db_config.get("oracle_password", ""),
            )

        # Check file system if path provided
        if output_path:
            results["file_system"] = self.check_file_system(output_path)

        # Determine overall status
        overall_status = self._calculate_overall_status(results)

        # Log results
        self._log_health_results(results, overall_status)

        return results, overall_status

    def _format_uptime(self, uptime: timedelta) -> str:
        """Format uptime as human-readable string."""
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m {seconds}s"

    def _calculate_overall_status(
        self, results: Dict[str, HealthCheckResult]
    ) -> HealthStatus:
        """Calculate overall status from individual check results."""
        statuses = [result.status for result in results.values()]

        if any(status == HealthStatus.UNHEALTHY for status in statuses):
            return HealthStatus.UNHEALTHY
        elif any(status == HealthStatus.DEGRADED for status in statuses):
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    def _log_health_results(
        self, results: Dict[str, HealthCheckResult], overall_status: HealthStatus
    ):
        """Log health check results."""
        self.logger.info(
            f"Health check completed - Overall status: {overall_status.value}"
        )

        for name, result in results.items():
            log_level = logging.INFO
            if result.status == HealthStatus.UNHEALTHY:
                log_level = logging.ERROR
            elif result.status == HealthStatus.DEGRADED:
                log_level = logging.WARNING

            self.logger.log(
                log_level,
                f"Health check '{name}': {result.status.value} - {result.message} ({result.duration_ms:.2f}ms)",
            )


def create_health_check_endpoint(
    health_checker: HealthChecker, **kwargs
) -> Dict[str, Any]:
    """
    Create a health check response suitable for HTTP endpoints.

    Args:
        health_checker: HealthChecker instance
        **kwargs: Additional configuration for health checks

    Returns:
        Dict containing health check response
    """
    results, overall_status = health_checker.run_all_checks(**kwargs)

    return {
        "status": overall_status.value,
        "timestamp": datetime.now().isoformat(),
        "checks": {
            name: {
                "status": result.status.value,
                "message": result.message,
                "duration_ms": result.duration_ms,
                "timestamp": result.timestamp.isoformat(),
                "details": result.details,
            }
            for name, result in results.items()
        },
    }
