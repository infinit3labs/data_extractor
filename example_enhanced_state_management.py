#!/usr/bin/env python3
"""
Comprehensive usage example for enhanced state management.

This example demonstrates:
1. Complete pipeline with state management
2. Idempotent operations 
3. Restart capabilities
4. 24-hour window processing
5. Error handling and recovery
6. Progress monitoring and reporting
"""

import os
import tempfile
import time
from pathlib import Path

from data_extractor.stateful_core import StatefulDataExtractor


def main():
    """Demonstrate enhanced state management in a realistic scenario."""
    
    print("🚀 Enhanced State Management - Complete Example")
    print("=" * 60)
    
    # Use temporary directory for this demo
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = os.path.join(temp_dir, "data")
        state_dir = os.path.join(temp_dir, "state")
        
        # Initialize stateful extractor
        extractor = StatefulDataExtractor(
            oracle_host="localhost",
            oracle_port="1521", 
            oracle_service="XE",
            oracle_user="demo_user",
            oracle_password="demo_password",
            output_base_path=output_dir,
            max_workers=4,
            state_dir=state_dir,
            enable_checkpoints=True,
            max_retry_attempts=3
        )
        
        # Define table configurations for different scenarios
        table_configs = [
            {
                "source_name": "sales_db",
                "table_name": "transactions",
                "schema_name": "public",
                "incremental_column": "created_at",
                "is_full_extract": False,
                "run_id": "daily_extract_20250123"
            },
            {
                "source_name": "sales_db", 
                "table_name": "customers",
                "schema_name": "public",
                "incremental_column": "last_modified",
                "is_full_extract": False,
                "run_id": "daily_extract_20250123"
            },
            {
                "source_name": "inventory_db",
                "table_name": "products",
                "schema_name": "catalog",
                "incremental_column": None,
                "is_full_extract": True,
                "run_id": "daily_extract_20250123"
            },
            {
                "source_name": "hr_db",
                "table_name": "employees", 
                "schema_name": "core",
                "incremental_column": "updated_at",
                "is_full_extract": False,
                "run_id": "daily_extract_20250123"
            }
        ]
        
        # Scenario 1: Initial pipeline run
        print("\n📋 Scenario 1: Initial Pipeline Run")
        print("-" * 40)
        
        print("🚀 Starting initial extraction pipeline...")
        
        # This would normally extract from Oracle, but we'll simulate the process
        results = simulate_extraction_with_state_management(extractor, table_configs)
        
        print(f"✅ Initial run completed: {sum(results.values())}/{len(results)} tables successful")
        
        # Show pipeline status
        status = extractor.get_pipeline_status()
        if status:
            print(f"📊 Pipeline Status: {status.get('status', 'Unknown')}")
            print(f"📊 Completion Rate: {status.get('completion_rate', 0):.1f}%")
        else:
            print("📊 No active pipeline found")
        
        # Scenario 2: Demonstrate idempotency (second run)
        print("\n📋 Scenario 2: Idempotent Second Run")
        print("-" * 40)
        
        print("🔄 Running pipeline again (should skip completed tables)...")
        
        # This should skip already completed tables
        results2 = simulate_extraction_with_state_management(extractor, table_configs)
        
        print(f"✅ Second run completed: {sum(results2.values())}/{len(results2)} tables processed")
        
        # Show extraction summary
        summary = extractor.get_extraction_summary()
        print(f"📈 Total extractions tracked: {summary['total_extractions']}")
        print(f"📈 Completed: {summary['by_status'].get('completed', 0)}")
        print(f"📈 Skipped: {summary['by_status'].get('skipped', 0)}")
        
        # Scenario 3: Force reprocessing
        print("\n📋 Scenario 3: Force Reprocessing")
        print("-" * 40)
        
        table_key = "sales_db.public.transactions"
        print(f"🔄 Forcing reprocessing of: {table_key}")
        
        success = extractor.force_reprocess_table(table_key)
        if success:
            print("✅ Table marked for reprocessing")
            
            # Run again to process the forced table
            results3 = simulate_extraction_with_state_management(extractor, table_configs[:1])
            print(f"✅ Forced reprocessing completed: {results3}")
        
        # Scenario 4: Simulate failures and recovery
        print("\n📋 Scenario 4: Failure Simulation and Recovery")
        print("-" * 40)
        
        # Simulate some failures
        print("❌ Simulating extraction failures...")
        simulate_extraction_failures(extractor)
        
        # Show failed extractions
        summary = extractor.get_extraction_summary()
        failed_count = len(summary['failed_tables'])
        print(f"❌ Failed extractions: {failed_count}")
        
        if failed_count > 0:
            print("🔄 Resetting failed extractions for retry...")
            reset_count = extractor.reset_failed_extractions()
            print(f"✅ Reset {reset_count} failed extractions")
        
        # Scenario 5: Window validation 
        print("\n📋 Scenario 5: 24-Hour Window Validation")
        print("-" * 40)
        
        validation = extractor.validate_extraction_window_consistency()
        print(f"📅 Window consistency: {validation['is_consistent']}")
        print(f"📅 Checked extractions: {validation['checked_extractions']}")
        
        if not validation['is_consistent']:
            print("⚠️  Window inconsistencies found:")
            for inconsistency in validation['inconsistent_extractions']:
                print(f"   - {inconsistency['table_key']}: {inconsistency['extraction_date']}")
        
        # Scenario 6: Comprehensive reporting
        print("\n📋 Scenario 6: Comprehensive Reporting")
        print("-" * 40)
        
        report = extractor.create_extraction_report()
        
        print("📋 Extraction Report Generated:")
        pipeline_info = report.get('pipeline_info', {})
        print(f"   Pipeline Status: {pipeline_info.get('status', 'Unknown')}")
        print(f"   Total Tables: {report.get('extraction_summary', {}).get('total_extractions', 0)}")
        print(f"   Completion Rate: {pipeline_info.get('completion_rate', 0):.1f}%")
        
        if report['recommendations']:
            print("\n💡 Recommendations:")
            for rec in report['recommendations']:
                print(f"   - {rec['type']}: {rec['message']}")
        
        # Scenario 7: Restart simulation
        print("\n📋 Scenario 7: Pipeline Restart Simulation")
        print("-" * 40)
        
        # Create new extractor instance (simulating application restart)
        print("🔄 Simulating application restart...")
        
        extractor2 = StatefulDataExtractor(
            oracle_host="localhost",
            oracle_port="1521",
            oracle_service="XE", 
            oracle_user="demo_user",
            oracle_password="demo_password",
            output_base_path=output_dir,
            state_dir=state_dir,
            enable_checkpoints=True
        )
        
        # Resume pipeline
        print("🚀 Resuming pipeline from state...")
        _ = simulate_extraction_with_state_management(extractor2, table_configs)
        
        status_resumed = extractor2.get_pipeline_status()
        if status_resumed:
            print(f"✅ Resumed pipeline - Restart count: {status_resumed.get('restart_count', 0)}")
        else:
            print("✅ No active pipeline to resume")
        
        # Scenario 8: Historical runs
        print("\n📋 Scenario 8: Historical Pipeline Runs")
        print("-" * 40)
        
        recent_runs = extractor2.list_recent_runs(limit=5)
        print(f"📋 Found {len(recent_runs)} recent pipeline runs:")
        
        for run in recent_runs:
            print(f"   - {run['run_id']}: {run['status']} ({run['completion_rate']:.1f}%)")
        
        # Scenario 9: Cleanup
        print("\n📋 Scenario 9: State Cleanup")
        print("-" * 40)
        
        cleanup_count = extractor2.cleanup_old_state()
        print(f"🧹 Cleaned up {cleanup_count} old state files")
        
        print("\n🎉 Enhanced State Management Demo Completed!")
        print("=" * 60)
        
        # Final summary
        final_report = extractor2.create_extraction_report()
        print("\n📊 Final Summary:")
        print(f"   Total Pipelines: {len(recent_runs)}")
        pipeline_info = final_report.get('pipeline_info', {})
        print(f"   Current Status: {pipeline_info.get('status', 'Unknown')}")
        print(f"   Success Rate: {pipeline_info.get('completion_rate', 0):.1f}%")


def simulate_extraction_with_state_management(extractor, table_configs):
    """
    Simulate the extraction process with state management.
    
    In a real scenario, this would call extractor.extract_tables_parallel()
    """
    print(f"🔄 Processing {len(table_configs)} table configurations...")
    
    # Create mock output files for successful extractions
    results = {}
    
    for config in table_configs:
        table_name = config["table_name"]
        
        # Simulate creating output file
        output_path = os.path.join(
            extractor.output_base_path,
            config["source_name"],
            table_name,
            "202501",
            "23",
            f"{config.get('run_id', 'default')}.parquet"
        )
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(f"mock parquet data for {table_name}")
        
        # Check if extraction is needed (idempotent check)
        table_key = f"{config['source_name']}.{config.get('schema_name', '')}.{table_name}"
        
        if extractor.state_manager.is_extraction_needed(table_key):
            print(f"   ✅ Processing: {table_name}")
            
            # Start extraction
            if extractor.state_manager.start_extraction(table_key, "main-thread"):
                
                # Simulate processing time
                time.sleep(0.1)
                
                # Complete extraction
                extractor.state_manager.complete_extraction(
                    table_key=table_key,
                    success=True,
                    record_count=1000 + hash(table_name) % 10000,
                    output_path=output_path,
                    file_size_bytes=Path(output_path).stat().st_size
                )
                
                results[table_name] = True
            else:
                results[table_name] = False
        else:
            print(f"   ⏭️  Skipping (already completed): {table_name}")
            results[table_name] = True
    
    return results


def simulate_extraction_failures(extractor):
    """Simulate some extraction failures for testing recovery."""
    
    # Create a table that will "fail"
    table_key = "hr_db.core.employees"
    
    if extractor.state_manager.start_extraction(table_key, "failure-thread"):
        # Simulate failure
        extractor.state_manager.complete_extraction(
            table_key=table_key,
            success=False,
            error_message="Simulated connection timeout"
        )
        print(f"   ❌ Simulated failure for: {table_key}")


if __name__ == "__main__":
    main()
