#!/usr/bin/env python3
"""
Test script to demonstrate enhanced state management capabilities.

This script demonstrates:
1. Idempotent operations
2. Restart capabilities 
3. 24-hour window processing
4. Progress tracking
5. Comprehensive reporting
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from data_extractor.state_manager import StateManager


def test_enhanced_state_management():
    """Test enhanced state management features."""
    
    print("🔄 Testing Enhanced State Management")
    print("=" * 50)
    
    # Create temporary state directory
    with tempfile.TemporaryDirectory() as temp_dir:
        state_manager = StateManager(
            state_dir=temp_dir,
            enable_checkpoints=True,
            checkpoint_interval_seconds=5,
            max_retry_attempts=3
        )
        
        # Test 1: Start a new pipeline
        print("\n1️⃣ Starting new pipeline...")
        
        table_configs = [
            {
                "source_name": "oracle_prod",
                "table_name": "customers",
                "schema_name": "sales",
                "incremental_column": "last_modified",
                "is_full_extract": False
            },
            {
                "source_name": "oracle_prod", 
                "table_name": "orders",
                "schema_name": "sales",
                "incremental_column": "order_date",
                "is_full_extract": False
            },
            {
                "source_name": "oracle_prod",
                "table_name": "products", 
                "schema_name": "inventory",
                "incremental_column": None,
                "is_full_extract": True
            }
        ]
        
        extraction_date = datetime.now() - timedelta(days=1)
        
        pipeline_id = state_manager.start_pipeline(
            table_configs=table_configs,
            run_id="test_run_20250123",
            extraction_date=extraction_date,
            output_base_path="/tmp/test_data",
            max_workers=4
        )
        
        print(f"✅ Started pipeline: {pipeline_id}")
        
        # Test 2: Set extraction window
        print("\n2️⃣ Setting extraction window...")
        state_manager.set_extraction_window(extraction_date, window_hours=24)
        
        start_time, end_time = state_manager.get_extraction_window()
        print(f"📅 Window: {start_time} to {end_time}")
        
        # Test 3: Check pending extractions
        print("\n3️⃣ Checking pending extractions...")
        pending = state_manager.get_pending_extractions()
        print(f"📝 Pending extractions: {len(pending)}")
        for table_key in pending[:3]:  # Show first 3
            print(f"   - {table_key}")
        
        # Test 4: Simulate some extractions
        print("\n4️⃣ Simulating extractions...")
        
        # Start extraction for customers table
        customers_key = "oracle_prod.sales.customers"
        if state_manager.start_extraction(customers_key, "thread-1"):
            print(f"✅ Started extraction: {customers_key}")
            
            # Simulate successful completion
            state_manager.complete_extraction(
                table_key=customers_key,
                success=True,
                record_count=15000,
                output_path="/tmp/test_data/customers.parquet",
                file_size_bytes=2048000,
                checksum="abc123def456"
            )
            print(f"✅ Completed extraction: {customers_key}")
        
        # Start extraction for orders table 
        orders_key = "oracle_prod.sales.orders"
        if state_manager.start_extraction(orders_key, "thread-2"):
            print(f"✅ Started extraction: {orders_key}")
            
            # Simulate failure
            state_manager.complete_extraction(
                table_key=orders_key,
                success=False,
                error_message="Connection timeout"
            )
            print(f"❌ Failed extraction: {orders_key}")
        
        # Test 5: Test idempotency
        print("\n5️⃣ Testing idempotency...")
        
        # Check if customers extraction is needed (should be False)
        is_needed = state_manager.is_extraction_needed(customers_key)
        print(f"🔍 Customers extraction needed: {is_needed}")
        
        # Check if orders extraction is needed (should be True for retry)
        is_needed = state_manager.is_extraction_needed(orders_key) 
        print(f"🔍 Orders extraction needed (for retry): {is_needed}")
        
        # Test 6: Force reprocessing
        print("\n6️⃣ Testing force reprocessing...")
        
        success = state_manager.force_reprocess_table(customers_key)
        print(f"🔄 Forced reprocessing customers: {success}")
        
        is_needed = state_manager.is_extraction_needed(customers_key)
        print(f"🔍 Customers extraction needed after force: {is_needed}")
        
        # Test 7: Progress tracking
        print("\n7️⃣ Pipeline progress...")
        
        progress = state_manager.get_pipeline_progress()
        print(f"📊 Pipeline Status: {progress['status']}")
        print(f"📊 Completion Rate: {progress['completion_rate']:.1f}%")
        print(f"📊 Completed Tables: {progress['completed_tables']}")
        print(f"📊 Failed Tables: {progress['failed_tables']}")
        
        # Test 8: Extraction summary
        print("\n8️⃣ Extraction summary...")
        
        summary = state_manager.get_extraction_summary()
        print(f"📈 Total Extractions: {summary['total_extractions']}")
        print(f"📈 Total Records: {summary['total_records_extracted']}")
        print("📈 Status Breakdown:")
        for status, count in summary['by_status'].items():
            if count > 0:
                print(f"   - {status}: {count}")
        
        # Test 9: Window validation
        print("\n9️⃣ Window validation...")
        
        validation = state_manager.validate_extraction_window_consistency()
        print(f"✅ Window Consistent: {validation['is_consistent']}")
        print(f"📊 Checked Extractions: {validation['checked_extractions']}")
        
        # Test 10: Reset failed extractions
        print("\n🔟 Resetting failed extractions...")
        
        reset_count = state_manager.reset_failed_extractions()
        print(f"🔄 Reset {reset_count} failed extractions")
        
        # Test 11: Comprehensive report
        print("\n1️⃣1️⃣ Comprehensive report...")
        
        report = state_manager.create_extraction_report()
        print(f"📋 Report generated with {len(report['recommendations'])} recommendations")
        
        if report['recommendations']:
            print("📋 Recommendations:")
            for rec in report['recommendations']:
                print(f"   - {rec['type']}: {rec['message']}")
        
        # Test 12: Restart simulation
        print("\n1️⃣2️⃣ Testing restart capabilities...")
        
        # Finish current pipeline
        state_manager.finish_pipeline(success=False)  # Mark as failed to test restart
        
        # Start new state manager (simulating restart)
        state_manager2 = StateManager(state_dir=temp_dir)
        
        # Resume pipeline with same run_id
        pipeline_id2 = state_manager2.start_pipeline(
            table_configs=table_configs,
            run_id="test_run_20250123",  # Same run_id
            extraction_date=extraction_date,
            resume_existing=True
        )
        
        print(f"🔄 Resumed pipeline: {pipeline_id2}")
        
        progress2 = state_manager2.get_pipeline_progress()
        print(f"📊 Restart Count: {progress2['restart_count']}")
        
        # Test 13: List recent pipelines
        print("\n1️⃣3️⃣ Recent pipelines...")
        
        recent = state_manager2.list_recent_pipelines(limit=5)
        print(f"📋 Found {len(recent)} recent pipelines")
        for pipeline in recent:
            print(f"   - {pipeline['run_id']}: {pipeline['status']} ({pipeline['completion_rate']:.1f}%)")
        
        # Test 14: Cleanup
        print("\n1️⃣4️⃣ Testing cleanup...")
        
        cleanup_count = state_manager2.cleanup_old_state_files()
        print(f"🧹 Cleaned up {cleanup_count} old files")
        
        print("\n✅ Enhanced state management test completed!")
        print("=" * 50)


def demonstrate_idempotent_usage():
    """Demonstrate idempotent usage patterns."""
    
    print("\n🔄 Demonstrating Idempotent Usage Patterns")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock output files to test idempotency
        output_dir = Path(temp_dir) / "mock_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a mock parquet file
        mock_file = output_dir / "customers.parquet"
        mock_file.write_text("mock parquet data")
        
        state_manager = StateManager(state_dir=temp_dir)
        
        table_configs = [{
            "source_name": "test_source",
            "table_name": "customers",
            "incremental_column": "last_modified"
        }]
        
        # First run
        print("\n1️⃣ First pipeline run...")
        _ = state_manager.start_pipeline(table_configs=table_configs)
        
        # Simulate successful extraction
        table_key = "test_source..customers"
        state_manager.start_extraction(table_key, "thread-1")
        state_manager.complete_extraction(
            table_key=table_key,
            success=True,
            record_count=1000,
            output_path=str(mock_file),
            file_size_bytes=mock_file.stat().st_size
        )
        
        print(f"✅ Completed first extraction: {table_key}")
        
        # Second run (should skip due to idempotency)
        print("\n2️⃣ Second pipeline run (idempotent check)...")
        
        is_needed = state_manager.is_extraction_needed(table_key)
        print(f"🔍 Extraction needed: {is_needed}")
        
        if not is_needed:
            print("✅ Idempotency working - extraction skipped!")
        
        # Third run with force reprocess
        print("\n3️⃣ Third run with force reprocess...")
        
        state_manager.force_reprocess_table(table_key)
        is_needed = state_manager.is_extraction_needed(table_key)
        print(f"🔍 Extraction needed after force: {is_needed}")
        
        # Fourth run after file deletion (should detect and re-extract)
        print("\n4️⃣ Fourth run after file deletion...")
        
        mock_file.unlink()  # Delete the file
        is_needed = state_manager.is_extraction_needed(table_key)
        print(f"🔍 Extraction needed after file deletion: {is_needed}")
        
        if is_needed:
            print("✅ File integrity check working - re-extraction triggered!")


def demonstrate_24hour_window():
    """Demonstrate 24-hour window processing."""
    
    print("\n📅 Demonstrating 24-Hour Window Processing")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        state_manager = StateManager(state_dir=temp_dir)
        
        # Set extraction window for yesterday
        yesterday = datetime.now() - timedelta(days=1)
        state_manager.set_extraction_window(yesterday, window_hours=24)
        
        start_time, end_time = state_manager.get_extraction_window()
        print(f"📅 Extraction Window: {start_time} to {end_time}")
        if start_time and end_time:
            print(f"📅 Window Duration: {(end_time - start_time).total_seconds() / 3600} hours")
        
        # Create test configurations for different scenarios
        table_configs = [
            {
                "source_name": "sales_db",
                "table_name": "transactions", 
                "incremental_column": "created_at"
            },
            {
                "source_name": "hr_db",
                "table_name": "employee_changes",
                "incremental_column": "modified_date"
            }
        ]
        
        pipeline_id = state_manager.start_pipeline(
            table_configs=table_configs,
            extraction_date=yesterday
        )
        
        print(f"✅ Started pipeline with 24h window: {pipeline_id}")
        
        # Validate window consistency
        validation = state_manager.validate_extraction_window_consistency()
        print(f"✅ Window validation: {validation}")
        
        if validation["is_consistent"]:
            print("✅ All extractions are using consistent 24-hour windows!")
        else:
            print("❌ Window inconsistencies detected!")
            for inconsistency in validation["inconsistent_extractions"]:
                print(f"   - {inconsistency}")


if __name__ == "__main__":
    # Run all tests
    test_enhanced_state_management()
    demonstrate_idempotent_usage()
    demonstrate_24hour_window()
    
    print("\n🎉 All enhanced state management tests completed successfully!")
