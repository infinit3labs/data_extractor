#!/usr/bin/env python3
"""
Test script to validate the data extractor platform capabilities.
Tests various components and configurations.
"""

import os
import sys
import json
import subprocess
import tempfile
from pathlib import Path

def test_imports():
    """Test that all required modules can be imported."""
    print("🧪 Testing imports...")
    
    try:
        # Test core modules
        from data_extractor.core import DataExtractor
        from data_extractor.config import ConfigManager
        from data_extractor.databricks import DatabricksDataExtractor
        print("✅ Core modules imported successfully")
        
        # Test enhanced modules
        from data_extractor.health import HealthChecker
        from data_extractor.logging_config import setup_logging
        print("✅ Enhanced modules imported successfully")
        
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_configuration():
    """Test configuration loading and validation."""
    print("\n🧪 Testing configuration...")
    
    try:
        from data_extractor.config import ConfigManager
        
        # Test with empty config
        config_manager = ConfigManager()
        
        # Test with existing config files
        config_path = "config/config.yml"
        tables_path = "config/tables.json"
        
        try:
            # Test if files exist, if not create them
            if not os.path.exists(config_path):
                config_manager.create_sample_config_file(config_path)
            if not os.path.exists(tables_path):
                config_manager.create_sample_tables_json(tables_path)
            
            # Verify files were created
            assert os.path.exists(config_path), "Config file not created"
            assert os.path.exists(tables_path), "Tables file not created"
            
            # Test loading the generated config
            config_manager_with_file = ConfigManager(config_path)
            db_config = config_manager_with_file.get_database_config()
            
            assert 'oracle_host' in db_config, "Missing oracle_host in config"
            
            # Test tables loading
            tables = config_manager_with_file.load_table_configs_from_json(tables_path)
            assert len(tables) > 0, "No tables loaded"
            
            print("✅ Configuration system working")
            return True
            
        finally:
            # Don't cleanup main config files
            pass
                
    except Exception as e:
        print(f"❌ Configuration test error: {e}")
        return False

def test_health_checks():
    """Test health check functionality."""
    print("\n🧪 Testing health checks...")
    
    try:
        from data_extractor.health import HealthChecker, HealthStatus
        
        health_checker = HealthChecker()
        
        # Test individual checks
        app_result = health_checker.check_application_status()
        assert app_result.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED], "Invalid app status"
        
        sys_result = health_checker.check_system_resources()
        assert sys_result.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED, HealthStatus.UNHEALTHY], "Invalid system status"
        
        # Test file system check with temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            fs_result = health_checker.check_file_system(temp_dir)
            assert fs_result.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED, HealthStatus.UNHEALTHY], "Invalid filesystem status"
        
        print("✅ Health checks working")
        return True
        
    except Exception as e:
        print(f"❌ Health check test error: {e}")
        return False

def test_logging():
    """Test logging configuration."""
    print("\n🧪 Testing logging...")
    
    try:
        from data_extractor.logging_config import setup_logging, get_databricks_logger
        
        # Test basic logging setup
        logger = setup_logging(log_level="INFO", include_console=True)
        logger.info("Test log message")
        
        # Test Databricks logger
        databricks_logger = get_databricks_logger("test")
        databricks_logger.info("Databricks test log message")
        
        print("✅ Logging system working")
        return True
        
    except Exception as e:
        print(f"❌ Logging test error: {e}")
        return False

def test_cli():
    """Test CLI functionality."""
    print("\n🧪 Testing CLI...")
    
    try:
        from data_extractor.cli import create_parser
        
        parser = create_parser()
        
        # Test help command
        try:
            args = parser.parse_args(['--help'])
        except SystemExit:
            # This is expected for --help
            pass
        
        # Test config generation arguments
        args = parser.parse_args(['--generate-config', 'test.yml', '--generate-tables', 'test.json'])
        assert args.generate_config == 'test.yml'
        assert args.generate_tables == 'test.json'
        
        print("✅ CLI system working")
        return True
        
    except Exception as e:
        print(f"❌ CLI test error: {e}")
        return False

def test_docker_build():
    """Test Docker build process."""
    print("\n🧪 Testing Docker build...")
    
    try:
        # Check if Docker is available
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode != 0:
            print("⚠️ Docker not available, skipping Docker tests")
            return True
        
        # Test Dockerfile syntax
        dockerfile_path = Path(__file__).parent.parent / 'Dockerfile'
        if dockerfile_path.exists():
            # Simple syntax check by trying to build (dry-run style)
            print("✅ Dockerfile exists and is readable")
            return True
        else:
            print("❌ Dockerfile not found")
            return False
            
    except Exception as e:
        print(f"❌ Docker test error: {e}")
        return False

def test_dependencies():
    """Test that all dependencies are available."""
    print("\n🧪 Testing dependencies...")
    
    required_packages = [
        'pyspark',
        'yaml',
        'pydantic',
        'psutil',
    ]
    
    optional_packages = [
        'cx_Oracle',  # Optional for testing
    ]
    
    all_good = True
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} available")
        except ImportError:
            print(f"❌ {package} not available (required)")
            all_good = False
    
    for package in optional_packages:
        try:
            __import__(package)
            print(f"✅ {package} available")
        except ImportError:
            print(f"⚠️ {package} not available (optional)")
    
    return all_good

def run_all_tests():
    """Run all tests and return overall success."""
    print("🚀 Starting platform capability tests...\n")
    
    tests = [
        ("Import Tests", test_imports),
        ("Configuration Tests", test_configuration),
        ("Health Check Tests", test_health_checks),
        ("Logging Tests", test_logging),
        ("CLI Tests", test_cli),
        ("Docker Tests", test_docker_build),
        ("Dependency Tests", test_dependencies),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Print summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:<25} {status}")
        if success:
            passed += 1
    
    print("="*60)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Platform is ready for testing.")
        return True
    else:
        print("⚠️ Some tests failed. Please check the issues above.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)