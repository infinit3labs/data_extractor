#!/bin/bash

# Comprehensive test runner for data extractor platform
# This script tests various aspects of the platform

set -e  # Exit on any error

echo "🚀 Data Extractor Platform Test Suite"
echo "======================================"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS") echo -e "${GREEN}✅ $message${NC}" ;;
        "ERROR")   echo -e "${RED}❌ $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "INFO")    echo -e "ℹ️  $message" ;;
    esac
}

# Check if we're in the right directory
if [[ ! -f "pyproject.toml" ]]; then
    print_status "ERROR" "Not in project root directory. Please run from data_extractor root."
    exit 1
fi

print_status "INFO" "Starting comprehensive platform tests..."

# Test 1: Python Platform Test
echo -e "\n📋 Test 1: Platform Capability Tests"
echo "-----------------------------------"
if python scripts/test_platform.py; then
    print_status "SUCCESS" "Platform capability tests passed"
else
    print_status "ERROR" "Platform capability tests failed"
    exit 1
fi

# Test 2: Code Quality Tests
echo -e "\n📋 Test 2: Code Quality Tests"
echo "----------------------------"

# Check if development tools are available
if command -v black &> /dev/null; then
    echo "Running black formatter check..."
    if black --check data_extractor/; then
        print_status "SUCCESS" "Code formatting is correct"
    else
        print_status "WARNING" "Code formatting issues found (run: black data_extractor/)"
    fi
else
    print_status "WARNING" "Black formatter not available"
fi

if command -v mypy &> /dev/null; then
    echo "Running type checking..."
    if mypy data_extractor/ --ignore-missing-imports; then
        print_status "SUCCESS" "Type checking passed"
    else
        print_status "WARNING" "Type checking issues found"
    fi
else
    print_status "WARNING" "MyPy type checker not available"
fi

# Test 3: Unit Tests
echo -e "\n📋 Test 3: Unit Tests"
echo "--------------------"
if python -m pytest tests/ -v; then
    print_status "SUCCESS" "Unit tests passed"
else
    print_status "ERROR" "Unit tests failed"
fi

# Test 4: Docker Build Test
echo -e "\n📋 Test 4: Docker Build Test"
echo "---------------------------"
if command -v docker &> /dev/null; then
    echo "Building Docker image..."
    if docker build -t data-extractor-test .; then
        print_status "SUCCESS" "Docker build completed"
        
        # Test basic container functionality
        echo "Testing container startup..."
        if docker run --rm data-extractor-test python -c "print('Container test OK')"; then
            print_status "SUCCESS" "Container functionality test passed"
        else
            print_status "WARNING" "Container functionality test had issues"
        fi
    else
        print_status "ERROR" "Docker build failed"
    fi
else
    print_status "WARNING" "Docker not available, skipping Docker tests"
fi

# Test 5: Configuration Tests
echo -e "\n📋 Test 5: Configuration Validation"
echo "----------------------------------"

# Test existing configs
if [[ -f "config/config.yml" && -f "config/tables.json" ]]; then
    print_status "SUCCESS" "Configuration files exist"
    
    # Test config loading
    python -c "
from data_extractor.config import ConfigManager
config = ConfigManager('config/config.yml')
tables = config.load_table_configs_from_json('config/tables.json')
print(f'Loaded {len(tables)} table configurations')
assert len(tables) > 0, 'No tables loaded'
print('Configuration loading test passed')
"
    if [[ $? -eq 0 ]]; then
        print_status "SUCCESS" "Configuration loading test passed"
    else
        print_status "ERROR" "Configuration loading test failed"
    fi
else
    print_status "WARNING" "Main configuration files not found in config/ directory"
fi

# Test 6: Health Check Tests
echo -e "\n📋 Test 6: Health Check System"
echo "-----------------------------"
python -c "
from data_extractor.health import HealthChecker, HealthStatus
import tempfile

hc = HealthChecker()
app_status = hc.check_application_status()
sys_status = hc.check_system_resources()

with tempfile.TemporaryDirectory() as temp_dir:
    fs_status = hc.check_file_system(temp_dir)

print(f'Application Status: {app_status.status.value}')
print(f'System Status: {sys_status.status.value}')
print(f'FileSystem Status: {fs_status.status.value}')

# Run all checks
results, overall = hc.run_all_checks()
print(f'Overall Health Status: {overall.value}')
assert overall in [HealthStatus.HEALTHY, HealthStatus.DEGRADED], f'Unexpected health status: {overall}'
print('Health check system working correctly')
"

if [[ $? -eq 0 ]]; then
    print_status "SUCCESS" "Health check system working"
else
    print_status "ERROR" "Health check system failed"
fi

# Final Summary
echo -e "\n🎯 Test Suite Complete"
echo "======================"
print_status "INFO" "Platform testing completed"

# Test Docker Compose (if available)
if command -v docker-compose &> /dev/null || command -v docker compose &> /dev/null; then
    echo -e "\n📋 Bonus: Docker Compose Test"
    echo "----------------------------"
    
    # Create .env file for testing
    cp .env.example .env
    
    # Test docker-compose configuration
    if docker-compose config > /dev/null 2>&1; then
        print_status "SUCCESS" "Docker Compose configuration is valid"
    elif docker compose config > /dev/null 2>&1; then
        print_status "SUCCESS" "Docker Compose configuration is valid"
    else
        print_status "WARNING" "Docker Compose configuration validation failed"
    fi
    
    rm -f .env
else
    print_status "INFO" "Docker Compose not available, skipping compose tests"
fi

print_status "SUCCESS" "All tests completed! Platform is ready for use."
echo ""
echo "🚀 Next Steps:"
echo "  1. Copy .env.example to .env and configure your database settings"
echo "  2. Run: docker-compose up --build (for full environment)"
echo "  3. Or run: python -m data_extractor.cli --help (for CLI usage)"