# Data Extractor Project Structure

This document describes the clean, organized project structure after reorganization.

## Directory Layout

```
data_extractor/
├── README.md                          # Main project documentation
├── pyproject.toml                     # Poetry configuration & tool settings
├── poetry.lock                        # Dependency lock file
├── Dockerfile                         # Container build configuration
├── docker-compose.yml                 # Multi-container setup
├── Makefile                          # Development workflow automation
├── .env.example                      # Environment variable template
├── PROJECT_STRUCTURE.md              # This file
├── ROADMAP.md                        # Development roadmap and future plans
│
├── config/                           # Configuration files
│   ├── config.yml                    # Main configuration
│   └── tables.json                   # Table extraction definitions
│
├── data_extractor/                   # Main package
│   ├── __init__.py                   # Package initialization & public API
│   ├── cli.py                        # Command-line interface
│   ├── config.py                     # Configuration management
│   ├── core.py                       # Core extraction engine
│   ├── databricks.py                 # Databricks integration
│   ├── databricks_job.py             # Databricks job runner
│   ├── health.py                     # Health monitoring
│   └── logging_config.py             # Enterprise logging setup
│
├── docs/                             # Documentation
│   ├── CONTRIBUTING.md              # Contribution guidelines aligned with roadmap
│   ├── DATABRICKS.md                 # Databricks usage guide
│   ├── DATABRICKS_IMPLEMENTATION.md  # Implementation details
│   ├── DEMO.md                       # Demo instructions
│   ├── GETTING_STARTED.md            # Quick start guide
│   ├── PERFORMANCE.md                # Performance optimization
│   ├── ROADMAP_QUICK_REFERENCE.md   # Quick roadmap overview
│   ├── UNITY_CATALOG.md              # Unity Catalog integration
│   └── development/                  # Development documentation
│       ├── CHECKLIST.md              # Development checklist
│       ├── CODE_CHANGE_CHECKLIST.md  # Code change process
│       ├── TESTING.md                # Testing guide
│       └── VALIDATION_SUMMARY.md     # Validation summary
│
├── examples/                         # Usage examples
│   ├── README.md                     # Examples documentation
│   ├── demo.py                       # Basic usage demo
│   ├── databricks_config.yml         # Databricks config example
│   ├── databricks_demo.py            # Databricks usage demo
│   ├── databricks_tables.json        # Databricks table definitions
│   ├── databricks_usage_examples.py  # Comprehensive examples
│   ├── unity_catalog_demo.py         # Unity Catalog demo
│   └── usage_examples.py             # General usage patterns
│
├── scripts/                          # Development & testing scripts
│   └── init-oracle.sql               # Oracle test database setup
│
├── tests/                            # Unit tests
│   ├── __init__.py
│   ├── test_core.py                  # Core functionality tests
│   ├── test_databricks.py            # Databricks tests
│   └── test_databricks_job.py        # Databricks job tests
│
├── data/                             # Output data directory (created at runtime)
└── logs/                             # Log files directory (created at runtime)
```

## Key Components

### Main Package (`data_extractor/`)
- **Clean API**: All public classes exported in `__init__.py`
- **Single Configuration System**: Unified YAML-based configuration in `config.py`
- **Enterprise Features**: Health monitoring, structured logging, security scanning

### Configuration (`config/`)
- **Main Config**: `config.yml` - single source of truth for all settings
- **Table Definitions**: `tables.json` - extraction table configurations
- **Environment Overrides**: All settings can be overridden via environment variables

### Documentation (`docs/`)
- **User Guides**: Main documentation for users
- **Development**: Separate folder for development-related docs
- **Examples**: Practical usage examples with explanations

### Development Tools
- **Poetry**: Modern dependency management with proper dev dependencies
- **Makefile**: Streamlined development workflow
- **Scripts**: Automated testing and validation
- **Docker**: Complete containerization with multi-stage builds

## Configuration Hierarchy

1. **Default Values**: In code
2. **Config File**: `config/config.yml`
3. **Environment Variables**: Override any config value
4. **Command Line**: Override specific parameters

## Development Workflow

```bash
# Setup development environment
make setup-env

# Run all quality checks
make ci

# Run comprehensive tests
make test-platform

# Build and test container
make docker-build && make docker-run

# Start full environment
make docker-compose-up
```

## Key Improvements Made

### ✅ Removed Redundancy
- Eliminated duplicate configuration files
- Consolidated settings system (removed redundant `settings.py`)
- Removed legacy files (`setup.py`, `requirements.txt`)

### ✅ Organized Structure
- Moved all development docs to `docs/development/`
- Consolidated all configuration in `config/` directory
- Clean separation between user docs and development docs

### ✅ Modern Dependencies
- Updated to Poetry group dependencies
- Added proper type hints with mypy support
- Organized dev dependencies by category

### ✅ Streamlined API
- Clean public API in `__init__.py`
- Consistent import structure
- Clear separation of concerns

### ✅ Production Ready
- Enterprise logging and monitoring
- Health checks and observability
- Security scanning and code quality
- Complete containerization

This structure follows Python packaging best practices and enterprise development standards.