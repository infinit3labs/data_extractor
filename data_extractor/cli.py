"""
Command Line Interface for the data extractor.
"""

import argparse
import sys
import os
from datetime import datetime
from typing import List, Dict

from .core import DataExtractor
from .config import ConfigManager


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Extract data from Oracle databases using Spark JDBC with parallel processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract using configuration files
  data-extractor --config config.ini --tables tables.json
  
  # Extract single table with incremental extraction
  data-extractor --host localhost --port 1521 --service XE --user hr --password secret \\
                 --source-name oracle_db --table-name employees --schema hr \\
                 --incremental-column last_modified
  
  # Extract single table with full extraction
  data-extractor --host localhost --port 1521 --service XE --user hr --password secret \\
                 --source-name oracle_db --table-name departments --schema hr --full-extract
  
  # Generate sample configuration files
  data-extractor --generate-config config.ini --generate-tables tables.json
        """
    )
    
    # Configuration file arguments
    parser.add_argument(
        '--config', '-c',
        help='Path to configuration file (INI format)'
    )
    parser.add_argument(
        '--tables', '-t',
        help='Path to tables configuration file (JSON format)'
    )
    
    # Database connection arguments
    parser.add_argument(
        '--host',
        help='Oracle database host'
    )
    parser.add_argument(
        '--port',
        help='Oracle database port (default: 1521)',
        default='1521'
    )
    parser.add_argument(
        '--service',
        help='Oracle service name'
    )
    parser.add_argument(
        '--user',
        help='Database username'
    )
    parser.add_argument(
        '--password',
        help='Database password'
    )
    
    # Single table extraction arguments
    parser.add_argument(
        '--source-name',
        help='Name of the data source'
    )
    parser.add_argument(
        '--table-name',
        help='Name of the table to extract'
    )
    parser.add_argument(
        '--schema',
        help='Schema name'
    )
    parser.add_argument(
        '--incremental-column',
        help='Column name for incremental extraction'
    )
    parser.add_argument(
        '--extraction-date',
        help='Date for incremental extraction (YYYY-MM-DD format, default: yesterday)'
    )
    parser.add_argument(
        '--full-extract',
        action='store_true',
        help='Perform full table extraction instead of incremental'
    )
    parser.add_argument(
        '--custom-query',
        help='Custom SQL query to execute instead of table extraction'
    )
    
    # General arguments
    parser.add_argument(
        '--output-path',
        help='Base output path for extracted files (default: data)',
        default='data'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        help='Maximum number of worker threads (default: number of CPU cores)'
    )
    parser.add_argument(
        '--use-global-spark',
        action='store_true',
        help='Use a single global Spark session (Databricks friendly)'
    )
    parser.add_argument(
        '--run-id',
        help='Unique run identifier (default: timestamp)'
    )
    
    # Utility arguments
    parser.add_argument(
        '--generate-config',
        help='Generate sample configuration file at specified path'
    )
    parser.add_argument(
        '--generate-tables',
        help='Generate sample tables configuration file at specified path'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser


def validate_single_table_args(args) -> List[str]:
    """Validate arguments for single table extraction."""
    errors = []
    
    required_args = ['host', 'service', 'user', 'password', 'source_name', 'table_name']
    for arg in required_args:
        if not getattr(args, arg.replace('-', '_')):
            errors.append(f"--{arg} is required for single table extraction")
            
    if not args.full_extract and not args.incremental_column:
        errors.append("--incremental-column is required unless --full-extract is specified")
        
    return errors


def extract_single_table(args) -> bool:
    """Extract a single table based on command line arguments."""
    # Parse extraction date
    extraction_date = None
    if args.extraction_date:
        try:
            extraction_date = datetime.strptime(args.extraction_date, '%Y-%m-%d')
        except ValueError:
            print(f"Error: Invalid date format: {args.extraction_date}. Use YYYY-MM-DD format.")
            return False
            
    # Create data extractor
    extractor = DataExtractor(
        oracle_host=args.host,
        oracle_port=args.port,
        oracle_service=args.service,
        oracle_user=args.user,
        oracle_password=args.password,
        output_base_path=args.output_path,
        max_workers=args.max_workers,
        use_global_spark_session=args.use_global_spark
    )
    
    # Extract table
    success = extractor.extract_table(
        source_name=args.source_name,
        table_name=args.table_name,
        schema_name=args.schema,
        incremental_column=args.incremental_column,
        extraction_date=extraction_date,
        is_full_extract=args.full_extract,
        custom_query=args.custom_query,
        run_id=args.run_id
    )
    
    # Cleanup
    extractor.cleanup_spark_sessions()
    
    return success


def extract_multiple_tables(args) -> bool:
    """Extract multiple tables based on configuration files."""
    # Load configuration
    config_manager = ConfigManager(args.config)
    
    # Get database configuration
    db_config = config_manager.get_runtime_config(
        output_base_path=args.output_path,
        max_workers=args.max_workers,
        use_global_spark_session=args.use_global_spark
    )
    
    # Override with command line arguments if provided
    if args.host:
        db_config['oracle_host'] = args.host
    if args.port:
        db_config['oracle_port'] = args.port
    if args.service:
        db_config['oracle_service'] = args.service
    if args.user:
        db_config['oracle_user'] = args.user
    if args.password:
        db_config['oracle_password'] = args.password
        
    # Validate required database configuration
    required_db_fields = ['oracle_host', 'oracle_service', 'oracle_user', 'oracle_password']
    for field in required_db_fields:
        if not db_config.get(field):
            print(f"Error: {field} is required. Provide it via config file or command line argument.")
            return False
            
    # Load table configurations
    if not args.tables:
        print("Error: --tables configuration file is required for multiple table extraction.")
        return False
        
    if not os.path.exists(args.tables):
        print(f"Error: Tables configuration file not found: {args.tables}")
        return False
        
    try:
        table_configs = config_manager.load_table_configs_from_json(args.tables)
    except Exception as e:
        print(f"Error loading tables configuration: {e}")
        return False
        
    if not table_configs:
        print("Error: No table configurations found in the tables file.")
        return False
        
    # Validate table configurations
    for i, table_config in enumerate(table_configs):
        errors = config_manager.validate_table_config(table_config)
        if errors:
            print(f"Error in table configuration {i}: {', '.join(errors)}")
            return False
            
    # Create data extractor
    extractor = DataExtractor(
        oracle_host=db_config['oracle_host'],
        oracle_port=db_config.get('oracle_port', '1521'),
        oracle_service=db_config['oracle_service'],
        oracle_user=db_config['oracle_user'],
        oracle_password=db_config['oracle_password'],
        output_base_path=db_config.get('output_base_path', 'data'),
        max_workers=db_config.get('max_workers'),
        use_global_spark_session=args.use_global_spark or db_config.get('use_global_spark_session', False)
    )
    
    # Extract tables in parallel
    results = extractor.extract_tables_parallel(table_configs)
    
    # Cleanup
    extractor.cleanup_spark_sessions()
    
    # Check results
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    
    print(f"Extraction completed: {successful}/{total} tables successful")
    
    if successful < total:
        print("Failed tables:")
        for table_name, success in results.items():
            if not success:
                print(f"  - {table_name}")
                
    return successful == total


def main():
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Handle utility commands
    if args.generate_config:
        config_manager = ConfigManager()
        config_manager.create_sample_config_file(args.generate_config)
        print(f"Sample configuration file created: {args.generate_config}")
        
        if args.generate_tables:
            config_manager.create_sample_tables_json(args.generate_tables)
            print(f"Sample tables configuration file created: {args.generate_tables}")
            
        return
        
    if args.generate_tables:
        config_manager = ConfigManager()
        config_manager.create_sample_tables_json(args.generate_tables)
        print(f"Sample tables configuration file created: {args.generate_tables}")
        return
        
    # Determine extraction mode
    if args.tables or args.config:
        # Multiple table extraction using configuration files
        success = extract_multiple_tables(args)
    else:
        # Single table extraction using command line arguments
        errors = validate_single_table_args(args)
        if errors:
            print("Error: " + ", ".join(errors))
            parser.print_help()
            sys.exit(1)
            
        success = extract_single_table(args)
        
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()