"""
migration_tools
---------------
A data migration validation and reporting toolkit.

Quick start:
    from migration_tools import run_preflight_checks, generate_html_report
"""

from .validations import (
    setup_logger,
    load_data,
    init_outputs,
    validate_date_formats,
    check_required_columns,
    check_mapping_coverage,
    run_preflight_checks,
    check_output_date_formats,
    run_date_correlation_check,
    run_column_uniqueness_check,
    write_outputs,
    print_summary,
)

from .loaders import (
    load_dataset,
    load_mapper,
)

from .reports import (
    generate_html_report,
    generate_excel_report,
)

__version__ = "0.1.0"
__author__  = "Your Name"
__license__ = "MIT"

__all__ = [
    # Logging
    "setup_logger",
    # Loading
    "load_data",
    "load_dataset",
    "load_mapper",
    "init_outputs",
    # Preflight
    "run_preflight_checks",
    "validate_date_formats",
    "check_required_columns",
    "check_mapping_coverage",
    "run_column_uniqueness_check",
    # Post-conversion
    "check_output_date_formats",
    "run_date_correlation_check",
    # Output
    "write_outputs",
    "print_summary",
    # Reports
    "generate_html_report",
    "generate_excel_report",
]
