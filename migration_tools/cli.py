"""
cli.py
------
Command-line interface for migration_tools.

Usage examples:
    migration-tools validate --dataset dataset1.xlsx --mapper mapper.xlsx
    migration-tools report   --dataset dataset1.xlsx --mapper mapper.xlsx --format html
    migration-tools report   --dataset dataset1.xlsx --mapper mapper.xlsx --format excel
"""

import argparse
import sys
import time

import pandas as pd

from .loaders import load_dataset, load_mapper
from .validations import (
    setup_logger,
    init_outputs,
    run_preflight_checks,
    check_output_date_formats,
    run_date_correlation_check,
    run_column_uniqueness_check,
    write_outputs,
    print_summary,
)
from .reports import generate_html_report, generate_excel_report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migration-tools",
        description="Data migration validation and reporting toolkit.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- validate ---
    val = sub.add_parser("validate", help="Run pre-flight and post-conversion checks.")
    val.add_argument("--dataset",     required=True, help="Path to the source dataset file (.xlsx, .csv, .json)")
    val.add_argument("--mapper",      required=True, help="Path to the mapper file (.xlsx, .json)")
    val.add_argument("--date-cols",   nargs="*",     help="Source date column names to validate (space-separated)")
    val.add_argument("--date-format", default="%d.%m.%Y", help="Expected date format in source data (default: %%d.%%m.%%Y)")
    val.add_argument("--log-dir",     default="logs", help="Directory for log files (default: logs)")

    # --- report ---
    rep = sub.add_parser("report", help="Run validation and generate an HTML or Excel report.")
    rep.add_argument("--dataset",     required=True, help="Path to the source dataset file")
    rep.add_argument("--mapper",      required=True, help="Path to the mapper file")
    rep.add_argument("--format",      choices=["html", "excel", "both"], default="html",
                     help="Report format (default: html)")
    rep.add_argument("--out-dir",     default="output", help="Output directory (default: output)")
    rep.add_argument("--date-cols",   nargs="*",     help="Source date column names to validate")
    rep.add_argument("--date-format", default="%d.%m.%Y", help="Expected date format in source data")
    rep.add_argument("--log-dir",     default="logs", help="Directory for log files")

    return parser


def _run_validate(args) -> None:
    start = time.time()
    logger = setup_logger(log_dir=args.log_dir, prefix="migration")

    data      = load_dataset(args.dataset, logger=logger)
    migration = load_mapper(args.mapper, logger=logger)
    output_success, output_failed = init_outputs(migration)

    run_preflight_checks(
        data,
        migration,
        date_columns=args.date_cols,
        date_format=args.date_format,
        logger=logger,
    )

    logger.info("Validate command complete. No conversion was run — preflight checks only.")
    logger.info(f"Elapsed: {time.time() - start:.2f}s")


def _run_report(args) -> None:
    start = time.time()
    logger = setup_logger(log_dir=args.log_dir, prefix="migration")

    dataset_name = args.dataset.rsplit(".", 1)[0]

    data      = load_dataset(args.dataset, logger=logger)
    migration = load_mapper(args.mapper, logger=logger)
    output_success, output_failed = init_outputs(migration)

    run_preflight_checks(
        data,
        migration,
        date_columns=args.date_cols,
        date_format=args.date_format,
        logger=logger,
    )

    # Note: full conversion loop is not run from CLI — reports are based on
    # preflight checks only unless output files are passed directly.
    # To report on a completed run, use generate_html_report() / generate_excel_report()
    # directly in your converter script.

    elapsed = time.time() - start

    if args.format in ("html", "both"):
        path = generate_html_report(
            dataset_name=dataset_name,
            total_rows=len(data),
            success_count=len(output_success),
            failed_count=len(output_failed),
            execution_time=elapsed,
            output_success=output_success,
            output_failed=output_failed,
            out_dir=args.out_dir,
            logger=logger,
        )
        print(f"HTML report: {path}")

    if args.format in ("excel", "both"):
        path = generate_excel_report(
            dataset_name=dataset_name,
            total_rows=len(data),
            success_count=len(output_success),
            failed_count=len(output_failed),
            execution_time=elapsed,
            output_success=output_success,
            output_failed=output_failed,
            out_dir=args.out_dir,
            logger=logger,
        )
        print(f"Excel report: {path}")


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    try:
        if args.command == "validate":
            _run_validate(args)
        elif args.command == "report":
            _run_report(args)
    except FileNotFoundError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
