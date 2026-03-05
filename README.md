# migration-tools

A Python toolkit for data migration validation, preflight checks, and reporting.

## Install

```bash
pip install migration-tools
```

## Quick start

```python
from migration_tools import (
    setup_logger,
    load_dataset,
    load_mapper,
    init_outputs,
    run_preflight_checks,
    check_output_date_formats,
    run_date_correlation_check,
    write_outputs,
    print_summary,
    generate_html_report,
)

logger = setup_logger(log_dir="logs")

data      = load_dataset("dataset1.xlsx", logger=logger)
migration = load_mapper("mapper.xlsx", logger=logger)
output_success, output_failed = init_outputs(migration)

# Pre-flight: column presence, mapping coverage, date format
run_preflight_checks(
    data,
    migration,
    date_columns=["PaatosPaiva", "VoimassaolonAlkamisPaiva"],
    date_format="%d.%m.%Y",
    logger=logger,
)

# ... your conversion loop ...

# Post-conversion checks
check_output_date_formats(output_success, date_format="%d.%m.%Y", logger=logger)
run_date_correlation_check(output_success, logger=logger)

# Write outputs and summary
output_zucc, output_fail = write_outputs(output_success, output_failed, "dataset1", logger)
print_summary(len(data), len(output_success), len(output_failed), output_zucc, output_fail, 12.4, logger)

# Generate HTML report
generate_html_report(
    dataset_name="dataset1",
    total_rows=len(data),
    success_count=len(output_success),
    failed_count=len(output_failed),
    execution_time=12.4,
    output_success=output_success,
    output_failed=output_failed,
    logger=logger,
)
```

## CLI

```bash
# Run preflight checks only
migration-tools validate --dataset dataset1.xlsx --mapper mapper.xlsx

# Validate and generate an HTML report
migration-tools report --dataset dataset1.xlsx --mapper mapper.xlsx --format html

# Validate and generate an Excel report
migration-tools report --dataset dataset1.xlsx --mapper mapper.xlsx --format excel

# Both formats at once
migration-tools report --dataset dataset1.xlsx --mapper mapper.xlsx --format both

# Custom date columns and format
migration-tools validate \
    --dataset dataset1.xlsx \
    --mapper mapper.xlsx \
    --date-cols PaatosPaiva VoimassaolonAlkamisPaiva \
    --date-format "%d.%m.%Y"
```

## Supported file formats

| File        | Formats supported         |
|-------------|---------------------------|
| Dataset     | `.xlsx`, `.csv`, `.json`  |
| Mapper      | `.xlsx`, `.json`          |

## What gets validated

| Check | When |
|---|---|
| Required columns present in dataset | Pre-flight |
| Mapping coverage (all value combos have a mapper row) | Pre-flight |
| Source date format | Pre-flight |
| Near-duplicate / whitespace column names | Pre-flight |
| Output date format after transformation | Post-conversion |
| `Decision_FromDate` ≤ `Decision_ToDate` | Post-conversion |
| `Actualization_Date` matches `Decision_FromDate` | Post-conversion |

## Publishing a new version

1. Bump `version` in `pyproject.toml` and `__init__.py`
2. Commit and push to `main`
3. Create a GitHub Release — CI will build and publish to PyPI automatically

## Development

```bash
git clone https://github.com/yourname/migration-tools
cd migration-tools
pip install -e ".[dev]"
pytest tests/ -v --cov=migration_tools
```

## License

MIT
