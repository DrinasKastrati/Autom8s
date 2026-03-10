"""
validations.py
--------------
All static pipeline infrastructure: logging, file loading, output handling,
date validation, and summary reporting.

main.py only needs to supply dataset/mapper names and the try/except row logic.
"""

import os
import logging
import warnings
import difflib
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logger(log_dir: str = ".", prefix: str = "migration") -> logging.Logger:
    """
    Create and return a logger that writes to both console and a timestamped
    .log file inside log_dir.

    Usage:
        logger = setup_logger()
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"{prefix}_{timestamp}.log")

    logger = logging.getLogger(prefix)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Log file created at: {log_path}")
    return logger


# ---------------------------------------------------------------------------
# Timestamped output filenames
# ---------------------------------------------------------------------------

def timestamped_filename(base: str, ext: str, out_dir: str = ".") -> str:
    """
    Return a full path with a timestamp appended before the extension.

    Example:
        timestamped_filename("output_zucc", "csv")
        -> "./output_zucc_20240517_143022.csv"
    """
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base}_{ts}.{ext.lstrip('.')}"
    return os.path.join(out_dir, filename)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(
    dataset_name: str,
    mapper_name: str,
    logger: logging.Logger = None,
    dataset_sheet: str = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Load the source dataset and mapper workbook.
    Raises SystemExit with a clear message if either file is not found.
    Returns (data, migration) tuple.

    Usage:
        data, migration = load_data(dataset_name, mapper_name, logger)
        data, migration = load_data(dataset_name, mapper_name, logger, dataset_sheet="Sheet2")
    """
    warnings.filterwarnings("ignore")

    dataset_path = dataset_name + ".xlsx"
    mapper_path  = mapper_name  + ".xlsx"

    # File existence checks
    for path in (dataset_path, mapper_path):
        if not os.path.exists(path):
            msg = f"File not found: '{path}' — check that the file exists in the working directory."
            if logger:
                logger.critical(msg)
            raise SystemExit(msg)

    if logger:
        logger.info(f"Loading dataset: {dataset_path}")
    data = (
        pd.read_excel(dataset_path)
        if dataset_sheet is None
        else pd.read_excel(dataset_path, sheet_name=dataset_sheet)
    )
    if logger:
        logger.info(f"Dataset loaded: {len(data)} rows x {len(data.columns)} columns")

    if logger:
        logger.info(f"Loading mapper: {mapper_path}")
    migration = pd.read_excel(mapper_path, sheet_name=None)
    if logger:
        sheet_summary = ", ".join(f"'{s}' ({len(migration[s])} rows)" for s in migration)
        logger.info(f"Mapper loaded: sheets — {sheet_summary}")

    return data, migration


# ---------------------------------------------------------------------------
# Output initialisation
# ---------------------------------------------------------------------------

def init_outputs(migration: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Initialise the success and failed output DataFrames.
    Returns (output_success, output_failed).

    Usage:
        output_success, output_failed = init_outputs(migration)
    """
    output_success = pd.DataFrame(columns=migration["Data"]["Field name"].values)
    output_failed  = pd.DataFrame()
    return output_success, output_failed


# ---------------------------------------------------------------------------
# Date format validation (pre-flight)
# ---------------------------------------------------------------------------

def validate_date_formats(
    data: pd.DataFrame,
    date_columns: list[str],
    date_format: str,
    logger: logging.Logger = None,
) -> dict[str, pd.Series]:
    """
    Validate that the given date columns match date_format.
    Null / empty values are always allowed.
    Logs a per-column count of bad rows (e.g. "3 of 1200 rows have bad dates").

    Usage:
        validate_date_formats(data, ["PaatosPaiva", "VoimassaolonAlkamisPaiva"], "%d.%m.%Y", logger)
    """
    def _is_valid(value) -> bool:
        if pd.isna(value) or str(value).strip() == "":
            return True
        try:
            datetime.strptime(str(value), date_format)
            return True
        except ValueError:
            return False

    total_rows = len(data)
    result = {}

    for col in date_columns:
        if col not in data.columns:
            if logger:
                logger.warning(f"Date column '{col}' not found in dataset -- skipping.")
            continue

        valid_mask = data[col].apply(_is_valid)
        bad_count  = (~valid_mask).sum()
        result[col] = valid_mask

        if bad_count > 0:
            msg = (
                f"Column '{col}': {bad_count} of {total_rows} rows have an unexpected date format "
                f"(expected: {date_format}). "
                f"First bad values: {data.loc[~valid_mask, col].head(5).tolist()}"
            )
            if logger:
                logger.warning(msg)
            else:
                print(f"[WARNING] {msg}")
        else:
            if logger:
                logger.info(f"Column '{col}': all {total_rows} values are valid ({date_format}).")

    return result


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def _extract_bracketed_columns(migration: dict) -> dict:
    """
    Scan all mapper sheets and collect columns whose names are wrapped in
    angle brackets, e.g. <PalveluSelite>. These are the join keys that must
    exist in the source dataset.

    Returns {sheet_name: [stripped_col_name, ...]}
    e.g. {"Mapping1": ["PalveluSelite"], "Mapping2": ["PalvelutehtavaNimi", "PalveluSelite"]}
    """
    import re
    result = {}
    for sheet, df in migration.items():
        bracketed = [
            re.sub(r"^<|>$", "", col)
            for col in df.columns
            if re.match(r"^<.+>$", str(col))
        ]
        if bracketed:
            result[sheet] = bracketed
    return result


def check_required_columns(
    data: pd.DataFrame,
    migration: dict,
    logger: logging.Logger = None,
) -> list:
    """
    Derive required source-dataset column names from the mapper's bracketed
    headers (e.g. <PalveluSelite> -> PalveluSelite) and verify they all exist
    in the dataset. Column names are read dynamically so renaming them in
    the mapper is enough - no code changes needed.

    Returns a list of any missing column names (empty list = all present).

    Usage:
        missing = check_required_columns(data, migration, logger)
    """
    log  = logger.info    if logger else print
    warn = logger.warning if logger else print
    err  = logger.error   if logger else print

    bracketed_map = _extract_bracketed_columns(migration)
    required = sorted({col for cols in bracketed_map.values() for col in cols})

    if not required:
        warn("[check_required_columns] No bracketed columns found in mapper - skipping.")
        return []

    missing = [col for col in required if col not in data.columns]
    present = [col for col in required if col in data.columns]

    log("=== Required column check ===")
    log(f"Columns derived from mapper : {required}")
    log(f"Present in dataset          : {present}")

    if missing:
        err(f"MISSING columns in dataset  : {missing}")
        err("These columns are referenced in the mapper but do not exist in the "
            "source file. The loop will fail for every row until this is fixed.")
    else:
        log("All required columns are present in the dataset.")

    return missing


def check_mapping_coverage(
    data: pd.DataFrame,
    migration: dict,
    logger: logging.Logger = None,
) -> dict:
    """
    For every mapping sheet that has bracketed join-key columns, check whether
    every unique combination of those key values in the dataset has a
    corresponding row in the mapper.

    Column names are derived dynamically from the bracketed headers so no
    hardcoding is needed - renaming columns in the mapper is sufficient.

    Returns a dict of {sheet_name: DataFrame-of-unmapped-combos}.
    An empty DataFrame for a sheet means full coverage.

    Usage:
        gaps = check_mapping_coverage(data, migration, logger)
    """
    log  = logger.info    if logger else print
    warn = logger.warning if logger else print

    log("=== Mapping coverage check ===")

    bracketed_map = _extract_bracketed_columns(migration)
    gaps = {}

    for sheet, key_cols in bracketed_map.items():
        missing_in_data = [c for c in key_cols if c not in data.columns]
        if missing_in_data:
            warn(f"Sheet '{sheet}': skipping coverage check - "
                 f"dataset is missing key column(s): {missing_in_data}")
            continue

        mapper_df   = migration[sheet]
        mapper_cols = [f"<{c}>" for c in key_cols]

        missing_mapper = [c for c in mapper_cols if c not in mapper_df.columns]
        if missing_mapper:
            warn(f"Sheet '{sheet}': bracketed columns not found in sheet: {missing_mapper}")
            continue

        data_combos   = data[key_cols].drop_duplicates()
        mapper_combos = (
            mapper_df[mapper_cols]
            .drop_duplicates()
            .rename(columns={f"<{c}>": c for c in key_cols})
        )

        unmapped = (
            data_combos
            .merge(mapper_combos, on=key_cols, how="left", indicator=True)
            .query('_merge == "left_only"')
            .drop(columns="_merge")
            .reset_index(drop=True)
        )

        affected_rows  = len(data.merge(unmapped, on=key_cols, how="inner"))
        total_combos   = len(data_combos)
        unmapped_count = len(unmapped)
        gaps[sheet]    = unmapped

        if unmapped_count == 0:
            log(f"Sheet '{sheet}': all {total_combos} combination(s) are covered.")
        else:
            warn(f"Sheet '{sheet}': {unmapped_count} of {total_combos} unique combination(s) "
                 f"have NO mapping - affects {affected_rows} source row(s).")
            warn(f"  Keys     : {key_cols}")
            warn(f"  Unmapped :\n{unmapped.to_string(index=False)}")

    return gaps


def run_preflight_checks(
    data: pd.DataFrame,
    migration: dict,
    date_columns: list = None,
    date_format: str = "%d.%m.%Y",
    logger: logging.Logger = None,
) -> None:
    """
    Run all pre-flight validation checks:
      - Required column presence (derived dynamically from mapper bracketed headers)
      - Mapping coverage (all dataset value combos present in each mapping sheet)
      - Date format validation (optional)

    Usage:
        run_preflight_checks(data, migration, date_columns=["PaatosPaiva"], date_format="%d.%m.%Y", logger=logger)
    """
    run_column_uniqueness_check(data, label="source dataset", logger=logger)
    check_required_columns(data, migration, logger)
    check_mapping_coverage(data, migration, logger)
    if date_columns:
        validate_date_formats(data, date_columns, date_format, logger)


# ---------------------------------------------------------------------------
# Post-conversion output date format check
# ---------------------------------------------------------------------------

def check_output_date_formats(
    output_success: pd.DataFrame,
    date_columns: list = None,
    date_format: str = "%d.%m.%Y",
    logger: logging.Logger = None,
) -> dict:
    """
    Validate that date columns in output_success match the expected output
    date_format after transformation. Catches cases where strftime produced
    a wrong, empty, or malformed value.

    date_columns defaults to all columns whose name contains "date" (case-insensitive)
    if not explicitly provided, so no hardcoding is needed.

    Returns a dict of {col: bad_row_count} for any columns with issues.

    Usage:
        check_output_date_formats(output_success, logger=logger)
        check_output_date_formats(output_success, date_columns=["Decision_FromDate"], date_format="%d.%m.%Y", logger=logger)
    """
    if output_success.empty:
        if logger:
            logger.info("Output date format check skipped — output_success is empty.")
        return {}

    log  = logger.info    if logger else print
    warn = logger.warning if logger else print

    # If no columns specified, auto-detect by name
    if date_columns is None:
        date_columns = [c for c in output_success.columns if "date" in c.lower()]

    if not date_columns:
        warn("[check_output_date_formats] No date columns found in output_success — skipping.")
        return {}

    def _is_valid(value) -> bool:
        if pd.isna(value) or str(value).strip() == "":
            return True
        try:
            datetime.strptime(str(value).strip(), date_format)
            return True
        except ValueError:
            return False

    log(f"=== Output date format check (expected: {date_format}) ===")

    total_rows = len(output_success)
    issues = {}

    for col in date_columns:
        if col not in output_success.columns:
            warn(f"Column '{col}' not found in output_success — skipping.")
            continue

        valid_mask = output_success[col].apply(_is_valid)
        bad_count  = int((~valid_mask).sum())

        if bad_count > 0:
            issues[col] = bad_count
            warn(f"Column '{col}': {bad_count} of {total_rows} rows have an unexpected format "
                 f"after transformation (expected: {date_format}).")
            warn(f"  First bad values: {output_success.loc[~valid_mask, col].head(5).tolist()}")
        else:
            log(f"Column '{col}': all {total_rows} transformed values match {date_format}.")

    if not issues:
        log("All output date columns passed format check.")

    return issues


# ---------------------------------------------------------------------------
# Post-conversion date correlation check
# ---------------------------------------------------------------------------

_CORRELATION_COLS = ["Person_ID", "Actualization_Date", "Decision_FromDate", "Decision_ToDate"]


def run_date_correlation_check(
    output_success: pd.DataFrame,
    logger: logging.Logger = None,
) -> None:
    """
    Check whether Actualization_Date matches Decision_FromDate and Decision_ToDate.
    Explicitly names any missing expected columns rather than raising a generic error.
    Logs matches, mismatches, and sample rows for any differing values.

    Usage:
        run_date_correlation_check(output_success, logger)
    """
    if output_success.empty:
        if logger:
            logger.info("Date correlation check skipped — output_success is empty.")
        return

    # Check for missing columns up front
    missing = [c for c in _CORRELATION_COLS if c not in output_success.columns]
    if missing:
        msg = f"Date correlation check skipped — missing expected columns: {missing}"
        if logger:
            logger.warning(msg)
        else:
            print(f"[WARNING] {msg}")
        return

    try:
        compare_df = output_success[_CORRELATION_COLS].copy()

        compare_df["Actualization_Date"] = compare_df["Actualization_Date"].fillna("").astype(str).str.strip()
        compare_df["Decision_FromDate"]  = compare_df["Decision_FromDate"].fillna("").astype(str).str.strip()
        compare_df["Decision_ToDate"]    = compare_df["Decision_ToDate"].fillna("").astype(str).str.strip()

        # Actualization_Date vs Decision_FromDate (string comparison is fine — same format both sides)
        act_from_match_mask = compare_df["Actualization_Date"] == compare_df["Decision_FromDate"]
        act_from_matches    = int(act_from_match_mask.sum())
        act_from_mismatches = len(compare_df) - act_from_matches

        # Decision_FromDate vs Decision_ToDate
        # Parse both as proper dates (DD.MM.YYYY) before comparing to avoid
        # incorrect alphabetical string comparison (e.g. "31.01.2024" > "01.02.2024" as text).
        def _parse_date(s: str):
            try:
                return datetime.strptime(s, "%d.%m.%Y")
            except ValueError:
                return None

        has_to_date  = compare_df["Decision_ToDate"] != ""
        from_parsed  = compare_df["Decision_FromDate"].apply(_parse_date)
        to_parsed    = compare_df["Decision_ToDate"].apply(_parse_date)

        both_valid         = from_parsed.notna() & to_parsed.notna() & has_to_date
        invalid_range_mask = both_valid & (from_parsed > to_parsed)
        invalid_range_count = int(invalid_range_mask.sum())

        total_rows = len(compare_df)

        log  = logger.info    if logger else print
        warn = logger.warning if logger else print

        log("=== Date correlation check ===")
        log(f"Total converted rows: {total_rows}")
        log(f"Actualization_Date vs Decision_FromDate: {act_from_matches} match, {act_from_mismatches} differ")
        log(f"Decision_FromDate vs Decision_ToDate   : {invalid_range_count} row(s) where FromDate > ToDate")

        if act_from_mismatches > 0:
            warn("Sample mismatches (Actualization_Date vs Decision_FromDate):")
            warn("\n" + compare_df.loc[~act_from_match_mask].head(10).to_string(index=False))

        if invalid_range_count > 0:
            warn("Sample mismatches (Decision_FromDate > Decision_ToDate):")
            warn("\n" + compare_df.loc[invalid_range_mask].head(10).to_string(index=False))

    except Exception as e:
        if logger:
            logger.error(f"Date correlation check failed: {e}")
        else:
            print(f"[ERROR] Date correlation check failed: {e}")


# ---------------------------------------------------------------------------
# Column uniqueness check (post-loop)
# ---------------------------------------------------------------------------

_SIMILARITY_THRESHOLD = 0.85  # 0.0 - 1.0: raise to be stricter, lower to catch more

def run_column_uniqueness_check(
    df: pd.DataFrame,
    similarity_threshold: float = _SIMILARITY_THRESHOLD,
    label: str = "dataset",
    logger: logging.Logger = None,
) -> bool:
    """
    Check a DataFrame's columns for:
      1. Whitespace issues  -- columns with leading/trailing spaces
      2. Near-duplicates    -- pairs of column names with a fuzzy similarity
                              score at or above similarity_threshold

    Similarity is computed with difflib.SequenceMatcher on lowercased,
    stripped names so casing differences do not mask a real collision
    (e.g. "Decision_Description" vs "decision_description_text" would score ~0.87).

    Use label to identify which DataFrame is being checked in log output.
    Returns True if no issues found, False if any warnings were raised.

    Usage:
        run_column_uniqueness_check(data, label="source dataset", logger=logger)
        run_column_uniqueness_check(output_success, label="output", logger=logger)
    """
    if len(df.columns) == 0:
        if logger:
            logger.info(f"Column uniqueness check skipped -- {label} has no columns.")
        return True

    log  = logger.info    if logger else print
    warn = logger.warning if logger else print

    columns      = list(df.columns)
    issues_found = False

    log(f"=== Column uniqueness check ({label}) ===")

    # 1. Whitespace check
    whitespace_cols = [c for c in columns if c != c.strip()]
    if whitespace_cols:
        issues_found = True
        for c in whitespace_cols:
            warn(f"Column has leading/trailing whitespace: {repr(c)}")
    else:
        log("No columns with leading/trailing whitespace.")

    # 2. Fuzzy near-duplicate check
    # Normalise names for comparison only -- original names are reported
    normalised = [c.strip().lower() for c in columns]
    near_dupes = []

    for i in range(len(normalised)):
        for j in range(i + 1, len(normalised)):
            score = difflib.SequenceMatcher(None, normalised[i], normalised[j]).ratio()
            if score >= similarity_threshold:
                near_dupes.append((columns[i], columns[j], score))

    if near_dupes:
        issues_found = True
        border = "!" * 60
        warn(border)
        warn(f"  ⚠  NEAR-DUPLICATE COLUMNS DETECTED ({len(near_dupes)} pair(s))  ⚠")
        warn(f"  Threshold : {similarity_threshold}")
        warn(border)
        for col_a, col_b, score in near_dupes:
            warn(f"  {repr(col_a)}")
            warn(f"    <--> {repr(col_b)}")
            warn(f"    similarity: {score:.2f}")
            warn("")
        warn("  These may be duplicate or misnamed columns — verify before proceeding.")
        warn(border)
    else:
        log(f"No near-duplicate column names found (threshold: {similarity_threshold}).")

    return not issues_found


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------

def write_outputs(
    output_success: pd.DataFrame,
    output_failed: pd.DataFrame,
    dataset_name: str,
    logger: logging.Logger = None,
) -> tuple[str, str]:
    """
    Write success CSV and failed XLSX to timestamped paths under ./output/.
    Logs a clear message if there are no failed rows rather than writing a blank file.
    Returns (output_success_path, output_failed_path).

    Usage:
        output_zucc, output_fail = write_outputs(output_success, output_failed, dataset_name, logger)
    """
    output_zucc = timestamped_filename("output_zucc", "csv",  out_dir="output")
    output_fail = timestamped_filename(dataset_name + "_not_converted", "xlsx", out_dir="output")

    output_success.to_csv(output_zucc, index=False, encoding="utf-8")
    if logger:
        logger.info(f"Success output written to : {output_zucc} ({len(output_success)} rows)")

    if output_failed.empty:
        if logger:
            logger.info("No failed rows — skipping failed output file.")
    else:
        output_failed.to_excel(output_fail, index=False)
        if logger:
            logger.info(f"Failed output written to  : {output_fail} ({len(output_failed)} rows)")

    return output_zucc, output_fail


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def print_summary(
    total_rows: int,
    success_count: int,
    failed_count: int,
    output_success_path: str,
    output_failed_path: str,
    execution_time: float,
    logger: logging.Logger = None,
) -> None:
    """
    Print and log a final summary of the migration run.

    Usage:
        print_summary(len(data), len(output_success), len(output_failed),
                      output_zucc, output_fail, execution_time, logger)
    """
    conversion_rate = f"{success_count / total_rows * 100:.1f}%" if total_rows else "N/A"
    lines = [
        "",
        "=" * 50,
        "         MIGRATION RUN SUMMARY",
        "=" * 50,
        f"  Total input rows   : {total_rows}",
        f"  Successfully mapped: {success_count}",
        f"  Failed             : {failed_count}",
        f"  Conversion rate    : {conversion_rate}",
        f"  Execution time     : {execution_time:.2f}s",
        "-" * 50,
        f"  Output (success)   : {output_success_path}",
        f"  Output (failed)    : {output_failed_path if failed_count > 0 else 'N/A — no failed rows'}",
        "=" * 50,
        "",
    ]
    summary = "\n".join(lines)
    if logger:
        logger.info(summary)
    else:
        print(summary)
