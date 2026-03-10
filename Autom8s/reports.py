"""
reports.py
----------
Auto-generate HTML and Excel validation reports from a migration run.
"""

import logging
import os
from datetime import datetime

import pandas as pd


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def generate_html_report(
    dataset_name: str,
    total_rows: int,
    success_count: int,
    failed_count: int,
    execution_time: float,
    output_success: pd.DataFrame,
    output_failed: pd.DataFrame,
    out_dir: str = "output",
    logger: logging.Logger = None,
) -> str:
    """
    Generate a self-contained HTML validation report for a migration run.
    Returns the path to the written file.

    Usage:
        path = generate_html_report(
            dataset_name="dataset1",
            total_rows=1200,
            success_count=1150,
            failed_count=50,
            execution_time=12.4,
            output_success=output_success,
            output_failed=output_failed,
            logger=logger,
        )
    """
    os.makedirs(out_dir, exist_ok=True)
    conversion_rate = f"{success_count / total_rows * 100:.1f}%" if total_rows else "N/A"

    # --- Not converted rows table ---
    if not output_failed.empty:
        failed_html = output_failed.to_html(
            index=False,
            classes="table",
            border=0,
            na_rep="",
        )
    else:
        failed_html = "<p class='ok'>✓ No unconverted rows.</p>"

    # --- Sample success rows (first 10) ---
    if not output_success.empty:
        sample_html = output_success.head(10).to_html(
            index=False,
            classes="table",
            border=0,
            na_rep="",
        )
    else:
        sample_html = "<p>No successful rows.</p>"

    # --- Error reason breakdown ---
    if not output_failed.empty and "Error_reason" in output_failed.columns:
        reason_counts = output_failed["Error_reason"].value_counts().reset_index()
        reason_counts.columns = ["Error reason", "Count"]
        reason_html = reason_counts.to_html(index=False, classes="table", border=0)
    else:
        reason_html = "<p class='ok'>✓ No errors recorded.</p>"

    status_color = "#2ecc71" if failed_count == 0 else "#e74c3c" if failed_count > total_rows * 0.1 else "#f39c12"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Migration Report — {dataset_name}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                margin: 0; padding: 0; background: #f5f6fa; color: #2c3e50; }}
        header {{ background: #2c3e50; color: white; padding: 24px 40px; }}
        header h1 {{ margin: 0; font-size: 1.6em; }}
        header p  {{ margin: 4px 0 0; opacity: 0.7; font-size: 0.9em; }}
        main {{ padding: 32px 40px; max-width: 1400px; }}
        .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 32px; }}
        .card {{ background: white; border-radius: 8px; padding: 20px 28px;
                 min-width: 160px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); flex: 1; }}
        .card .value {{ font-size: 2em; font-weight: 700; color: {status_color}; }}
        .card .label {{ font-size: 0.85em; color: #7f8c8d; margin-top: 4px; }}
        h2 {{ font-size: 1.1em; color: #2c3e50; border-bottom: 2px solid #ecf0f1;
              padding-bottom: 8px; margin-top: 36px; }}
        .table {{ width: 100%; border-collapse: collapse; font-size: 0.85em;
                  background: white; border-radius: 8px; overflow: hidden;
                  box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
        .table th {{ background: #2c3e50; color: white; padding: 10px 14px;
                     text-align: left; font-weight: 500; }}
        .table td {{ padding: 8px 14px; border-bottom: 1px solid #ecf0f1; }}
        .table tr:last-child td {{ border-bottom: none; }}
        .table tr:nth-child(even) td {{ background: #f9fafb; }}
        .ok {{ color: #27ae60; font-weight: 500; }}
        footer {{ padding: 24px 40px; font-size: 0.8em; color: #95a5a6; }}
    </style>
</head>
<body>
<header>
    <h1>Migration Validation Report</h1>
    <p>Dataset: {dataset_name} &nbsp;|&nbsp; Generated: {_now()}</p>
</header>
<main>
    <div class="cards">
        <div class="card"><div class="value">{total_rows}</div><div class="label">Total input rows</div></div>
        <div class="card"><div class="value" style="color:#2ecc71">{success_count}</div><div class="label">Successfully converted</div></div>
        <div class="card"><div class="value" style="color:{'#e74c3c' if failed_count > 0 else '#2ecc71'}">{failed_count}</div><div class="label">Not converted rows</div></div>
        <div class="card"><div class="value">{conversion_rate}</div><div class="label">Conversion rate</div></div>
        <div class="card"><div class="value">{execution_time:.1f}s</div><div class="label">Execution time</div></div>
    </div>

    <h2>Failure Breakdown</h2>
    {reason_html}

    <h2>Not converted rows</h2>
    {failed_html}

    <h2>Sample Output (first 10 rows)</h2>
    {sample_html}
</main>
<footer>migration-tools — {_now()}</footer>
</body>
</html>"""

    path = os.path.join(out_dir, f"{dataset_name}_report_{_ts()}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    if logger:
        logger.info(f"HTML report written to: {path}")

    return path


# ---------------------------------------------------------------------------
# Excel report
# ---------------------------------------------------------------------------

def generate_excel_report(
    dataset_name: str,
    total_rows: int,
    success_count: int,
    failed_count: int,
    execution_time: float,
    output_success: pd.DataFrame,
    output_failed: pd.DataFrame,
    out_dir: str = "output",
    logger: logging.Logger = None,
) -> str:
    """
    Generate an Excel validation report with separate sheets for:
      - Summary
      - Not converted rows
      - Sample output (first 50 rows)
      - Error breakdown

    Returns the path to the written file.

    Usage:
        path = generate_excel_report(
            dataset_name="dataset1",
            total_rows=1200,
            success_count=1150,
            failed_count=50,
            execution_time=12.4,
            output_success=output_success,
            output_failed=output_failed,
            logger=logger,
        )
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{dataset_name}_report_{_ts()}.xlsx")
    conversion_rate = f"{success_count / total_rows * 100:.1f}%" if total_rows else "N/A"

    with pd.ExcelWriter(path, engine="openpyxl") as writer:

        # --- Summary sheet ---
        summary_df = pd.DataFrame([
            {"Metric": "Dataset",            "Value": dataset_name},
            {"Metric": "Generated",          "Value": _now()},
            {"Metric": "Total input rows",   "Value": total_rows},
            {"Metric": "Successfully mapped","Value": success_count},
            {"Metric": "Not converted rows",        "Value": failed_count},
            {"Metric": "Conversion rate",    "Value": conversion_rate},
            {"Metric": "Execution time (s)", "Value": f"{execution_time:.2f}"},
        ])
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        # --- Not converted rows sheet ---
        if not output_failed.empty:
            output_failed.to_excel(writer, sheet_name="Not converted rows", index=False)
        else:
            pd.DataFrame([{"Info": "No unconverted rows."}]).to_excel(
                writer, sheet_name="Not converted rows", index=False
            )

        # --- Error breakdown sheet ---
        if not output_failed.empty and "Error_reason" in output_failed.columns:
            reason_df = (
                output_failed["Error_reason"]
                .value_counts()
                .reset_index()
            )
            reason_df.columns = ["Error reason", "Count"]
            reason_df.to_excel(writer, sheet_name="Error Breakdown", index=False)

        # --- Sample output sheet ---
        if not output_success.empty:
            output_success.head(50).to_excel(writer, sheet_name="Sample Output", index=False)

    if logger:
        logger.info(f"Excel report written to: {path}")

    return path
