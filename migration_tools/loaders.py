"""
loaders.py
----------
Format-agnostic file loading for migration datasets and mappers.
Supports Excel (.xlsx), CSV (.csv), and JSON (.json).
"""

import logging
import os
import json
import pandas as pd


SUPPORTED_FORMATS = (".xlsx", ".csv", ".json")


def _infer_format(path: str) -> str:
    _, ext = os.path.splitext(path.lower())
    if ext not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported file format: '{ext}'. "
            f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
        )
    return ext


def load_dataset(
    path: str,
    sheet_name: str = None,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Load a dataset from an Excel, CSV, or JSON file into a DataFrame.

    For Excel files, sheet_name selects a specific sheet (default: first sheet).
    CSV and JSON files ignore sheet_name.

    Usage:
        data = load_dataset("dataset1.xlsx", logger=logger)
        data = load_dataset("dataset1.csv", logger=logger)
        data = load_dataset("dataset1.json", logger=logger)
    """
    if not os.path.exists(path):
        msg = f"Dataset file not found: '{path}'"
        if logger:
            logger.critical(msg)
        raise FileNotFoundError(msg)

    ext = _infer_format(path)

    if logger:
        logger.info(f"Loading dataset: {path}")

    if ext == ".xlsx":
        data = pd.read_excel(path) if sheet_name is None else pd.read_excel(path, sheet_name=sheet_name)
    elif ext == ".csv":
        data = pd.read_csv(path)
    elif ext == ".json":
        data = pd.read_json(path)

    if logger:
        logger.info(f"Dataset loaded: {len(data)} rows x {len(data.columns)} columns")

    return data


def load_mapper(
    path: str,
    logger: logging.Logger = None,
) -> dict:
    """
    Load a mapper file into a dict of {sheet_name: DataFrame}.

    For Excel files, all sheets are loaded.
    For CSV, a single-sheet dict is returned keyed by the filename stem.
    For JSON, the top-level keys are used as sheet names — each value must
    be a list of records, e.g. {"Mapping1": [...], "Mapping2": [...]}.

    Usage:
        migration = load_mapper("mapper.xlsx", logger=logger)
        migration = load_mapper("mapper.json", logger=logger)
    """
    if not os.path.exists(path):
        msg = f"Mapper file not found: '{path}'"
        if logger:
            logger.critical(msg)
        raise FileNotFoundError(msg)

    ext = _infer_format(path)

    if logger:
        logger.info(f"Loading mapper: {path}")

    if ext == ".xlsx":
        migration = pd.read_excel(path, sheet_name=None)
    elif ext == ".csv":
        stem = os.path.splitext(os.path.basename(path))[0]
        migration = {stem: pd.read_csv(path)}
    elif ext == ".json":
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("JSON mapper must be a top-level object with sheet names as keys.")
        migration = {sheet: pd.DataFrame(records) for sheet, records in raw.items()}

    if logger:
        sheet_summary = ", ".join(f"'{s}' ({len(migration[s])} rows)" for s in migration)
        logger.info(f"Mapper loaded: {sheet_summary}")

    return migration
