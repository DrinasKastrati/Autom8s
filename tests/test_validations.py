"""
tests/test_validations.py
--------------------------
Unit tests for migration_tools.validations
"""

import pytest
import pandas as pd
from migration_tools import (
    validate_date_formats,
    check_required_columns,
    check_mapping_coverage,
    check_output_date_formats,
    run_date_correlation_check,
    run_column_uniqueness_check,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "PalveluSelite":      ["TypeA", "TypeB", "TypeC"],
        "PalvelutehtavaNimi": ["Task1", "Task2", "Task1"],
        "PaatosPaiva":        ["01.01.2023", "15.06.2023", "bad-date"],
        "Hetu":               ["010190-123A", "020280-456B", "030370-789C"],
    })


@pytest.fixture
def sample_migration():
    return {
        "Data": pd.DataFrame({"Field name": ["Person_ID", "Decision_FromDate", "Decision_ToDate"]}),
        "Mapping1": pd.DataFrame({
            "<PalveluSelite>": ["TypeA", "TypeB"],
            "Sosiaalipalvelu": ["ServiceA", "ServiceB"],
        }),
        "Mapping2": pd.DataFrame({
            "<PalvelutehtavaNimi>": ["Task1", "Task2"],
            "<PalveluSelite>":      ["TypeA", "TypeB"],
            "Palvelun tyyppi":      ["Type1", "Type2"],
        }),
    }


@pytest.fixture
def sample_output():
    return pd.DataFrame({
        "Person_ID":          ["010190-123A", "020280-456B"],
        "Actualization_Date": ["01.01.2023",  "15.06.2023"],
        "Decision_FromDate":  ["01.01.2023",  "15.06.2023"],
        "Decision_ToDate":    ["31.12.2023",  "31.12.2023"],
    })


# ---------------------------------------------------------------------------
# validate_date_formats
# ---------------------------------------------------------------------------

def test_validate_date_formats_all_valid():
    data = pd.DataFrame({"PaatosPaiva": ["01.01.2023", "15.06.2023"]})
    result = validate_date_formats(data, ["PaatosPaiva"], "%d.%m.%Y")
    assert result["PaatosPaiva"].all()


def test_validate_date_formats_bad_value():
    data = pd.DataFrame({"PaatosPaiva": ["01.01.2023", "not-a-date"]})
    result = validate_date_formats(data, ["PaatosPaiva"], "%d.%m.%Y")
    assert not result["PaatosPaiva"].all()
    assert result["PaatosPaiva"].sum() == 1


def test_validate_date_formats_nulls_allowed():
    data = pd.DataFrame({"PaatosPaiva": ["01.01.2023", None, ""]})
    result = validate_date_formats(data, ["PaatosPaiva"], "%d.%m.%Y")
    assert result["PaatosPaiva"].all()


def test_validate_date_formats_missing_column():
    data = pd.DataFrame({"OtherCol": ["01.01.2023"]})
    result = validate_date_formats(data, ["PaatosPaiva"], "%d.%m.%Y")
    assert "PaatosPaiva" not in result


# ---------------------------------------------------------------------------
# check_required_columns
# ---------------------------------------------------------------------------

def test_check_required_columns_all_present(sample_data, sample_migration):
    missing = check_required_columns(sample_data, sample_migration)
    assert missing == []


def test_check_required_columns_missing(sample_migration):
    data = pd.DataFrame({"PalveluSelite": ["TypeA"]})  # missing PalvelutehtavaNimi
    missing = check_required_columns(data, sample_migration)
    assert "PalvelutehtavaNimi" in missing


def test_check_required_columns_no_brackets():
    migration = {"Data": pd.DataFrame({"Field name": ["col1"]})}
    data = pd.DataFrame({"col1": [1]})
    missing = check_required_columns(data, migration)
    assert missing == []


# ---------------------------------------------------------------------------
# check_mapping_coverage
# ---------------------------------------------------------------------------

def test_check_mapping_coverage_full(sample_data, sample_migration):
    data = pd.DataFrame({
        "PalveluSelite":      ["TypeA", "TypeB"],
        "PalvelutehtavaNimi": ["Task1", "Task2"],
    })
    gaps = check_mapping_coverage(data, sample_migration)
    assert len(gaps["Mapping1"]) == 0
    assert len(gaps["Mapping2"]) == 0


def test_check_mapping_coverage_gap(sample_migration):
    data = pd.DataFrame({
        "PalveluSelite":      ["TypeA", "TypeZ"],  # TypeZ not in mapper
        "PalvelutehtavaNimi": ["Task1", "Task1"],
    })
    gaps = check_mapping_coverage(data, sample_migration)
    assert len(gaps["Mapping1"]) == 1
    assert "TypeZ" in gaps["Mapping1"]["PalveluSelite"].values


# ---------------------------------------------------------------------------
# check_output_date_formats
# ---------------------------------------------------------------------------

def test_check_output_date_formats_valid(sample_output):
    issues = check_output_date_formats(sample_output, date_format="%d.%m.%Y")
    assert issues == {}


def test_check_output_date_formats_bad_value(sample_output):
    sample_output.loc[0, "Decision_FromDate"] = "2023/01/01"
    issues = check_output_date_formats(
        sample_output,
        date_columns=["Decision_FromDate"],
        date_format="%d.%m.%Y",
    )
    assert "Decision_FromDate" in issues
    assert issues["Decision_FromDate"] == 1


def test_check_output_date_formats_empty_allowed(sample_output):
    sample_output.loc[0, "Decision_ToDate"] = ""
    issues = check_output_date_formats(
        sample_output,
        date_columns=["Decision_ToDate"],
        date_format="%d.%m.%Y",
    )
    assert issues == {}


def test_check_output_date_formats_empty_df():
    issues = check_output_date_formats(pd.DataFrame())
    assert issues == {}


# ---------------------------------------------------------------------------
# run_date_correlation_check
# ---------------------------------------------------------------------------

def test_run_date_correlation_no_errors(sample_output):
    # Should not raise
    run_date_correlation_check(sample_output)


def test_run_date_correlation_invalid_range(sample_output):
    # ToDate before FromDate
    sample_output.loc[0, "Decision_ToDate"] = "01.01.2020"
    # Should not raise, just warn
    run_date_correlation_check(sample_output)


def test_run_date_correlation_empty():
    run_date_correlation_check(pd.DataFrame())


# ---------------------------------------------------------------------------
# run_column_uniqueness_check_
# ---------------------------------------------------------------------------

def test_column_uniqueness_clean():
    df = pd.DataFrame(columns=["Person_ID", "Decision_FromDate", "Decision_ToDate"])
    result = run_column_uniqueness_check(df, similarity_threshold=0.90)
    assert result is True


def test_column_uniqueness_whitespace():
    df = pd.DataFrame({"Person_ID ": ["123"], "Decision_FromDate": ["01.01.2023"]})
    result = run_column_uniqueness_check(df, similarity_threshold=0.90)
    assert result is False


def test_column_uniqueness_near_duplicate():
    df = pd.DataFrame({"Decision_Description": ["text"], "Decision_Description_Text": ["text"]})
    result = run_column_uniqueness_check(df, similarity_threshold=0.90)
    assert result is False
