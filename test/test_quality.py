from pathlib import Path

import pandas as pd
import pytest

from src.quality import (
    check_date_range,
    check_null_percentage,
    check_unique_keys,
    run_quality_checks,
)
from src.validate import load_contract

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"
DATA_DIR = Path(__file__).parent.parent / "data"


@pytest.fixture
def rinde_contract():
    return load_contract(CONTRACTS_DIR / "rinde_lotes.yaml")


@pytest.fixture
def rinde_df():
    return pd.read_csv(DATA_DIR / "rinde_lotes.csv")


class TestNullPercentage:
    def test_passes_when_under_threshold(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = check_null_percentage(df, max_null_pct=5)
        assert result["passed"]

    def test_fails_when_over_threshold(self):
        df = pd.DataFrame({"a": [1, None, None], "b": [4, 5, 6]})
        result = check_null_percentage(df, max_null_pct=70)
        assert result["passed"]
        result_strict = check_null_percentage(df, max_null_pct=10)
        assert not result_strict["passed"]
        assert "a" in result_strict["failures"]


class TestUniqueKeys:
    def test_detects_duplicates(self, rinde_df):
        result = check_unique_keys(rinde_df, ["campaña", "lote", "fecha_cosecha"])
        assert not result["passed"], "rinde_lotes.csv has duplicate rows"
        assert result["duplicate_rows"] > 0

    def test_passes_with_unique_data(self):
        df = pd.DataFrame({
            "campaña": ["2024", "2024", "2023"],
            "lote": ["A15", "B22", "A15"],
            "fecha": ["2024-03-15", "2024-04-01", "2023-03-18"],
        })
        result = check_unique_keys(df, ["campaña", "lote", "fecha"])
        assert result["passed"]

    def test_handles_missing_key_column(self):
        df = pd.DataFrame({"a": [1]})
        result = check_unique_keys(df, ["a", "b"])
        assert not result["passed"]
        assert "error" in result


class TestDateRange:
    def test_detects_out_of_range(self):
        df = pd.DataFrame({"fecha": ["2019-01-01", "2024-06-15"]})
        result = check_date_range(df, ["fecha"], "2020-01-01", "2026-12-31")
        assert not result["passed"]

    def test_passes_in_range(self):
        df = pd.DataFrame({"fecha": ["2024-03-15", "2024-04-01"]})
        result = check_date_range(df, ["fecha"], "2020-01-01", "2026-12-31")
        assert result["passed"]


class TestRunQualityChecks:
    def test_rinde_data_fails_dq(self, rinde_df, rinde_contract):
        result = run_quality_checks(rinde_df, rinde_contract)
        assert not result["all_passed"], "Dirty rinde data should fail DQ"

    def test_clean_data_passes_dq(self, rinde_contract):
        df = pd.DataFrame({
            "campaña": ["2024", "2024"],
            "lote": ["A15", "B22"],
            "cultivo": ["soja", "maíz"],
            "rinde_kg_ha": [3200, 8500],
            "superficie_ha": [150.5, 200.0],
            "fecha_cosecha": ["2024-03-15", "2024-04-01"],
        })
        result = run_quality_checks(df, rinde_contract)
        assert result["all_passed"]
