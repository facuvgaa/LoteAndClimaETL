from pathlib import Path

import pandas as pd
import pytest

from src.validate import load_contract, validate_schema

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"
DATA_DIR = Path(__file__).parent.parent / "data"


@pytest.fixture
def rinde_contract():
    return load_contract(CONTRACTS_DIR / "rinde_lotes.yaml")


@pytest.fixture
def clima_contract():
    return load_contract(CONTRACTS_DIR / "clima_diario.yaml")


@pytest.fixture
def rinde_df():
    return pd.read_csv(DATA_DIR / "rinde_lotes.csv")


@pytest.fixture
def clima_df():
    return pd.read_csv(DATA_DIR / "clima_diario.csv")


class TestValidateSchema:
    def test_detects_missing_columns(self, rinde_contract):
        df = pd.DataFrame({"campaña": ["2024"], "lote": ["A15"]})
        result = validate_schema(df, rinde_contract)
        assert not result["valid"]
        missing_err = next(e for e in result["errors"] if e["type"] == "missing_columns")
        assert "rinde_kg_ha" in missing_err["columns"]

    def test_detects_invalid_date(self, rinde_contract, rinde_df):
        result = validate_schema(rinde_df, rinde_contract)
        date_errors = [e for e in result["errors"] if e["type"] == "invalid_date"]
        assert len(date_errors) > 0, "Should detect '2025-13-01' as invalid date"

    def test_detects_above_max(self, rinde_contract, rinde_df):
        result = validate_schema(rinde_df, rinde_contract)
        max_errors = [e for e in result["errors"] if e["type"] == "above_max"]
        assert len(max_errors) > 0, "Should detect rinde_kg_ha=99999 as above max"

    def test_detects_unexpected_nulls(self, rinde_contract):
        df = pd.DataFrame({
            "campaña": [None, "2024"],
            "lote": ["A15", "B22"],
            "cultivo": ["soja", "maíz"],
            "rinde_kg_ha": [3200, 8500],
            "superficie_ha": [150.5, 200.0],
            "fecha_cosecha": ["2024-03-15", "2024-04-01"],
        })
        result = validate_schema(df, rinde_contract)
        null_errors = [e for e in result["errors"] if e["type"] == "unexpected_nulls"]
        assert any(e["column"] == "campaña" for e in null_errors)

    def test_valid_dataframe_passes(self, rinde_contract):
        df = pd.DataFrame({
            "campaña": ["2024"],
            "lote": ["A15"],
            "cultivo": ["soja"],
            "rinde_kg_ha": [3200],
            "superficie_ha": [150.5],
            "fecha_cosecha": ["2024-03-15"],
        })
        result = validate_schema(df, rinde_contract)
        assert result["valid"]
        assert len(result["errors"]) == 0

    def test_clima_detects_out_of_range_humidity(self, clima_contract):
        df = pd.DataFrame({
            "fecha": ["2024-03-15"],
            "lote": ["A15"],
            "temp_max": [30.0],
            "temp_min": [15.0],
            "precipitacion_mm": [0.0],
            "humedad_pct": [150],
        })
        result = validate_schema(df, clima_contract)
        assert not result["valid"]
        max_errors = [e for e in result["errors"] if e["type"] == "above_max"]
        assert any(e["column"] == "humedad_pct" for e in max_errors)
