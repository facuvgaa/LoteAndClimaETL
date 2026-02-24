from pathlib import Path

import pandas as pd
import yaml


def load_contract(contract_path: str | Path) -> dict:
    with open(contract_path) as f:
        return yaml.safe_load(f)


def validate_schema(df: pd.DataFrame, contract: dict) -> dict:
    """Validates a DataFrame against a data contract definition.

    Returns dict with 'valid' (bool) and 'errors' (list of dicts).
    """
    errors = []
    schema = contract["schema"]
    expected_cols = {col["name"] for col in schema}
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    if missing:
        errors.append({
            "type": "missing_columns",
            "columns": sorted(missing),
        })

    extra = actual_cols - expected_cols
    if extra:
        errors.append({
            "type": "extra_columns",
            "columns": sorted(extra),
            "severity": "warning",
        })

    for col_def in schema:
        name = col_def["name"]
        if name not in df.columns:
            continue

        col = df[name]

        if not col_def.get("nullable", True) and col.isnull().any():
            null_count = int(col.isnull().sum())
            errors.append({
                "type": "unexpected_nulls",
                "column": name,
                "null_count": null_count,
            })

        if col_def["type"] == "numeric":
            coerced = pd.to_numeric(col, errors="coerce")
            non_numeric_mask = coerced.isna() & col.notna()
            if non_numeric_mask.any():
                errors.append({
                    "type": "invalid_type",
                    "column": name,
                    "expected": "numeric",
                    "invalid_count": int(non_numeric_mask.sum()),
                })

            if "min" in col_def and (coerced < col_def["min"]).any():
                errors.append({
                    "type": "below_min",
                    "column": name,
                    "min": col_def["min"],
                })
            if "max" in col_def and (coerced > col_def["max"]).any():
                errors.append({
                    "type": "above_max",
                    "column": name,
                    "max": col_def["max"],
                })

        if col_def["type"] == "date":
            fmt = col_def.get("format", "%Y-%m-%d")
            parsed = pd.to_datetime(col, format=fmt, errors="coerce")
            invalid_mask = parsed.isna() & col.notna()
            if invalid_mask.any():
                errors.append({
                    "type": "invalid_date",
                    "column": name,
                    "format": fmt,
                    "invalid_count": int(invalid_mask.sum()),
                })

        if "allowed_values" in col_def:
            valid_set = set(col_def["allowed_values"])
            actual_values = set(col.dropna().unique())
            invalid = actual_values - valid_set
            if invalid:
                errors.append({
                    "type": "invalid_values",
                    "column": name,
                    "invalid": sorted(invalid),
                    "allowed": sorted(valid_set),
                })

    real_errors = [e for e in errors if e.get("severity") != "warning"]
    warnings = [e for e in errors if e.get("severity") == "warning"]

    return {
        "valid": len(real_errors) == 0,
        "errors": real_errors,
        "warnings": warnings,
    }
