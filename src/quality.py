import pandas as pd


def check_null_percentage(df: pd.DataFrame, max_null_pct: float) -> dict:
    """Check that no column exceeds the max null percentage."""
    failures = {}
    for col in df.columns:
        pct = df[col].isnull().mean() * 100
        if pct > max_null_pct:
            failures[col] = round(pct, 2)

    return {
        "check": "null_percentage",
        "passed": len(failures) == 0,
        "max_allowed_pct": max_null_pct,
        "failures": failures,
    }


def check_unique_keys(df: pd.DataFrame, keys: list[str]) -> dict:
    """Check that the combination of key columns has no duplicates."""
    if not all(k in df.columns for k in keys):
        missing = [k for k in keys if k not in df.columns]
        return {
            "check": "unique_keys",
            "passed": False,
            "error": f"Missing key columns: {missing}",
        }

    duplicates = df.duplicated(subset=keys, keep=False)
    dup_count = int(duplicates.sum())

    return {
        "check": "unique_keys",
        "passed": dup_count == 0,
        "keys": keys,
        "duplicate_rows": dup_count,
    }


def check_date_range(
    df: pd.DataFrame,
    date_columns: list[str],
    min_date: str,
    max_date: str,
) -> dict:
    """Check that date columns fall within the expected range."""
    failures = {}
    min_dt = pd.Timestamp(min_date)
    max_dt = pd.Timestamp(max_date)

    for col in date_columns:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce").dropna()
        out_of_range = parsed[(parsed < min_dt) | (parsed > max_dt)]
        if len(out_of_range) > 0:
            failures[col] = {
                "out_of_range_count": len(out_of_range),
                "sample_values": sorted(out_of_range.dt.strftime("%Y-%m-%d").unique()[:5].tolist()),
            }

    return {
        "check": "date_range",
        "passed": len(failures) == 0,
        "expected_range": {"min": min_date, "max": max_date},
        "failures": failures,
    }


def run_quality_checks(df: pd.DataFrame, contract: dict) -> dict:
    """Run all quality checks defined in a data contract."""
    quality = contract.get("quality", {})
    results = []

    if "max_null_pct" in quality:
        results.append(check_null_percentage(df, quality["max_null_pct"]))

    if "unique_keys" in quality:
        results.append(check_unique_keys(df, quality["unique_keys"]))

    if "date_range" in quality:
        date_cols = [
            col["name"] for col in contract["schema"] if col["type"] == "date"
        ]
        results.append(check_date_range(
            df,
            date_cols,
            quality["date_range"]["min"],
            quality["date_range"]["max"],
        ))

    all_passed = all(r["passed"] for r in results)

    return {
        "dataset": contract.get("name", "unknown"),
        "all_passed": all_passed,
        "checks": results,
    }
