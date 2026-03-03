import re
import pandas as pd
from utils.constants import BRAND_PREFIX_LENGTH


def read_file_to_df(file) -> pd.DataFrame:
    """Read an uploaded file (CSV or Excel) into a DataFrame."""
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    elif name.endswith((".xlsx", ".xls")):
        return pd.read_excel(file, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported file type: {file.name}. Use CSV or Excel.")


def coerce_amount(series: pd.Series) -> pd.Series:
    """Strip currency symbols/commas and convert to float."""
    cleaned = series.astype(str).str.replace(r"[^\d.\-]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def coerce_date(series: pd.Series) -> pd.Series:
    """Flexible date parsing with fallbacks."""
    return pd.to_datetime(series, dayfirst=True, errors="coerce")


def normalize_txn_id(series: pd.Series) -> pd.Series:
    """Strip whitespace and uppercase for consistent matching."""
    return series.astype(str).str.strip().str.upper()


def extract_brand(txn_id_series: pd.Series) -> pd.Series:
    """Extract brand from first N characters of TxnID, uppercased."""
    return txn_id_series.astype(str).str[:BRAND_PREFIX_LENGTH].str.upper()
