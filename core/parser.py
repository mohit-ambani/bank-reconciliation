import pandas as pd
from utils.constants import (
    BANK_COL_TXN_ID, BANK_COL_AMOUNT, BANK_COL_DATE, BANK_COL_BRAND,
    LMS_REQUIRED_COLUMNS, LMS_COL_SOURCE_FILE,
)
from utils.helpers import (
    read_file_to_df, coerce_amount, coerce_date, normalize_txn_id, extract_brand,
)

# Known aliases for LMS columns (lowercase alias -> canonical name)
LMS_ALIASES = {
    "transid": "TxnID",
    "trans_id": "TxnID",
    "transaction_id": "TxnID",
    "txnid": "TxnID",
    "createdon": "Date",
    "created_on": "Date",
    "created_at": "Date",
    "date": "Date",
    "amount": "Amount",
}


def apply_bank_mapping(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Apply column mapping and clean a bank statement DataFrame."""
    reverse_map = {v: k for k, v in column_map.items()}
    df = df.rename(columns=reverse_map)

    df[BANK_COL_TXN_ID] = normalize_txn_id(df[BANK_COL_TXN_ID])
    df[BANK_COL_AMOUNT] = coerce_amount(df[BANK_COL_AMOUNT])
    df[BANK_COL_DATE] = coerce_date(df[BANK_COL_DATE])
    df[BANK_COL_BRAND] = extract_brand(df[BANK_COL_TXN_ID])

    return df


def apply_lms_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Apply alias resolution and cleaning to a combined LMS DataFrame."""
    # Rename _sourceFile to SourceFile if present (from client-side tagging)
    if "_sourceFile" in df.columns:
        df = df.rename(columns={"_sourceFile": LMS_COL_SOURCE_FILE})

    # Rename known aliases to canonical names
    alias_rename = {}
    for col in df.columns:
        canon = LMS_ALIASES.get(col.strip().lower())
        if canon and col != canon:
            alias_rename[col] = canon
    if alias_rename:
        df = df.rename(columns=alias_rename)

    # Case-insensitive column matching
    col_lower_map = {c.strip().lower(): c for c in df.columns}
    rename_map = {}
    missing = []
    for req in LMS_REQUIRED_COLUMNS:
        matched_col = col_lower_map.get(req.lower())
        if matched_col is None:
            missing.append(req)
        elif matched_col != req:
            rename_map[matched_col] = req

    if missing:
        raise ValueError(
            f"LMS data is missing required columns: {', '.join(missing)}. "
            f"Found columns: {', '.join(df.columns)}"
        )

    df = df.rename(columns=rename_map)

    df[BANK_COL_TXN_ID] = normalize_txn_id(df[BANK_COL_TXN_ID])
    df[BANK_COL_AMOUNT] = coerce_amount(df[BANK_COL_AMOUNT])
    df[BANK_COL_DATE] = coerce_date(df[BANK_COL_DATE])

    return df


def parse_bank_statement(file, column_map: dict) -> pd.DataFrame:
    """Read bank statement file and apply user-provided column mapping."""
    df = read_file_to_df(file)
    return apply_bank_mapping(df, column_map)


def parse_lms_files(files: list) -> pd.DataFrame:
    """Read all LMS files, validate required columns, concat into one DataFrame."""
    frames = []
    for f in files:
        df = read_file_to_df(f)
        df[LMS_COL_SOURCE_FILE] = f.name
        frames.append(df)

    if not frames:
        raise ValueError("No LMS files provided.")

    combined = pd.concat(frames, ignore_index=True)
    return apply_lms_cleaning(combined)
