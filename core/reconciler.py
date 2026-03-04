from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from utils.constants import (
    BANK_COL_TXN_ID, BANK_COL_AMOUNT, BANK_COL_BRAND,
    BANK_COL_STATUS, LMS_COL_TRANS_STATUS,
    LABEL_NOT_IN_LMS, LABEL_NOT_IN_BANK,
    CAT_MATCHED, CAT_AMOUNT_MISMATCH, CAT_BANK_ONLY, CAT_LMS_ONLY, CAT_DUPLICATE,
    LMS_COL_SOURCE_FILE,
)
from utils.helpers import extract_brand


@dataclass
class ReconciliationResult:
    matched: pd.DataFrame = field(default_factory=pd.DataFrame)
    amount_mismatch: pd.DataFrame = field(default_factory=pd.DataFrame)
    bank_only: pd.DataFrame = field(default_factory=pd.DataFrame)
    lms_only: pd.DataFrame = field(default_factory=pd.DataFrame)
    bank_duplicates: pd.DataFrame = field(default_factory=pd.DataFrame)
    lms_duplicates: pd.DataFrame = field(default_factory=pd.DataFrame)
    summary: dict = field(default_factory=dict)
    brand_summary: pd.DataFrame = field(default_factory=pd.DataFrame)
    status_cross_match: pd.DataFrame = field(default_factory=pd.DataFrame)
    total_bank: int = 0
    total_lms: int = 0


def find_duplicates(df: pd.DataFrame, label: str = "bank") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect duplicate TxnIDs. Returns (duplicates_df, deduplicated_df).
    The deduplicated version keeps the first occurrence.
    """
    dup_mask = df.duplicated(subset=[BANK_COL_TXN_ID], keep=False)
    duplicates = df[dup_mask].copy()
    deduped = df.drop_duplicates(subset=[BANK_COL_TXN_ID], keep="first").copy()
    return duplicates, deduped


def reconcile(bank_df: pd.DataFrame, lms_df: pd.DataFrame) -> ReconciliationResult:
    """
    Full reconciliation: dedup, merge, classify, summarize.
    """
    result = ReconciliationResult()
    result.total_bank = len(bank_df)
    result.total_lms = len(lms_df)

    # Step 1: Find duplicates in bank
    bank_dupes, bank_deduped = find_duplicates(bank_df, "bank")
    result.bank_duplicates = bank_dupes

    # Also check LMS duplicates (warning only)
    lms_dupes, lms_deduped = find_duplicates(lms_df, "lms")
    result.lms_duplicates = lms_dupes

    # Step 2: Outer merge on TxnID
    merged = pd.merge(
        bank_deduped,
        lms_deduped,
        on=BANK_COL_TXN_ID,
        how="outer",
        indicator=True,
        suffixes=("_Bank", "_LMS"),
    )

    # Step 3: Classify
    both_mask = merged["_merge"] == "both"
    bank_only_mask = merged["_merge"] == "left_only"
    lms_only_mask = merged["_merge"] == "right_only"

    # For rows present in both, compare amounts
    both_rows = merged[both_mask].copy()
    if not both_rows.empty:
        amt_match = np.isclose(
            both_rows["Amount_Bank"].fillna(0),
            both_rows["Amount_LMS"].fillna(0),
            atol=0.01,
        )
        result.matched = both_rows[amt_match].drop(columns=["_merge"]).copy()
        result.amount_mismatch = both_rows[~amt_match].drop(columns=["_merge"]).copy()
        # Add a Difference column for mismatches
        if not result.amount_mismatch.empty:
            result.amount_mismatch["Difference"] = (
                result.amount_mismatch["Amount_Bank"].fillna(0)
                - result.amount_mismatch["Amount_LMS"].fillna(0)
            )

    result.bank_only = merged[bank_only_mask].drop(columns=["_merge"]).copy()
    result.lms_only = merged[lms_only_mask].drop(columns=["_merge"]).copy()

    # Step 4: Summary metrics
    result.summary = {
        "Total Bank Transactions": result.total_bank,
        "Total LMS Transactions": result.total_lms,
        "Bank Duplicates": len(bank_dupes),
        "LMS Duplicates": len(lms_dupes),
        "Matched": len(result.matched),
        "Amount Mismatches": len(result.amount_mismatch),
        "Bank Only": len(result.bank_only),
        "LMS Only": len(result.lms_only),
        "Match Rate (%)": (
            round(len(result.matched) / len(bank_deduped) * 100, 2)
            if len(bank_deduped) > 0 else 0
        ),
        "Matched Amount (Bank)": result.matched["Amount_Bank"].sum() if not result.matched.empty else 0,
        "Mismatch Amount (Bank)": result.amount_mismatch["Amount_Bank"].sum() if not result.amount_mismatch.empty else 0,
        "Bank Only Amount": result.bank_only["Amount_Bank"].sum() if not result.bank_only.empty else 0,
        "LMS Only Amount": result.lms_only["Amount_LMS"].sum() if not result.lms_only.empty else 0,
    }

    # Step 5: Brand summary
    result.brand_summary = build_brand_summary(result)

    # Step 6: Status cross-match
    result.status_cross_match = build_status_cross_match(result)

    return result


def build_brand_summary(result: ReconciliationResult) -> pd.DataFrame:
    """Group each category by brand (first 2 chars of TxnID) and aggregate."""
    rows = []

    def _add_brand_rows(df, category, amount_col):
        if df.empty:
            return
        brands = extract_brand(df[BANK_COL_TXN_ID])
        for brand, group in df.groupby(brands):
            rows.append({
                "Brand": brand,
                "Category": category,
                "Count": len(group),
                "Total Amount": group[amount_col].sum() if amount_col in group.columns else 0,
            })

    _add_brand_rows(result.matched, CAT_MATCHED, "Amount_Bank")
    _add_brand_rows(result.amount_mismatch, CAT_AMOUNT_MISMATCH, "Amount_Bank")
    _add_brand_rows(result.bank_only, CAT_BANK_ONLY, "Amount_Bank")
    _add_brand_rows(result.lms_only, CAT_LMS_ONLY, "Amount_LMS")
    _add_brand_rows(result.bank_duplicates, CAT_DUPLICATE, BANK_COL_AMOUNT)

    if not rows:
        return pd.DataFrame(columns=["Brand", "Category", "Count", "Total Amount"])

    summary = pd.DataFrame(rows)
    # Pivot for a cleaner view: brands as rows, categories as column groups
    pivot = summary.pivot_table(
        index="Brand",
        columns="Category",
        values=["Count", "Total Amount"],
        aggfunc="sum",
        fill_value=0,
    )
    # Flatten multi-level columns
    pivot.columns = [f"{val} ({cat})" for val, cat in pivot.columns]
    pivot = pivot.reset_index().sort_values("Brand")
    return pivot


def build_status_cross_match(result: ReconciliationResult) -> pd.DataFrame:
    """Group transactions by Bank Status, LMS TransStatus, and Brand (first char of TxnID)."""
    frames = []

    def _prepare(df, bank_status_default=None, lms_status_default=None, amount_col="Amount_Bank"):
        if df.empty:
            return
        chunk = pd.DataFrame()
        chunk[BANK_COL_TXN_ID] = df[BANK_COL_TXN_ID]
        # Bank status
        if BANK_COL_STATUS + "_Bank" in df.columns:
            chunk["Bank Status"] = df[BANK_COL_STATUS + "_Bank"].fillna(bank_status_default or "")
        elif BANK_COL_STATUS in df.columns:
            chunk["Bank Status"] = df[BANK_COL_STATUS].fillna(bank_status_default or "")
        else:
            chunk["Bank Status"] = bank_status_default or ""
        # LMS TransStatus
        if LMS_COL_TRANS_STATUS + "_LMS" in df.columns:
            chunk["LMS TransStatus"] = df[LMS_COL_TRANS_STATUS + "_LMS"].fillna(lms_status_default or "")
        elif LMS_COL_TRANS_STATUS in df.columns:
            chunk["LMS TransStatus"] = df[LMS_COL_TRANS_STATUS].fillna(lms_status_default or "")
        else:
            chunk["LMS TransStatus"] = lms_status_default or ""
        # Brand = first character of TxnID
        chunk["Brand"] = df[BANK_COL_TXN_ID].astype(str).str[:1].str.upper()
        # Amount: prefer Amount_Bank, fallback to Amount_LMS, then Amount
        if amount_col in df.columns:
            chunk["Amount"] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)
        elif "Amount_LMS" in df.columns:
            chunk["Amount"] = pd.to_numeric(df["Amount_LMS"], errors="coerce").fillna(0)
        elif BANK_COL_AMOUNT in df.columns:
            chunk["Amount"] = pd.to_numeric(df[BANK_COL_AMOUNT], errors="coerce").fillna(0)
        else:
            chunk["Amount"] = 0
        frames.append(chunk)

    _prepare(result.matched)
    _prepare(result.amount_mismatch)
    _prepare(result.bank_only, lms_status_default=LABEL_NOT_IN_LMS)
    _prepare(result.lms_only, bank_status_default=LABEL_NOT_IN_BANK, amount_col="Amount_LMS")

    if not frames:
        return pd.DataFrame(columns=["Bank Status", "LMS TransStatus", "Brand", "Count", "Total Amount"])

    combined = pd.concat(frames, ignore_index=True)
    grouped = (
        combined
        .groupby(["Bank Status", "LMS TransStatus", "Brand"], dropna=False)
        .agg(Count=("Amount", "size"), **{"Total Amount": ("Amount", "sum")})
        .reset_index()
        .sort_values(["Bank Status", "LMS TransStatus", "Brand"])
    )
    return grouped
