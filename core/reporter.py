from io import BytesIO
import pandas as pd
from core.reconciler import ReconciliationResult
from utils.constants import (
    SHEET_SUMMARY, SHEET_BRAND_SUMMARY, SHEET_MATCHED,
    SHEET_AMOUNT_MISMATCH, SHEET_BANK_ONLY, SHEET_LMS_ONLY, SHEET_DUPLICATES,
)


def _auto_adjust_columns(writer, sheet_name, df):
    """Auto-adjust column widths based on content."""
    worksheet = writer.sheets[sheet_name]
    for i, col in enumerate(df.columns):
        max_len = max(
            df[col].astype(str).map(len).max() if len(df) > 0 else 0,
            len(str(col)),
        ) + 2
        max_len = min(max_len, 50)  # Cap width
        worksheet.column_dimensions[chr(65 + i) if i < 26 else f"A{chr(65 + i - 26)}"].width = max_len


def _set_column_widths(worksheet, df):
    """Set column widths using openpyxl column letter utilities."""
    from openpyxl.utils import get_column_letter
    for i, col in enumerate(df.columns, 1):
        max_len = max(
            df[col].astype(str).map(len).max() if len(df) > 0 else 0,
            len(str(col)),
        ) + 2
        max_len = min(max_len, 50)
        worksheet.column_dimensions[get_column_letter(i)].width = max_len


def generate_excel_report(result: ReconciliationResult) -> BytesIO:
    """
    Generate a multi-sheet Excel report from reconciliation results.
    Returns a BytesIO buffer ready for download.
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Sheet 1: Summary
        summary_df = pd.DataFrame(
            list(result.summary.items()),
            columns=["Metric", "Value"],
        )
        summary_df.to_excel(writer, sheet_name=SHEET_SUMMARY, index=False)
        _set_column_widths(writer.sheets[SHEET_SUMMARY], summary_df)

        # Sheet 2: Brand Summary
        if not result.brand_summary.empty:
            result.brand_summary.to_excel(writer, sheet_name=SHEET_BRAND_SUMMARY, index=False)
            _set_column_widths(writer.sheets[SHEET_BRAND_SUMMARY], result.brand_summary)
        else:
            pd.DataFrame({"Info": ["No brand data"]}).to_excel(
                writer, sheet_name=SHEET_BRAND_SUMMARY, index=False
            )

        # Sheet 3-7: Detail sheets
        detail_sheets = [
            (SHEET_MATCHED, result.matched),
            (SHEET_AMOUNT_MISMATCH, result.amount_mismatch),
            (SHEET_BANK_ONLY, result.bank_only),
            (SHEET_LMS_ONLY, result.lms_only),
            (SHEET_DUPLICATES, result.bank_duplicates),
        ]

        for sheet_name, df in detail_sheets:
            if df.empty:
                placeholder = pd.DataFrame({"Info": [f"No {sheet_name.lower()} records"]})
                placeholder.to_excel(writer, sheet_name=sheet_name, index=False)
                _set_column_widths(writer.sheets[sheet_name], placeholder)
            else:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                _set_column_widths(writer.sheets[sheet_name], df)

    output.seek(0)
    return output
