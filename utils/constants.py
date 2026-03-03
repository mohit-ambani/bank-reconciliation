# Canonical column names used internally after mapping
BANK_COL_TXN_ID = "TxnID"
BANK_COL_AMOUNT = "Amount"
BANK_COL_DATE = "Date"
BANK_COL_DESCRIPTION = "Description"
BANK_COL_BRAND = "Brand"

# The columns the user must map from their bank statement
BANK_REQUIRED_FIELDS = [BANK_COL_TXN_ID, BANK_COL_AMOUNT, BANK_COL_DATE, BANK_COL_DESCRIPTION]

# LMS files are expected to have these columns (case-insensitive match)
LMS_REQUIRED_COLUMNS = ["TxnID", "Amount", "Date"]

# Column added to LMS data to track which file each row came from
LMS_COL_SOURCE_FILE = "SourceFile"

# Brand extraction: number of leading characters from TxnID
BRAND_PREFIX_LENGTH = 2

# Category labels used in reconciliation output
CAT_MATCHED = "Matched"
CAT_AMOUNT_MISMATCH = "Amount Mismatch"
CAT_BANK_ONLY = "Bank Only"
CAT_LMS_ONLY = "LMS Only"
CAT_DUPLICATE = "Bank Duplicate"

# Excel report sheet names
SHEET_SUMMARY = "Summary"
SHEET_BRAND_SUMMARY = "Brand Summary"
SHEET_MATCHED = "Matched"
SHEET_AMOUNT_MISMATCH = "Amount Mismatch"
SHEET_BANK_ONLY = "Bank Only"
SHEET_LMS_ONLY = "LMS Only"
SHEET_DUPLICATES = "Bank Duplicates"
