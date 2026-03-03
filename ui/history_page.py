import streamlit as st
import pandas as pd
from utils.database import db_is_configured, get_run_history


def render_history_page():
    st.header("Reconciliation History")

    if not db_is_configured():
        st.info("Database not configured. Add DATABASE_URL to Streamlit secrets to enable history.")
        return

    runs = get_run_history()

    if not runs:
        st.info("No reconciliation runs found yet.")
        return

    df = pd.DataFrame(runs)
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.strftime("%Y-%m-%d %H:%M")
    df = df.rename(columns={
        "id": "Run #",
        "run_date": "Date",
        "total_bank": "Bank Txns",
        "total_lms": "LMS Txns",
        "matched": "Matched",
        "amount_mismatches": "Mismatches",
        "bank_only": "Bank Only",
        "lms_only": "LMS Only",
        "bank_duplicates": "Duplicates",
        "match_rate": "Match %",
        "matched_amount": "Matched Amt",
    })

    st.dataframe(df, use_container_width=True, hide_index=True)
