import json
import streamlit as st
import pandas as pd
from utils.constants import BANK_REQUIRED_FIELDS, LMS_REQUIRED_COLUMNS
from utils.helpers import read_file_to_df
from ui.components import render_column_mapper
from core.parser import parse_bank_statement, parse_lms_files
from core.reconciler import reconcile
from utils.database import db_is_configured, save_run


def render_upload_page():
    st.header("Upload Files")

    # --- Bank Statement Section ---
    st.subheader("1. Bank Statement")
    bank_file = st.file_uploader(
        "Upload bank statement (CSV or Excel)",
        type=["csv", "xlsx", "xls"],
        key="bank_upload",
    )

    column_map = None
    bank_preview_df = None

    if bank_file is not None:
        try:
            bank_preview_df = read_file_to_df(bank_file)
            st.success(f"Loaded **{len(bank_preview_df):,}** rows, **{len(bank_preview_df.columns)}** columns")
            with st.expander("Preview first 5 rows", expanded=True):
                st.dataframe(bank_preview_df.head(5), use_container_width=True, hide_index=True)

            column_map = render_column_mapper(
                columns=list(bank_preview_df.columns),
                required_fields=BANK_REQUIRED_FIELDS,
            )
        except Exception as e:
            st.error(f"Error reading bank statement: {e}")

    st.divider()

    # --- LMS Files Section ---
    st.subheader("2. LMS System Files")
    lms_files = st.file_uploader(
        "Upload LMS files (CSV or Excel, multiple allowed)",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
        key="lms_upload",
    )

    if lms_files:
        st.success(f"**{len(lms_files)}** LMS file(s) uploaded")
        for f in lms_files:
            try:
                preview = read_file_to_df(f)
                st.caption(f"  {f.name}: {len(preview):,} rows")
                f.seek(0)  # Reset file pointer after preview read
            except Exception as e:
                st.warning(f"  {f.name}: Error - {e}")

    st.divider()

    # --- Run Reconciliation ---
    can_run = bank_file is not None and column_map is not None and len(lms_files) > 0

    if st.button("Run Reconciliation", type="primary", disabled=not can_run, use_container_width=True):
        with st.spinner("Running reconciliation..."):
            try:
                # Reset file pointers
                bank_file.seek(0)
                for f in lms_files:
                    f.seek(0)

                bank_df = parse_bank_statement(bank_file, column_map)
                lms_df = parse_lms_files(lms_files)
                result = reconcile(bank_df, lms_df)

                # Save to Neon if configured
                if db_is_configured():
                    brand_json = result.brand_summary.to_json() if not result.brand_summary.empty else "{}"
                    save_run(result.summary, brand_json)

                # Store in session state and navigate
                st.session_state["recon_result"] = result
                st.session_state["page"] = "results"
                st.rerun()

            except Exception as e:
                st.error(f"Reconciliation failed: {e}")
                st.exception(e)

    if not can_run:
        parts = []
        if bank_file is None:
            parts.append("bank statement")
        if column_map is None and bank_file is not None:
            parts.append("valid column mapping")
        if not lms_files:
            parts.append("LMS file(s)")
        if parts:
            st.info(f"Please upload: {', '.join(parts)}")
