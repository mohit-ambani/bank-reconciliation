import streamlit as st
from core.reconciler import ReconciliationResult
from core.reporter import generate_excel_report
from ui.components import render_dataframe_with_download
from utils.constants import (
    CAT_MATCHED, CAT_AMOUNT_MISMATCH, CAT_BANK_ONLY, CAT_LMS_ONLY, CAT_DUPLICATE,
)


def render_results_page():
    result: ReconciliationResult = st.session_state.get("recon_result")

    if result is None:
        st.warning("No reconciliation results found. Please upload files first.")
        if st.button("Go to Upload"):
            st.session_state["page"] = "upload"
            st.rerun()
        return

    st.header("Reconciliation Results")

    # --- Top Metrics Row ---
    s = result.summary
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Bank Txns", f"{s.get('Total Bank Transactions', 0):,}")
    col2.metric("Matched", f"{s.get('Matched', 0):,}", delta=f"{s.get('Match Rate (%)', 0)}%")
    col3.metric("Amount Mismatches", f"{s.get('Amount Mismatches', 0):,}")
    col4.metric("Bank Only", f"{s.get('Bank Only', 0):,}")
    col5.metric("LMS Only", f"{s.get('LMS Only', 0):,}")
    col6.metric("Bank Duplicates", f"{s.get('Bank Duplicates', 0):,}")

    # --- Amount Metrics ---
    st.subheader("Amount Summary")
    acol1, acol2, acol3, acol4 = st.columns(4)
    acol1.metric("Matched Amount", f"{s.get('Matched Amount (Bank)', 0):,.2f}")
    acol2.metric("Mismatch Amount", f"{s.get('Mismatch Amount (Bank)', 0):,.2f}")
    acol3.metric("Bank Only Amount", f"{s.get('Bank Only Amount', 0):,.2f}")
    acol4.metric("LMS Only Amount", f"{s.get('LMS Only Amount', 0):,.2f}")

    st.divider()

    # --- Download Report ---
    st.subheader("Download Report")
    report_bytes = generate_excel_report(result)
    st.download_button(
        label="Download Full Excel Report",
        data=report_bytes,
        file_name="reconciliation_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )

    st.divider()

    # --- Brand Summary ---
    st.subheader("Brand Summary")
    if not result.brand_summary.empty:
        st.dataframe(result.brand_summary, use_container_width=True, hide_index=True)
    else:
        st.info("No brand summary data available.")

    st.divider()

    # --- LMS Duplicate Warning ---
    if not result.lms_duplicates.empty:
        st.warning(f"Found **{len(result.lms_duplicates):,}** duplicate TxnIDs in LMS files. Review the Duplicates tab.")

    # --- Detail Tabs ---
    st.subheader("Detailed Results")
    tab_matched, tab_mismatch, tab_bank_only, tab_lms_only, tab_dupes = st.tabs([
        f"Matched ({len(result.matched):,})",
        f"Amount Mismatch ({len(result.amount_mismatch):,})",
        f"Bank Only ({len(result.bank_only):,})",
        f"LMS Only ({len(result.lms_only):,})",
        f"Duplicates ({len(result.bank_duplicates):,})",
    ])

    with tab_matched:
        render_dataframe_with_download(result.matched, CAT_MATCHED, "matched")

    with tab_mismatch:
        render_dataframe_with_download(result.amount_mismatch, CAT_AMOUNT_MISMATCH, "mismatch")

    with tab_bank_only:
        render_dataframe_with_download(result.bank_only, CAT_BANK_ONLY, "bank_only")

    with tab_lms_only:
        render_dataframe_with_download(result.lms_only, CAT_LMS_ONLY, "lms_only")

    with tab_dupes:
        render_dataframe_with_download(result.bank_duplicates, CAT_DUPLICATE, "dupes")

    st.divider()

    # --- Back Button ---
    if st.button("Back to Upload", use_container_width=True):
        st.session_state["page"] = "upload"
        st.rerun()
