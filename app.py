import streamlit as st
from utils.database import db_is_configured, init_db

st.set_page_config(
    page_title="Bank Reconciliation Tool",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Initialize database tables on first load
if db_is_configured():
    init_db()

st.title("Bank Reconciliation Tool")

# Initialize session state
if "page" not in st.session_state:
    st.session_state["page"] = "upload"

# Sidebar navigation
with st.sidebar:
    st.header("Navigation")
    if st.button("Upload & Reconcile", use_container_width=True):
        st.session_state["page"] = "upload"
        st.rerun()
    if st.button("View Results", use_container_width=True, disabled="recon_result" not in st.session_state):
        st.session_state["page"] = "results"
        st.rerun()
    if db_is_configured():
        if st.button("History", use_container_width=True):
            st.session_state["page"] = "history"
            st.rerun()

# Route to the correct page
if st.session_state["page"] == "results":
    from ui.results_page import render_results_page
    render_results_page()
elif st.session_state["page"] == "history":
    from ui.history_page import render_history_page
    render_history_page()
else:
    from ui.upload_page import render_upload_page
    render_upload_page()
