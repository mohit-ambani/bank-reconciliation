import json
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

INIT_SQL = """
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP NOT NULL DEFAULT NOW(),
    total_bank INT,
    total_lms INT,
    matched INT,
    amount_mismatches INT,
    bank_only INT,
    lms_only INT,
    bank_duplicates INT,
    match_rate NUMERIC(5,2),
    matched_amount NUMERIC(15,2),
    mismatch_amount NUMERIC(15,2),
    bank_only_amount NUMERIC(15,2),
    lms_only_amount NUMERIC(15,2),
    brand_summary JSONB
);
"""


def get_connection():
    """Get a Neon Postgres connection using Streamlit secrets."""
    return psycopg2.connect(st.secrets["DATABASE_URL"])


def init_db():
    """Create tables if they don't exist."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)
        conn.commit()
        conn.close()
    except Exception:
        pass  # Silently fail if DB not configured


def save_run(summary: dict, brand_summary_json: str):
    """Save a reconciliation run to the database."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reconciliation_runs
                    (total_bank, total_lms, matched, amount_mismatches,
                     bank_only, lms_only, bank_duplicates, match_rate,
                     matched_amount, mismatch_amount, bank_only_amount,
                     lms_only_amount, brand_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    summary.get("Total Bank Transactions", 0),
                    summary.get("Total LMS Transactions", 0),
                    summary.get("Matched", 0),
                    summary.get("Amount Mismatches", 0),
                    summary.get("Bank Only", 0),
                    summary.get("LMS Only", 0),
                    summary.get("Bank Duplicates", 0),
                    summary.get("Match Rate (%)", 0),
                    float(summary.get("Matched Amount (Bank)", 0)),
                    float(summary.get("Mismatch Amount (Bank)", 0)),
                    float(summary.get("Bank Only Amount", 0)),
                    float(summary.get("LMS Only Amount", 0)),
                    brand_summary_json,
                ),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        return run_id
    except Exception as e:
        st.warning(f"Could not save to database: {e}")
        return None


def get_run_history(limit: int = 20) -> list:
    """Fetch recent reconciliation runs."""
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, run_date, total_bank, total_lms, matched,
                       amount_mismatches, bank_only, lms_only, bank_duplicates,
                       match_rate, matched_amount
                FROM reconciliation_runs
                ORDER BY run_date DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def db_is_configured() -> bool:
    """Check if DATABASE_URL is present in secrets."""
    try:
        _ = st.secrets["DATABASE_URL"]
        return True
    except Exception:
        return False
