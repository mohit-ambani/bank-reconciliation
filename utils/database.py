import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

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
    """Get a Neon Postgres connection using DATABASE_URL env var."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url)


def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(INIT_SQL)
    conn.commit()
    conn.close()


def save_run(summary: dict, brand_summary_json: str) -> int:
    """Save a reconciliation run to the database. Returns the run id."""
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


def get_run_history(limit: int = 20) -> list:
    """Fetch recent reconciliation runs."""
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
    return [dict(r) for r in rows]


def db_is_configured() -> bool:
    return bool(os.environ.get("DATABASE_URL"))
