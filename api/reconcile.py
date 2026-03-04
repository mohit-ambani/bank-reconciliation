"""
POST /api/reconcile
Accepts JSON body (optionally gzip-compressed) with:
  - bank_data: list of row dicts (parsed client-side)
  - lms_data: list of row dicts (parsed client-side)
  - column_map: dict mapping canonical names to original column names
Returns JSON with reconciliation results.
"""
import json
import sys
import os
import gzip
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from http.server import BaseHTTPRequestHandler
import pandas as pd
from core.parser import apply_bank_mapping, apply_lms_cleaning
from core.reconciler import reconcile
from utils.database import db_is_configured, init_db, save_run


def _df_to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-serializable list of dicts."""
    if df.empty:
        return []
    out = df.copy()
    for col in out.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        out[col] = out[col].astype(str)
    return out.fillna("").to_dict(orient="records")


def _read_body(handler):
    """Read request body, decompressing gzip if needed."""
    length = int(handler.headers.get("Content-Length", 0))
    raw = handler.rfile.read(length)
    if handler.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw)


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            body = _read_body(self)

            bank_rows = body.get("bank_data")
            lms_rows = body.get("lms_data")
            column_map = body.get("column_map")

            if not bank_rows:
                self._json_response(400, {"error": "No bank data provided"})
                return
            if not lms_rows:
                self._json_response(400, {"error": "No LMS data provided"})
                return
            if not column_map:
                self._json_response(400, {"error": "Missing column_map"})
                return

            bank_df = apply_bank_mapping(pd.DataFrame(bank_rows), column_map)
            lms_df = apply_lms_cleaning(pd.DataFrame(lms_rows))
            result = reconcile(bank_df, lms_df)

            # Save to Neon if configured
            run_id = None
            if db_is_configured():
                try:
                    init_db()
                    brand_json = result.brand_summary.to_json() if not result.brand_summary.empty else "{}"
                    run_id = save_run(result.summary, brand_json)
                except Exception:
                    pass

            # Cap detail rows to stay under Vercel 4.5MB response limit
            ROW_LIMIT = 1000

            response = {
                "summary": result.summary,
                "brand_summary": _df_to_records(result.brand_summary),
                "matched": _df_to_records(result.matched.head(ROW_LIMIT)),
                "matched_total": len(result.matched),
                "amount_mismatch": _df_to_records(result.amount_mismatch.head(ROW_LIMIT)),
                "amount_mismatch_total": len(result.amount_mismatch),
                "bank_only": _df_to_records(result.bank_only.head(ROW_LIMIT)),
                "bank_only_total": len(result.bank_only),
                "lms_only": _df_to_records(result.lms_only.head(ROW_LIMIT)),
                "lms_only_total": len(result.lms_only),
                "bank_duplicates": _df_to_records(result.bank_duplicates.head(ROW_LIMIT)),
                "bank_duplicates_total": len(result.bank_duplicates),
                "status_cross_match": _df_to_records(result.status_cross_match),
                "status_txn_map": result.status_txn_map,
                "bank_success_lms_fail": result.bank_success_lms_fail,
                "lms_duplicate_count": len(result.lms_duplicates),
                "run_id": run_id,
            }

            # Safety: drop status_txn_map if response exceeds 4MB
            body_bytes = json.dumps(response, default=str).encode()
            if len(body_bytes) > 4 * 1024 * 1024:
                response["status_txn_map"] = {}

            self._json_response(200, response)

        except Exception as e:
            self._json_response(500, {"error": str(e), "trace": traceback.format_exc()})

    def _json_response(self, status, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
