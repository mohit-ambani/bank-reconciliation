"""
POST /api/reconcile
Accepts multipart form data with:
  - bank_file: single CSV/Excel file
  - lms_files: one or more CSV/Excel files
  - column_map: JSON string mapping canonical names to original column names
Returns JSON with reconciliation results.
"""
import json
import sys
import os
import io
import traceback

# Add project root to path so imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from http.server import BaseHTTPRequestHandler
import cgi
import pandas as pd
from core.parser import parse_bank_statement, parse_lms_files
from core.reconciler import reconcile
from utils.database import db_is_configured, init_db, save_run


class _UploadedFile:
    """Adapter to mimic Streamlit's UploadedFile interface for our parser."""
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._buf = io.BytesIO(data)
    def read(self, *a):
        return self._buf.read(*a)
    def seek(self, *a):
        return self._buf.seek(*a)
    def tell(self):
        return self._buf.tell()
    def seekable(self):
        return True


def _df_to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-serializable list of dicts."""
    if df.empty:
        return []
    # Convert timestamps to strings
    out = df.copy()
    for col in out.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        out[col] = out[col].astype(str)
    return out.fillna("").to_dict(orient="records")


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_type = self.headers.get("Content-Type", "")
            if "multipart/form-data" not in content_type:
                self._json_response(400, {"error": "Expected multipart/form-data"})
                return

            # Parse multipart form
            env = {
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": content_type,
                "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
            }
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=env)

            # Get column_map
            column_map_raw = form.getvalue("column_map")
            if not column_map_raw:
                self._json_response(400, {"error": "Missing column_map"})
                return
            column_map = json.loads(column_map_raw)

            # Get bank file
            bank_item = form["bank_file"]
            if not bank_item.filename:
                self._json_response(400, {"error": "Missing bank_file"})
                return
            bank_file = _UploadedFile(bank_item.filename, bank_item.file.read())

            # Get LMS files
            lms_items = form["lms_files"]
            if not isinstance(lms_items, list):
                lms_items = [lms_items]
            lms_files = []
            for item in lms_items:
                if item.filename:
                    lms_files.append(_UploadedFile(item.filename, item.file.read()))

            if not lms_files:
                self._json_response(400, {"error": "No LMS files provided"})
                return

            # Run reconciliation
            bank_df = parse_bank_statement(bank_file, column_map)
            lms_df = parse_lms_files(lms_files)
            result = reconcile(bank_df, lms_df)

            # Save to Neon if configured
            run_id = None
            if db_is_configured():
                try:
                    init_db()
                    brand_json = result.brand_summary.to_json() if not result.brand_summary.empty else "{}"
                    run_id = save_run(result.summary, brand_json)
                except Exception:
                    pass  # Don't fail reconciliation if DB save fails

            response = {
                "summary": result.summary,
                "brand_summary": _df_to_records(result.brand_summary),
                "matched": _df_to_records(result.matched),
                "amount_mismatch": _df_to_records(result.amount_mismatch),
                "bank_only": _df_to_records(result.bank_only),
                "lms_only": _df_to_records(result.lms_only),
                "bank_duplicates": _df_to_records(result.bank_duplicates),
                "status_cross_match": _df_to_records(result.status_cross_match),
                "lms_duplicate_count": len(result.lms_duplicates),
                "run_id": run_id,
            }

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
        pass  # Suppress logs
