"""
POST /api/report
Accepts JSON body (optionally gzip-compressed) with bank_data, lms_data, column_map.
Returns an Excel file download.
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
from core.reporter import generate_excel_report


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

            column_map = body.get("column_map", {})
            bank_df = apply_bank_mapping(pd.DataFrame(body["bank_data"]), column_map)
            lms_df = apply_lms_cleaning(pd.DataFrame(body["lms_data"]))
            result = reconcile(bank_df, lms_df)
            excel_buf = generate_excel_report(result)

            data = excel_buf.getvalue()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", "attachment; filename=reconciliation_report.xlsx")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        except Exception as e:
            body = json.dumps({"error": str(e), "trace": traceback.format_exc()}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format, *args):
        pass
