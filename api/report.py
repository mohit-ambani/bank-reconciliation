"""
POST /api/report
Accepts the same input as /api/reconcile but returns an Excel file download.
"""
import json
import sys
import os
import io
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from http.server import BaseHTTPRequestHandler
import cgi
from core.parser import parse_bank_statement, parse_lms_files
from core.reconciler import reconcile
from core.reporter import generate_excel_report


class _UploadedFile:
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._buf = io.BytesIO(data)
    def read(self, *a):
        return self._buf.read(*a)
    def seek(self, *a):
        return self._buf.seek(*a)


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_type = self.headers.get("Content-Type", "")
            env = {
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": content_type,
                "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
            }
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=env)

            column_map = json.loads(form.getvalue("column_map"))

            bank_item = form["bank_file"]
            bank_file = _UploadedFile(bank_item.filename, bank_item.file.read())

            lms_items = form["lms_files"]
            if not isinstance(lms_items, list):
                lms_items = [lms_items]
            lms_files = [_UploadedFile(i.filename, i.file.read()) for i in lms_items if i.filename]

            bank_df = parse_bank_statement(bank_file, column_map)
            lms_df = parse_lms_files(lms_files)
            result = reconcile(bank_df, lms_df)
            excel_buf = generate_excel_report(result)

            body = excel_buf.getvalue()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", "attachment; filename=reconciliation_report.xlsx")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        except Exception as e:
            body = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format, *args):
        pass
