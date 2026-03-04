"""
POST /api/preview
Accepts a bank statement file upload and returns column names + first 5 rows.
Used for column mapping UI.
"""
import json
import sys
import os
import io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from http.server import BaseHTTPRequestHandler
import cgi
from utils.helpers import read_file_to_df


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

            bank_item = form["bank_file"]
            if not bank_item.filename:
                self._json_response(400, {"error": "No file uploaded"})
                return

            f = _UploadedFile(bank_item.filename, bank_item.file.read())
            df = read_file_to_df(f)

            # Convert preview rows - handle dates
            preview = df.head(5).copy()
            for col in preview.select_dtypes(include=["datetime64", "datetimetz"]).columns:
                preview[col] = preview[col].astype(str)

            self._json_response(200, {
                "columns": list(df.columns),
                "row_count": len(df),
                "col_count": len(df.columns),
                "preview": preview.fillna("").to_dict(orient="records"),
            })

        except Exception as e:
            self._json_response(500, {"error": str(e)})

    def _json_response(self, status, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
