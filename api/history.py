"""
GET /api/history
Returns recent reconciliation runs from Neon.
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from http.server import BaseHTTPRequestHandler
from utils.database import db_is_configured, get_run_history, init_db


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if not db_is_configured():
                self._json_response(200, {"runs": [], "message": "Database not configured"})
                return

            init_db()
            runs = get_run_history()
            self._json_response(200, {"runs": runs})

        except Exception as e:
            self._json_response(500, {"error": str(e)})

    def _json_response(self, status, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
