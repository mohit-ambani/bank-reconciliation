"""
Local dev server that mimics Vercel routing:
  /api/*  → Python handlers in api/
  /*      → static files from public/
"""
import sys, os, importlib, io
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

PORT = 8000
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)


class DevHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.join(ROOT, "public"), **kwargs)

    def do_GET(self):
        if self.path.startswith("/api/"):
            self._route_api("GET")
        else:
            super().do_GET()

    def do_POST(self):
        if self.path.startswith("/api/"):
            self._route_api("POST")
        else:
            self.send_error(405)

    def _route_api(self, method):
        path = urlparse(self.path).path  # e.g. /api/reconcile
        name = path.strip("/").replace("/", ".")  # api.reconcile
        module_file = os.path.join(ROOT, path.strip("/") + ".py")

        if not os.path.isfile(module_file):
            self.send_error(404, f"No handler at {module_file}")
            return

        try:
            mod = importlib.import_module(name)
            importlib.reload(mod)  # always reload for dev
            handler_cls = getattr(mod, "handler")

            # Build a fake request object the Vercel-style handler expects
            fake = object.__new__(handler_cls)
            fake.headers = self.headers
            fake.rfile = self.rfile
            fake.wfile = self.wfile
            fake.requestline = self.requestline
            fake.command = method
            fake.request_version = self.request_version
            fake.client_address = self.client_address
            fake.server = self.server
            fake._headers_buffer = []
            fake.responses = self.responses

            if method == "POST":
                fake.do_POST()
            else:
                fake.do_GET()

        except Exception as e:
            import traceback
            self.send_error(500, str(e))
            traceback.print_exc()

    def log_message(self, format, *args):
        print(f"[{self.log_date_time_string()}] {format % args}")


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), DevHandler)
    print(f"Dev server running at http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
