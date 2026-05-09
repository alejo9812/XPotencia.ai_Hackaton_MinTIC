from __future__ import annotations

from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import os


ROOT = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", "4176"))


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in {"/api/summary", "/api/summary.json"}:
            payload = (ROOT / "api" / "summary.json").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/":
            self.path = "/index.html"
        return super().do_GET()


if __name__ == "__main__":
    os.chdir(ROOT)
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Dashboard listo en http://127.0.0.1:{PORT}")
    print(f"API disponible en http://127.0.0.1:{PORT}/api/summary")
    server.serve_forever()
