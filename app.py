# app.py
from http.server import BaseHTTPRequestHandler, HTTPServer

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Hello from Docker container!")

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 80), SimpleHandler)
    print("Server running on port 80...")
    server.serve_forever()

