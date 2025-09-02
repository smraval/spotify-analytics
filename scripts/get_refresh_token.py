"""
get_refresh_token.py
--------------------
Run this locally to generate a Spotify refresh token.

Usage:
  export SPOTIFY_CLIENT_ID=your_client_id
  export SPOTIFY_CLIENT_SECRET=your_client_secret
  python3 get_refresh_token.py

Then open the printed URL, log into Spotify, approve access,
and copy the refresh token displayed in your browser.
"""

import base64
import http.server
import json
import os
import socketserver
import urllib.parse
import urllib.request

CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]

# must match exactly the redirect URI in your Spotify app settings
REDIRECT_URI = "http://127.0.0.1:8080/callback"

# scopes needed for your project
SCOPES = "user-read-recently-played user-top-read user-follow-read"

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            query = urllib.parse.urlparse(self.path).query
            params = dict(urllib.parse.parse_qsl(query))
            code = params.get("code")

            # exchange authorization code for tokens
            data = urllib.parse.urlencode({
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI
            }).encode("utf-8")

            creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
            req = urllib.request.Request(
                TOKEN_URL,
                data=data,
                method="POST",
                headers={
                    "Authorization": f"Basic {creds}",
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            )

            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read().decode())

            refresh = body.get("refresh_token")
            access = body.get("access_token")

            # display refresh token in browser
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Spotify auth complete.<br>")
            self.wfile.write(b"<strong>Refresh token:</strong><br>")
            self.wfile.write(refresh.encode() + b"<br><br>")
            self.wfile.write(b"Copy this string and save it into AWS Secrets Manager.<br>")
            self.wfile.write(b"You can close this tab now.")

            print("\n=== SUCCESS ===")
            print("Refresh token:", refresh)
            print("Access token:", access)
            print("================\n")
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    params = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES
    })

    print("\nOpen this URL in your browser:\n")
    print(f"{AUTH_URL}?{params}\n")

    with socketserver.TCPServer(("127.0.0.1", 8080), Handler) as httpd:
        print("Waiting on http://127.0.0.1:8080/callback ...")
        httpd.serve_forever()
