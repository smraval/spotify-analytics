
import os, json, time, datetime, urllib.parse, urllib.request, base64
import boto3

S3_BUCKET = os.environ["RAW_BUCKET"]
SECRET_ID = os.environ.get("SPOTIFY_SECRET_ID", "spotify/oauth")

secrets = boto3.client("secretsmanager")
s3 = boto3.client("s3")

TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE  = "https://api.spotify.com/v1"

def _secret():
    return json.loads(secrets.get_secret_value(SecretId=SECRET_ID)["SecretString"])

def _refresh_access_token():
    sec = _secret()
    data = urllib.parse.urlencode({
        "grant_type":"refresh_token",
        "refresh_token": sec["refresh_token"]
    }).encode("utf-8")
    creds = base64.b64encode(f"{sec['client_id']}:{sec['client_secret']}".encode()).decode()
    req = urllib.request.Request(TOKEN_URL, data=data, method="POST",
                                 headers={"Authorization": f"Basic {creds}",
                                          "Content-Type":"application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode())
    return body["access_token"]

def _get(path, token, params=None):
    url = f"{API_BASE}{path}"
    if params: url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    while True:
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(int(e.headers.get("Retry-After","2")) + 1); continue
            raise

def _today_parts():
    now = datetime.datetime.utcnow().date()
    return str(now.year), f"{now.month:02d}", f"{now.day:02d}", now.isoformat()

def _put_jsonl(objs, key):
    body = "\n".join(json.dumps(o, separators=(",",":")) for o in objs) + "\n"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

def handler(event, context):
    token = _refresh_access_token()
    print(f"✓ Got access token")

    # 1) recently played (up to last ~50 plays)
    recents = _get("/me/player/recently-played", token, {"limit":50}).get("items", [])
    print(f"✓ Got {len(recents)} recent tracks")

    # 2) top tracks (medium term)
    top = _get("/me/top/tracks", token, {"limit":50, "time_range":"medium_term"}).get("items", [])
    print(f"✓ Got {len(top)} top tracks")

    # 3) get user profile information
    print("✓ Getting user profile...")
    try:
        profile = _get("/me", token)
        print(f"✓ Got profile for user: {profile.get('display_name', 'Unknown')}")
    except Exception as e:
        print(f"⚠️ Error getting profile: {e}")
        profile = {}

    # 4) get followed artists 
    print("✓ Getting followed artists...")
    followed_artists = []
    try:
        followed = _get("/me/following", token, {"type": "artist", "limit": 50})
        items = followed.get("artists", {}).get("items", [])
        followed_artists.extend(items)
        
        # pagination
        next_url = followed.get("artists", {}).get("next")
        pages = 1
        while next_url and pages < 3:  # Limit to 3 pages to avoid timeout
            print(f"✓ Getting followed artists page {pages + 1}...")
            if "after=" in next_url:
                after_cursor = next_url.split("after=")[1].split("&")[0]
                followed = _get("/me/following", token, {"type": "artist", "limit": 50, "after": after_cursor})
                items = followed.get("artists", {}).get("items", [])
                followed_artists.extend(items)
                next_url = followed.get("artists", {}).get("next")
                pages += 1
            else:
                break
        
        print(f"✓ Got {len(followed_artists)} followed artists across {pages} page(s)")
        
    except Exception as e:
        error_msg = str(e)
        if "403" in error_msg or "Forbidden" in error_msg:
            print(f"⚠️ Followed artists requires 'user-follow-read' scope. Current error: {e}")
            print("⚠️ Please re-generate your refresh token with the missing scope")
        else:
            print(f"⚠️ Error getting followed artists: {e}")
        followed_artists = []

    # 5) more top artists
    print("✓ Getting top artists...")
    try:
        top_artists = _get("/me/top/artists", token, {"limit": 50, "time_range": "medium_term"}).get("items", [])
        print(f"✓ Got {len(top_artists)} top artists")
    except Exception as e:
        print(f"⚠️ Error getting top artists: {e}")
        top_artists = []

    out_recent = []
    for it in recents:
        tr = it.get("track", {})
        album = tr.get("album", {})
        artists = tr.get("artists", [])
        
        out_recent.append({
            "source": "recently_played",
            "played_at": it.get("played_at"),
            "track_id": tr.get("id"),
            "track_name": tr.get("name"),
            "artist_name": ", ".join(a["name"] for a in artists),
            "artist_id": artists[0]["id"] if artists else None,
            "album_name": album.get("name"),
            "album_id": album.get("id"),
            "release_date": album.get("release_date"),
            "track_popularity": tr.get("popularity"),
            "track_duration_ms": tr.get("duration_ms"),
            "track_explicit": tr.get("explicit"),
            "available_markets": tr.get("available_markets", []),
            "user_country": profile.get("country"),
            "user_followers": profile.get("followers", {}).get("total")
        })

    out_top_tracks = []
    snap = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    for tr in top:
        album = tr.get("album", {})
        artists = tr.get("artists", [])
        
        out_top_tracks.append({
            "source": "top_tracks_medium_term",
            "snapshot_ts": snap,
            "track_id": tr.get("id"),
            "track_name": tr.get("name"),
            "artist_name": ", ".join(a["name"] for a in artists),
            "artist_id": artists[0]["id"] if artists else None,
            "album_name": album.get("name"),
            "album_id": album.get("id"),
            "release_date": album.get("release_date"),
            "track_popularity": tr.get("popularity"),
            "track_duration_ms": tr.get("duration_ms"),
            "track_explicit": tr.get("explicit"),
            "available_markets": tr.get("available_markets", []),
            "user_country": profile.get("country"),
            "user_followers": profile.get("followers", {}).get("total")
        })

    out_top_artists = []
    for artist in top_artists:
        out_top_artists.append({
            "source": "top_artists_medium_term",
            "snapshot_ts": snap,
            "artist_id": artist.get("id"),
            "artist_name": artist.get("name"),
            "artist_popularity": artist.get("popularity"),
            "artist_followers": artist.get("followers", {}).get("total"),
            "artist_genres": artist.get("genres", []),
            "user_country": profile.get("country"),
            "user_followers": profile.get("followers", {}).get("total")
        })

    out_followed_artists = []
    for artist in followed_artists:
        out_followed_artists.append({
            "source": "followed_artists",
            "snapshot_ts": snap,
            "artist_id": artist.get("id"),
            "artist_name": artist.get("name"),
            "artist_popularity": artist.get("popularity"),
            "artist_followers": artist.get("followers", {}).get("total"),
            "artist_genres": artist.get("genres", []),
            "user_country": profile.get("country"),
            "user_followers": profile.get("followers", {}).get("total")
        })

    # save to s3
    y,m,d,iso = _today_parts()
    keys = []
    
    if out_recent:
        key_recent = f"raw/{y}/{m}/{d}/recently_played_{iso}.jsonl"
        _put_jsonl(out_recent, key_recent)
        keys.append(key_recent)
    
    if out_top_tracks:
        key_top_tracks = f"raw/{y}/{m}/{d}/top_tracks_{iso}.jsonl"
        _put_jsonl(out_top_tracks, key_top_tracks)
        keys.append(key_top_tracks)
    
    if out_top_artists:
        key_top_artists = f"raw/{y}/{m}/{d}/top_artists_{iso}.jsonl"
        _put_jsonl(out_top_artists, key_top_artists)
        keys.append(key_top_artists)
    
    if out_followed_artists:
        key_followed = f"raw/{y}/{m}/{d}/followed_artists_{iso}.jsonl"
        _put_jsonl(out_followed_artists, key_followed)
        keys.append(key_followed)

    return {
        "recent_count": len(out_recent),
        "top_tracks_count": len(out_top_tracks), 
        "top_artists_count": len(out_top_artists),
        "followed_artists_count": len(out_followed_artists),
        "keys": keys
    }
