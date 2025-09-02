CREATE OR REPLACE VIEW spotify_raw_db.v_curated AS
SELECT 
    dt,
    region,
    source,
    -- Track data (for recently_played and top_tracks)
    track_id,
    track_name,
    artist_name,
    artist_id,
    album_name,
    album_id,
    release_date,
    release_year,
    track_popularity,
    track_duration_minutes,
    is_explicit,
    popularity_tier,
    played_at,
    snapshot_ts,
    -- Artist data (for top_artists and followed_artists)
    artist_popularity,
    artist_followers,
    primary_genre,
    genre_count,
    -- User context
    user_country,
    user_followers
FROM spotify_raw_db.processed;

-- View for listening patterns (recently played tracks only)
CREATE OR REPLACE VIEW spotify_raw_db.v_listening_patterns AS
SELECT 
    dt,
    region,
    COUNT(*) as total_plays,
    COUNT(DISTINCT track_id) as unique_tracks,
    COUNT(DISTINCT artist_name) as unique_artists,
    AVG(track_duration_minutes) as avg_track_duration,
    AVG(track_popularity) as avg_track_popularity,
    SUM(CASE WHEN is_explicit = true THEN 1 ELSE 0 END) as explicit_tracks,
    COUNT(CASE WHEN popularity_tier = 'High' THEN 1 END) as high_popularity_tracks,
    COUNT(CASE WHEN popularity_tier = 'Medium' THEN 1 END) as medium_popularity_tracks,
    COUNT(CASE WHEN popularity_tier = 'Low' THEN 1 END) as low_popularity_tracks
FROM spotify_raw_db.processed
WHERE source = 'recently_played'
GROUP BY dt, region
ORDER BY dt DESC;

-- View for top artists analysis
CREATE OR REPLACE VIEW spotify_raw_db.v_top_artists_analysis AS
SELECT 
    artist_name,
    artist_id,
    artist_popularity,
    artist_followers,
    primary_genre,
    genre_count,
    user_country,
    snapshot_ts
FROM spotify_raw_db.processed
WHERE source = 'top_artists_medium_term'
ORDER BY artist_popularity DESC;

-- View for followed artists analysis  
CREATE OR REPLACE VIEW spotify_raw_db.v_followed_artists AS
SELECT 
    artist_name,
    artist_id,
    artist_popularity,
    artist_followers,
    primary_genre,
    genre_count,
    user_country,
    snapshot_ts
FROM spotify_raw_db.processed
WHERE source = 'followed_artists'
ORDER BY artist_followers DESC;

-- View for track popularity analysis
CREATE OR REPLACE VIEW spotify_raw_db.v_track_popularity AS
SELECT 
    track_name,
    artist_name,
    album_name,
    track_popularity,
    popularity_tier,
    release_year,
    track_duration_minutes,
    is_explicit,
    COUNT(*) as play_count
FROM spotify_raw_db.processed
WHERE source IN ('recently_played', 'top_tracks_medium_term')
GROUP BY track_name, artist_name, album_name, track_popularity, 
         popularity_tier, release_year, track_duration_minutes, is_explicit
ORDER BY track_popularity DESC, play_count DESC;

-- View for genre analysis (from artist data)
CREATE OR REPLACE VIEW spotify_raw_db.v_genre_analysis AS
SELECT 
    primary_genre,
    source,
    COUNT(*) as artist_count,
    AVG(artist_popularity) as avg_popularity,
    AVG(artist_followers) as avg_followers,
    MAX(artist_followers) as max_followers,
    user_country
FROM spotify_raw_db.processed
WHERE source IN ('top_artists_medium_term', 'followed_artists')
  AND primary_genre IS NOT NULL
GROUP BY primary_genre, source, user_country
ORDER BY artist_count DESC;

-- View for temporal analysis (listening over time)
CREATE OR REPLACE VIEW spotify_raw_db.v_temporal_analysis AS
SELECT 
    dt,
    EXTRACT(HOUR FROM CAST(played_at AS TIMESTAMP)) as hour_of_day,
    EXTRACT(DOW FROM dt) as day_of_week,
    COUNT(*) as plays_in_hour,
    COUNT(DISTINCT track_id) as unique_tracks_in_hour,
    AVG(track_duration_minutes) as avg_duration
FROM spotify_raw_db.processed
WHERE source = 'recently_played'
  AND played_at IS NOT NULL
GROUP BY dt, EXTRACT(HOUR FROM CAST(played_at AS TIMESTAMP)), EXTRACT(DOW FROM dt)
ORDER BY dt DESC, hour_of_day;

-- View for release year trends
CREATE OR REPLACE VIEW spotify_raw_db.v_release_year_trends AS
SELECT 
    release_year,
    COUNT(*) as track_count,
    COUNT(DISTINCT artist_name) as unique_artists,
    AVG(track_popularity) as avg_popularity,
    COUNT(CASE WHEN source = 'recently_played' THEN 1 END) as recent_plays,
    COUNT(CASE WHEN source = 'top_tracks_medium_term' THEN 1 END) as top_tracks
FROM spotify_raw_db.processed
WHERE source IN ('recently_played', 'top_tracks_medium_term')
  AND release_year IS NOT NULL
  AND release_year >= 1950  -- Filter out invalid years
GROUP BY release_year
ORDER BY release_year DESC;

-- View for user context analysis
CREATE OR REPLACE VIEW spotify_raw_db.v_user_context AS
SELECT 
    user_country,
    user_followers,
    dt,
    source,
    COUNT(*) as record_count,
    COUNT(DISTINCT CASE WHEN source LIKE '%track%' THEN track_id END) as unique_tracks,
    COUNT(DISTINCT CASE WHEN source LIKE '%artist%' THEN artist_id END) as unique_artists
FROM spotify_raw_db.processed
GROUP BY user_country, user_followers, dt, source
ORDER BY dt DESC;