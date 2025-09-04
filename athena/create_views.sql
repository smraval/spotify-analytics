-- Create curated view with enhanced data quality
CREATE OR REPLACE VIEW spotify.v_curated AS
SELECT 
    dt,
    track_name,
    artist_name,
    album_name,
    track_popularity,
    track_duration_minutes,
    popularity_tier,
    is_explicit,
    region,
    release_year,
    primary_genre,
    genre_count,
    played_at,
    source
FROM spotify.processed_parquet
WHERE track_name IS NOT NULL 
  AND artist_name IS NOT NULL
  AND release_year IS NOT NULL  -- Only include tracks with valid release years
  AND release_year <= YEAR(CURRENT_DATE)  -- Filter out future dates
  AND release_year >= 1900;  -- Filter out obviously wrong dates

-- Daily listening summary with valid release years only
CREATE OR REPLACE VIEW spotify.v_daily_listening AS
SELECT 
    dt,
    COUNT(*) as total_tracks,
    COUNT(DISTINCT track_name) as unique_tracks,
    COUNT(DISTINCT artist_name) as unique_artists,
    ROUND(AVG(track_popularity), 2) as avg_popularity,
    ROUND(AVG(track_duration_minutes), 2) as avg_duration,
    SUM(CASE WHEN is_explicit = true THEN 1 ELSE 0 END) as explicit_count,
    SUM(CASE WHEN popularity_tier = 'High' THEN 1 ELSE 0 END) as high_pop_tracks,
    SUM(CASE WHEN popularity_tier = 'Medium' THEN 1 ELSE 0 END) as medium_pop_tracks,
    SUM(CASE WHEN popularity_tier = 'Low' THEN 1 ELSE 0 END) as low_pop_tracks,
    region,
    -- Add decade analysis
    CASE 
        WHEN release_year >= 2020 THEN '2020s'
        WHEN release_year >= 2010 THEN '2010s'
        WHEN release_year >= 2000 THEN '2000s'
        WHEN release_year >= 1990 THEN '1990s'
        WHEN release_year >= 1980 THEN '1980s'
        ELSE 'Pre-1980s'
    END as release_decade
FROM spotify.processed_parquet
WHERE release_year IS NOT NULL 
  AND release_year <= YEAR(CURRENT_DATE)
  AND release_year >= 1900
GROUP BY dt, region, 
    CASE 
        WHEN release_year >= 2020 THEN '2020s'
        WHEN release_year >= 2010 THEN '2010s'
        WHEN release_year >= 2000 THEN '2000s'
        WHEN release_year >= 1990 THEN '1990s'
        WHEN release_year >= 1980 THEN '1980s'
        ELSE 'Pre-1980s'
    END
ORDER BY dt DESC;

-- Top artists analysis (from top_artists source)
CREATE OR REPLACE VIEW spotify.v_top_artists_analysis AS
SELECT 
    artist_name,
    artist_popularity,
    artist_followers,
    primary_genre,
    genre_count,
    region,
    snapshot_ts as analysis_date
FROM spotify.processed_parquet
WHERE source = 'top_artists'
  AND artist_name IS NOT NULL
  AND artist_popularity IS NOT NULL
ORDER BY artist_popularity DESC;

-- Track popularity distribution
CREATE OR REPLACE VIEW spotify.v_track_popularity_distribution AS
SELECT 
    popularity_tier,
    COUNT(*) as track_count,
    ROUND(AVG(track_popularity), 2) as avg_popularity,
    ROUND(AVG(track_duration_minutes), 2) as avg_duration,
    SUM(CASE WHEN is_explicit = true THEN 1 ELSE 0 END) as explicit_count,
    region
FROM spotify.processed_parquet
WHERE track_name IS NOT NULL
  AND release_year IS NOT NULL
  AND release_year <= YEAR(CURRENT_DATE)
GROUP BY popularity_tier, region
ORDER BY popularity_tier;

-- Genre trends with valid release years
CREATE OR REPLACE VIEW spotify.v_genre_trends AS
SELECT 
    primary_genre,
    release_year,
    COUNT(*) as track_count,
    ROUND(AVG(track_popularity), 2) as avg_popularity,
    region
FROM spotify.processed_parquet
WHERE primary_genre IS NOT NULL
  AND release_year IS NOT NULL
  AND release_year <= YEAR(CURRENT_DATE)
  AND release_year >= 1900
GROUP BY primary_genre, release_year, region
HAVING COUNT(*) >= 1
ORDER BY release_year DESC, track_count DESC;

-- User profile summary
CREATE OR REPLACE VIEW spotify.v_user_profile_summary AS
SELECT 
    user_followers,
    user_country,
    snapshot_ts as profile_date
FROM spotify.processed_parquet
WHERE source = 'user_profile'
  AND user_followers IS NOT NULL
LIMIT 1;

-- Followed artists summary
CREATE OR REPLACE VIEW spotify.v_followed_artists_summary AS
SELECT 
    artist_name,
    artist_popularity,
    artist_followers,
    primary_genre,
    genre_count,
    region,
    snapshot_ts as follow_date
FROM spotify.processed_parquet
WHERE source = 'followed_artists'
  AND artist_name IS NOT NULL
ORDER BY artist_popularity DESC;