from pyspark.sql import SparkSession, functions as F

RAW_BUCKET  = "spotify-analytics-smraval-raw"   
PROC_BUCKET = "spotify-analytics-smraval-proc"  

spark = SparkSession.builder.appName("spotify-transform").getOrCreate()

src = f"s3://{RAW_BUCKET}/raw/*/*/*/*.jsonl"
df  = spark.read.json(src)

df = (df
    .withColumn("dt", F.to_date(F.coalesce(F.col("played_at"), F.col("snapshot_ts"))))
    .withColumn("region", F.coalesce(F.col("user_country"), F.element_at("available_markets", 1)))
    # Add derived columns for analytics
    .withColumn("track_duration_minutes", F.col("track_duration_ms") / 60000.0)
    .withColumn("release_year", F.year(F.to_date(F.col("release_date"))))
    .withColumn("is_explicit", F.col("track_explicit"))
    .withColumn("popularity_tier", 
        F.when(F.col("track_popularity") >= 80, "High")
         .when(F.col("track_popularity") >= 60, "Medium") 
         .when(F.col("track_popularity") >= 40, "Low")
         .otherwise("Unknown"))
    .withColumn("primary_genre", F.element_at("artist_genres", 1))
    .withColumn("genre_count", F.size("artist_genres"))
    .drop("available_markets")
)

out = f"s3://{PROC_BUCKET}/processed/"
(df.repartition("dt", "source", "region")
   .write.mode("overwrite")
   .format("parquet")
   .partitionBy("dt", "source", "region")
   .save(out))