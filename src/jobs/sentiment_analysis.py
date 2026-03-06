from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, avg, count

def get_top_sentiments(df: DataFrame) -> DataFrame:
    """
    Returns the top 10 Sentiments by average Likes and Retweets.
    """
    return df.groupBy("Sentiment") \
        .agg(
            avg(col("Likes")).alias("avg_likes"),
            avg(col("Retweets")).alias("avg_retweets")
        ) \
        .orderBy(desc("avg_likes")) \
        .limit(10)

def get_sentiment_distribution(df: DataFrame) -> DataFrame:
    """
    Returns Sentiment Distribution by Platform.
    """
    return df.groupBy("Platform", "Sentiment") \
        .agg(count("*").alias("sentiment_count")) \
        .orderBy(desc("sentiment_count"))

def get_country_analysis(df: DataFrame) -> DataFrame:
    """
    Returns Average Likes and Retweets by Country.
    """
    return df.groupBy("Country") \
        .agg(
            avg(col("Likes")).alias("avg_likes"),
            avg(col("Retweets")).alias("avg_retweets")
        ) \
        .orderBy(desc("avg_likes"))

def get_hourly_sentiment_trends(df: DataFrame) -> DataFrame:
    """
    Returns Sentiment Trends by Hour.
    """
    return df.groupBy("Hour", "Sentiment") \
        .agg(count("*").alias("sentiment_count")) \
        .orderBy("Hour")

def get_popular_platforms(df: DataFrame) -> DataFrame:
    """
    Returns Most Popular Platforms by Engagement (Likes + Retweets).
    """
    return df.groupBy("Platform") \
        .agg(avg(col("Likes") + col("Retweets")).alias("avg_engagement")) \
        .orderBy(desc("avg_engagement"))
