import pytest
from pyspark.sql import Row
from src.jobs.sentiment_analysis import (
    get_top_sentiments,
    get_sentiment_distribution,
    get_country_analysis,
    get_hourly_sentiment_trends,
    get_popular_platforms
)

@pytest.fixture
def sample_data(spark_session):
    data = [
        Row(Sentiment="Positive", Likes=20.0, Retweets=10.0, Platform="Twitter", Country="USA", Hour=10),
        Row(Sentiment="Positive", Likes=30.0, Retweets=15.0, Platform="Facebook", Country="USA", Hour=10),
        Row(Sentiment="Negative", Likes=5.0, Retweets=2.0, Platform="Twitter", Country="UK", Hour=12),
        Row(Sentiment="Neutral", Likes=10.0, Retweets=5.0, Platform="Instagram", Country="Canada", Hour=12),
        Row(Sentiment="Positive", Likes=40.0, Retweets=20.0, Platform="Twitter", Country="USA", Hour=14),
    ]
    return spark_session.createDataFrame(data)

def test_get_top_sentiments(sample_data):
    df_result = get_top_sentiments(sample_data)
    results = df_result.collect()
    
    assert len(results) == 3
    # Positive: (20+30+40)/3 = 30 Avg Likes
    assert results[0]["Sentiment"] == "Positive"
    assert results[0]["avg_likes"] == 30.0

def test_get_sentiment_distribution(sample_data):
    df_result = get_sentiment_distribution(sample_data)
    results = df_result.toPandas()
    
    # Twitter should have 2 Positive entries
    twitter_pos = results[(results["Platform"] == "Twitter") & (results["Sentiment"] == "Positive")]
    assert twitter_pos.iloc[0]["sentiment_count"] == 2

def test_get_country_analysis(sample_data):
    df_result = get_country_analysis(sample_data)
    results = df_result.collect()
    
    # USA Avg Likes: (20+30+40)/3 = 30
    usa_result = next(row for row in results if row["Country"] == "USA")
    assert usa_result["avg_likes"] == 30.0

def test_get_hourly_sentiment_trends(sample_data):
    df_result = get_hourly_sentiment_trends(sample_data)
    results = df_result.toPandas()
    
    # Hour 10 has 2 Positive entries
    hour_10_pos = results[(results["Hour"] == 10) & (results["Sentiment"] == "Positive")]
    assert hour_10_pos.iloc[0]["sentiment_count"] == 2

def test_get_popular_platforms(sample_data):
    df_result = get_popular_platforms(sample_data)
    results = df_result.collect()
    
    # Twitter Avg Engagement (Likes + Retweets): 
    # Row1: 30, Row2: 7, Row3: 60 -> avg(30, 7, 60) = 32.333
    twitter_result = next(row for row in results if row["Platform"] == "Twitter")
    assert round(twitter_result["avg_engagement"], 2) == 32.33
