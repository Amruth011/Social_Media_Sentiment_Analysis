import os
import json
import argparse
from src.utils.spark_utils import get_spark_session, get_logger
from src.jobs.sentiment_analysis import (
    get_top_sentiments,
    get_sentiment_distribution,
    get_country_analysis,
    get_hourly_sentiment_trends,
    get_popular_platforms
)

def main():
    parser = argparse.ArgumentParser(description="Social Media Sentiment Analysis PySpark Job")
    parser.add_argument("--config", default="config/config.json", help="Path to config.json file")
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config_data = json.load(f)

    app_name = config_data.get("app_name", "SocialMediaSentimentAnalysis")
    logger = get_logger(app_name)
    spark = get_spark_session(app_name)
    
    logger.info("Initializing Spark Application: %s", app_name)
    
    input_path = config_data["data_paths"]["input"]
    logger.info("Reading input data from: %s", input_path)

    try:
        # Load data and clean
        df = spark.read.csv(input_path, header=True, inferSchema=True) \
            .dropDuplicates() \
            .na.drop()
        logger.info("Successfully read input data. Row count after cleaning: %d", df.count())
    except Exception as e:
        logger.error("Failed to read or clean data. Error: %s", str(e))
        spark.stop()
        raise e

    logger.info("Starting transformations...")

    try:
        # Transformation 1
        logger.info("Running get_top_sentiments...")
        top_sentiments_df = get_top_sentiments(df)
        top_sentiments_path = config_data["data_paths"]["output_top_sentiments"]
        top_sentiments_df.write.mode("overwrite").csv(top_sentiments_path, header=True)
        
        # Transformation 2
        logger.info("Running get_sentiment_distribution...")
        sentiment_dist_df = get_sentiment_distribution(df)
        sentiment_dist_path = config_data["data_paths"]["output_sentiment_platform"]
        sentiment_dist_df.write.mode("overwrite").csv(sentiment_dist_path, header=True)
        
        # Transformation 3
        logger.info("Running get_country_analysis...")
        country_analysis_df = get_country_analysis(df)
        country_analysis_path = config_data["data_paths"]["output_country_analysis"]
        country_analysis_df.write.mode("overwrite").csv(country_analysis_path, header=True)
        
        # Transformation 4
        logger.info("Running get_hourly_sentiment_trends...")
        hourly_sentiment_df = get_hourly_sentiment_trends(df)
        hourly_sentiment_path = config_data["data_paths"]["output_hourly_sentiment"]
        hourly_sentiment_df.write.mode("overwrite").csv(hourly_sentiment_path, header=True)
        
        # Transformation 5
        logger.info("Running get_popular_platforms...")
        platform_engagement_df = get_popular_platforms(df)
        platform_engagement_path = config_data["data_paths"]["output_platform_engagement"]
        platform_engagement_df.write.mode("overwrite").csv(platform_engagement_path, header=True)

        logger.info("Successfully completed all transformations and data writes.")

    except Exception as e:
        logger.error("Error during transformation or writing process. Error: %s", str(e))
        raise e
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()

if __name__ == '__main__':
    main()
