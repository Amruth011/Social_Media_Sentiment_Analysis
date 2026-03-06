import os
import json
import argparse
from pyspark.sql.functions import col
from src.utils.spark_utils import get_spark_session, get_logger
from src.utils.data_quality import run_data_quality_checks
from src.utils.udfs import text_length_udf
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
        # Load data, clean, and apply Feature Engineering UDF
        df = spark.read.csv(input_path, header=True, inferSchema=True) \
            .dropDuplicates() \
            .na.drop() \
            .withColumn("Text_Length_Category", text_length_udf(col("Text")))
            
        logger.info("Successfully read input data and engineered Text_Length_Category. Row count: %d", df.count())
        
        # Enforce Data Quality Assertions
        run_data_quality_checks(df, logger)
        
    except Exception as e:
        logger.error("Failed to read or clean data. Error: %s", str(e))
        spark.stop()
        raise e

    logger.info("Starting transformations and saving as Delta Tables...")

    try:
        # Transformation 1
        logger.info("Running get_top_sentiments...")
        top_sentiments_df = get_top_sentiments(df)
        top_sentiments_path = config_data["data_paths"]["output_top_sentiments"]
        top_sentiments_df.write.format("delta").mode("overwrite").save(top_sentiments_path)
        
        # Transformation 2
        logger.info("Running get_sentiment_distribution...")
        sentiment_dist_df = get_sentiment_distribution(df)
        sentiment_dist_path = config_data["data_paths"]["output_sentiment_platform"]
        sentiment_dist_df.write.format("delta").mode("overwrite").save(sentiment_dist_path)
        
        # Transformation 3
        logger.info("Running get_country_analysis...")
        country_analysis_df = get_country_analysis(df)
        country_analysis_path = config_data["data_paths"]["output_country_analysis"]
        country_analysis_df.write.format("delta").mode("overwrite").save(country_analysis_path)
        
        # Transformation 4
        logger.info("Running get_hourly_sentiment_trends...")
        hourly_sentiment_df = get_hourly_sentiment_trends(df)
        hourly_sentiment_path = config_data["data_paths"]["output_hourly_sentiment"]
        hourly_sentiment_df.write.format("delta").mode("overwrite").save(hourly_sentiment_path)
        
        # Transformation 5
        logger.info("Running get_popular_platforms...")
        platform_engagement_df = get_popular_platforms(df)
        platform_engagement_path = config_data["data_paths"]["output_platform_engagement"]
        platform_engagement_df.write.format("delta").mode("overwrite").save(platform_engagement_path)

        logger.info("Successfully completed all transformations and data writes to Delta.")

    except Exception as e:
        logger.error("Error during transformation or writing process. Error: %s", str(e))
        raise e
    finally:
        logger.info("Stopping Spark session.")
        spark.stop()

if __name__ == '__main__':
    main()
