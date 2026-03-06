import logging
from pyspark.sql import DataFrame

def run_data_quality_checks(df: DataFrame, logger: logging.Logger) -> None:
    """
    Executes mandatory data quality assertions on the incoming DataFrame.
    Raises ValueError if any assertion fails.
    """
    logger.info("Running Data Quality Checks on Raw Data...")
    
    # 1. Check for empty dataframe
    row_count = df.count()
    if row_count == 0:
        logger.error("Data Quality Check Failed: DataFrame is empty.")
        raise ValueError("DataFrame is empty.")
        
    # 2. Check for required columns
    required_cols = {"Text", "Sentiment", "Platform", "Likes", "Retweets", "Country", "Hour"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        logger.error(f"Data Quality Check Failed: Missing essential columns: {missing_cols}")
        raise ValueError(f"Missing essential columns: {missing_cols}")
        
    # 3. Check for negative engagement values (which should be mathematically impossible)
    negative_engagements = df.filter((df.Likes < 0) | (df.Retweets < 0)).count()
    if negative_engagements > 0:
        logger.error(f"Data Quality Check Failed: Found {negative_engagements} rows with negative likes or retweets.")
        raise ValueError("Negative engagement metrics found.")

    logger.info("All Data Quality Checks passed successfully.")
