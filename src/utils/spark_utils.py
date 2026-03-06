import logging
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """
    Initializes and returns a PySpark session.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def get_logger(app_name: str) -> logging.Logger:
    """
    Initializes a standard Python logger.
    """
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.INFO)
    
    # Create console handler and set level to info
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Add formatter to ch
    ch.setFormatter(formatter)
    
    # Add ch to logger if it doesn't already have handlers
    if not logger.handlers:
        logger.addHandler(ch)

    return logger
