import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """
    Fixture for creating a local SparkSession for testing purposes.
    """
    spark = SparkSession.builder \
        .appName("SentimentAnalysisTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()
