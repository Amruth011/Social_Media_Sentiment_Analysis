# Social Media Sentiment Analysis - Production PySpark Job

This repository contains a production-ready PySpark application for analyzing social media sentiment data. The project was refactored from a Jupyter Notebook into a standard, scalable, and testable PySpark project structure.

## Project Structure
```text
├── config/
│   └── config.json           # Input paths and PySpark configurations
├── src/
│   ├── jobs/
│   │   └── sentiment_analysis.py  # Core Spark DataFrame transformations
│   ├── utils/
│   │   └── spark_utils.py    # SparkSession initialization and logging
│   └── main.py               # Application entry point
├── tests/
│   ├── conftest.py           # PyTest fixtures (local SparkSession)
│   └── test_sentiment_analysis.py # Unit tests for the jobs
├── data/
│   └── output/               # Default output directory for Spark jobs
├── Dockerfile                # Container execution environment
├── requirements.txt          # Python dependencies
└── SocialMediaSentimentAnalysiscsv.csv # Source data
```

## Setup and Installation

### Local Execution
1. Ensure you have Python 3.8+ and Java 8/11 installed for PySpark.
2. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

### Running the PySpark Job
To execute the pipeline locally, run the main entry point:
```bash
python -m src.main
```
This will read the `SocialMediaSentimentAnalysiscsv.csv` file, perform 5 distinct analytical transformations, and write the partitioned CSV output into the `data/output/` directory.

### Running Unit Tests
Correctness of the DataFrame aggregations is verified using `pytest`:
```bash
pytest tests/
```

## Docker Containerization
For execution on clusters or in isolated environments, build the Docker container:
```bash
docker build -t sentiment_analysis_job .
```

To run the job inside the container:
```bash
docker run -it sentiment_analysis_job
```

## Analysis Outputs
The pipeline generates the following distributed datasets in `data/output/`:
1. **Top Sentiments** (`output_top_sentiments`): Top 10 Sentiments by average Likes and Retweets.
2. **Sentiment Distribution** (`output_sentiment_platform`): Sentiment count segmented by social media platform.
3. **Country Analysis** (`output_country_analysis`): Average engagement metrics grouped by country.
4. **Hourly Trends** (`output_hourly_sentiment`): Sentiment fluctuations across different hours of the day.
5. **Platform Engagement** (`output_platform_engagement`): Overall popularity based on total likes and retweets per platform.
