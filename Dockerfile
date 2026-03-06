FROM jupyter/pyspark-notebook:latest

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY config/ config/
COPY tests/ tests/
COPY SocialMediaSentimentAnalysiscsv.csv SocialMediaSentimentAnalysiscsv.csv

# Defining the command needed to run the main pyspark script
ENTRYPOINT ["python", "-m", "src.main"]
