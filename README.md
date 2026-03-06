<div align="center">

# 🌊 Social Media Sentiment Analysis — Enterprise PySpark Engine

### *Most tutorials write to CSV. This one builds a Delta Lakehouse.*

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-Data_Engineering-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Transactions-00A4E4?style=for-the-badge)](https://delta.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Live_Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI/CD-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

<br>

### 👉 [**View the Live Executive Dashboard →**](#) *(Link will be active once deployed on Streamlit Cloud)*

<br>

> **"Data Engineering isn't just about aggregating numbers—it's about reliability, scalability, and data quality."**
>
> *This project abandons the standard 'Jupyter Notebook to CSV' approach. It represents a fully modularized, production-grade PySpark pipeline featuring Delta Lake architectures, custom UDF feature engineering, fail-fast data quality assertions, and automated CI/CD.*

</div>

---

## 📋 Table of Contents

- [Project Mission](#-project-mission)
- [Architecture Workflow](#️-architecture-workflow)
- [What Makes This Different](#-what-makes-this-different)
- [Delta Lake Analysis Outputs](#-delta-lake-analysis-outputs)
- [Tech Stack](#️-tech-stack)
- [Project Structure](#-project-structure)
- [Setup and Installation (Local)](#️-setup-and-installation)
- [Docker](#-ci-cd-and-docker)
- [Author](#-author)

---

## 🎯 Project Mission

Translating messy social media sentiment data into structured insights is typically done through isolated, fragile scripts. This repository demonstrates how to build an **Enterprise-Grade Batch Pipeline** that validates data on entry, enriches it via Python User-Defined Functions (UDFs), performs distributed aggregations, and sinks the results into ACID-compliant Delta tables.

**Built end-to-end: from raw CSV → Data Quality validation → UDF feature engineering → PySpark distributed processing → Delta Lake storage → GitHub Actions testing.**

---

## 🏗️ Architecture Workflow

```mermaid
graph TD
    A[Raw Source Data\nSocialMediaSentimentAnalysiscsv.csv] -->|PySpark Read| B(Spark Session)
    
    subgraph Data Quality & Feature Engineering
        B --> C{Data Quality Assertions\nNon-null, No Negatives}
        C -- Fail --> D[Raise Error / Stop]
        C -- Pass --> E[Apply Python UDF\nText Length Categorization]
    end
    
    subgraph Distributed Transformations
        E --> F[get_top_sentiments]
        E --> G[get_sentiment_distribution]
        E --> H[get_country_analysis]
        E --> I[get_hourly_sentiment_trends]
        E --> J[get_popular_platforms]
    end
    
    subgraph Delta Lake Data Sinks
        F -->|Write Delta| K[(data/output/top_sentiments)]
        G -->|Write Delta| L[(data/output/sentiment_platform)]
        H -->|Write Delta| M[(data/output/country_analysis)]
        I -->|Write Delta| N[(data/output/hourly_sentiment)]
        J -->|Write Delta| O[(data/output/platform_engagement)]
    end
    
    style A fill:#f9f,stroke:#333,stroke-width:2px,color:#000
    style B fill:#fcf,stroke:#333,stroke-width:2px,color:#000
    style K fill:#6cf,stroke:#333,stroke-width:2px,color:#000
    style L fill:#6cf,stroke:#333,stroke-width:2px,color:#000
    style M fill:#6cf,stroke:#333,stroke-width:2px,color:#000
    style N fill:#6cf,stroke:#333,stroke-width:2px,color:#000
    style O fill:#6cf,stroke:#333,stroke-width:2px,color:#000
```

---

## 💡 What Makes This Different

| Feature | This Project | Typical Student Project |
|---|---|---|
| **Data Storage** | ✅ Delta Lake (ACID, Time-Travel) | ❌ Flat CSV files |
| **Code Structure** | ✅ Modularized OOP Python functions | ❌ Single linear Jupyter Notebook |
| **Quality Control** | ✅ Pre-processing assertions (Fail-fast) | ❌ No schema validation |
| **Automation** | ✅ Makefile abstracting commands | ❌ Manual execution execution |
| **Testing** | ✅ Unit tested via PyTest and Mock Data | ❌ Not tested |
| **CI/CD** | ✅ GitHub Actions automated remote testing | ❌ None |
| **Deployment** | ✅ Dockerized for cluster deployment | ❌ Local execution only |
| **Front-End UI** | ✅ Interactive Streamlit Dashboard (`app.py`) | ❌ No business-facing visuals |
| **Configuration** | ✅ Externalized `config.json` | ❌ Hardcoded file paths |

---

## 📦 Delta Lake Analysis Outputs

Unlike standard tutorials, this pipeline does not write slow, unreliable CSVs. It generates transactional, ACID-compliant **Delta tables** in the `data/output/` directory:

1. **Top Sentiments** (`output_top_sentiments`): Top 10 Sentiments by average Likes and Retweets.
2. **Sentiment Distribution** (`output_sentiment_platform`): Sentiment count segmented by platform.
3. **Country Analysis** (`output_country_analysis`): Average engagement metrics grouped by country.
4. **Hourly Trends** (`output_hourly_sentiment`): Sentiment fluctuations across different hours of the day.
5. **Platform Engagement** (`output_platform_engagement`): Popularity based on total likes and retweets per platform.

---

## 🛠️ Tech Stack

| Category | Tool | Purpose |
|---|---|---|
| Language | Python 3.9+ / Java 11 | Core |
| Big Data Engine | PySpark | Distributed data processing |
| Storage Format | Delta Lake | Reliable, ACID data sinks |
| Pipeline Automation | Makefile | Simplified execution |
| Testing | PyTest | Asserting logic correctness |
| CI/CD | GitHub Actions | Remote test automation |
| Container | Docker | Reproducible deployment |
| Configuration | JSON | Parameter management |

---

## 📁 Project Structure

```text
├── .github/workflows/
│   └── spark_ci.yml          # GitHub Actions CI/CD Pipeline
├── config/
│   └── config.json           # Input paths and PySpark configurations
├── src/
│   ├── jobs/
│   │   └── sentiment_analysis.py  # Core Spark DataFrame transformations
│   ├── utils/
│   │   ├── spark_utils.py    # SparkSession logic (Delta injected)
│   │   ├── data_quality.py   # Row & schema validations
│   │   └── udfs.py           # Custom Feature Engineering
│   └── main.py               # Application entry point
├── tests/
│   ├── conftest.py           # PyTest fixtures (local SparkSession)
│   └── test_sentiment_analysis.py # Unit tests for the jobs
├── data/
│   └── output/               # Local Delta Lake tables
├── Dockerfile                # Container execution environment
├── Makefile                  # Local automation commands
└── requirements.txt          # Python dependencies
```

---

## ⚙️ Setup and Installation

### Local Execution (Via Makefile)
This project uses a Makefile to abstract execution logic. Make sure you have Python 3.8+ and Java 11 installed.

1. **Install Dependencies:**
   ```bash
   make install
   ```

2. **Run the Delta Pipeline:**
   Reads the source data, enforces Data Quality, applies UDFs, runs 5 distributed jobs, and generates Delta Lake directories locally.
   ```bash
   make run
   ```

3. **Run Unit Tests:**
   Verifies functional correctness of distributed logic using mocked DataFrames.
   ```bash
   make test
   ```

4. **Clean Workspace:**
   Removes built Delta directories and PySpark metastores.
   ```bash
   make clean
   ```

---

## 🛳️ CI/CD and Docker

- **Continuous Integration (CI):** Whenever code is pushed to the `main` branch, a GitHub Action spins up an Ubuntu environment, installs Java and PySpark, and runs the `pytest` suite automatically.
- **Docker:** Build the container for cloud cluster deployment (Kubernetes, AWS EMR, etc.):
  ```bash
  docker build -t sentiment_analysis_job .
  docker run -it sentiment_analysis_job
  ```

---

## ☁️ Cloud Deployment (Databricks)

While the project includes a `Dockerfile` for containerized execution on any cloud environment, this architecture is explicitly designed to be deployed natively on **Databricks**. 

To deploy this in a real-world enterprise:
1. **Compute:** Provision a Databricks Job Cluster (e.g., using Databricks Runtime 13.3 LTS Machine Learning).
2. **Orchestration:** Upload the `src/main.py` entry point to Databricks Workflows.
3. **Execution:** Databricks naturally supports the exact Delta Lake write operations we've coded, seamlessly sinking our processed DataFrames directly into cloud storage (AWS S3 or Azure Data Lake) as Delta Tables for BI Dashboards and ML Models to consume.

---

## 👤 Author

<div align="center">

**Amruth Kumar M**

B.Tech — Artificial Intelligence & Data Science  
REVA University, Bengaluru | Data Science Intern @ iStudio

[![GitHub](https://img.shields.io/badge/GitHub-Amruth011-181717?style=for-the-badge&logo=github)](https://github.com/Amruth011)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin)](https://linkedin.com/in/amruth-kumar-m)

</div>

---

<div align="center">

*Built from scratch to demonstrate Production-Quality PySpark Data Engineering.*

**Python • PySpark • Delta Lake • Docker • GitHub Actions**
</div>
