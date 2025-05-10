# A Beautiful Football ETL pipelines with Apache Airflow

---

## Features

- Extracts and processes raw job offer descriptions (e.g. from scraped or uploaded data)
- Supports **English** sing spaCy’s `en_core_web_sm`
- Cleans and lemmatizes texts for further NLP or ML analysis
- Stores processed data in a structured **PostgreSQL** table
- Built to be **idempotent**, modular, and production-ready
- Uses Airflow **PostgresHook** and **Variables** for configuration and flexibility
- Fully orchestrated and scheduled via **Apache Airflow**

---

## Tech Stack

- **Apache Airflow**
- **Python 3.11**
- **spaCy**
- **PostgreSQL**
- **Docker**
- **Docker Compose**

---

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/nlp-job-offer-pipeline.git
cd nlp-job-offer-pipeline
### 2. Set up .env
```

### 2. Set up .env
```bash
AIRFLOW_VAR_DB_NAME=airflow
AIRFLOW_VAR_DB_USER=airflow
AIRFLOW_VAR_DB_PASSWORD=airflow
AIRFLOW_VAR_DB_HOST=postgres
AIRFLOW_VAR_DB_PORT=5432
```

### 3. Run Airflow with Astro
```bash
astro dev start
```


   
