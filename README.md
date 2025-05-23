# A Beautiful Football ETL pipelines with Apache Airflow

---

## Features

- Extracts football data from [API-Football](https://www.api-football.com/)
- Automatically creates normalized Oracle DB tables (if not present)
- Loads and transforms data using Python and SQL
- Performs incremental loads using Airflow Variables and XComs
- Searches existing DB data to avoid redundant API calls
- Designed to be idempotent and referentially consistent
- Fully automated and scheduled via Airflow

---

## Tech Stack

- **Astronomer (Local Dev CLI via Astro)**
- **Apache Airflow**
- **Python**
- **Oracle Database**
- **Docker** 
- **API-Football (public API)**

---

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/football-etl-pipeline.git
cd football-etl-pipeline
```

### 2. Set up .env
 
```bash
export AIRFLOW_VAR_API_KEY=YOUR_API_KEY
export AIRFLOW_VAR_BASE_URL=https://v3.football.api-sports.io
export AIRFLOW_VAR_DATA_BASE_USERNAME=airflow
export AIRFLOW_VAR_DATA_BASE_PASSWORD=airflow
export AIRFLOW_VAR_DATA_BASE_HOST=oracle-db
export AIRFLOW_VAR_DATA_BASE_PORT=1521 
export AIRFLOW_VAR_DATA_BASE_SERVICE_NAME=XEPDB1
```

### 3. Run Airflow with Astro
```bash
astro dev start
```

### 4. Add network (must be done)
```bash
docker network connect football-stats-app_a20170_airflow oracle-db
```
   
