# Beautiful Football ETL with Apache Airflow

A beautiful, automated ETL pipeline built with **Apache Airflow** that extracts football data from a public API, creates relational tables in an **Oracle Database**, and loads cleaned, structured data.

It supports **incremental updates** via Airflow Variables and XComs, ensures **referential integrity**, and runs on a **scheduled basis**.

------------

## Features

- Extracts football data from [API-Football](https://www.api-football.com/)
- Creates normalized Oracle DB tables automatically (if they don’t exist)
- Loads and transforms data using Python and SQL
- Handles incremental loads with Airflow Variables/XComs
- Designed to be idempotent and referentially consistent
- Fully automated and scheduled using Airflow

------------

## Tech Stack

- **Apache Airflow**
- **Python**
- **Oracle Database**
- **Docker** (optional, for local dev)
- **API-Football (public API)**

------------

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/football-etl-pipeline.git
cd football-etl-pipeline

