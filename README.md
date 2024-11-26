# ETL webserver log to PostgreSQL with Apache Airflow

This repository demonstrates an **ETL (Extract, Transform, Load)** pipeline using **Apache Airflow**. The pipeline processes web server log data, extracts relevant information, transforms the data, and loads it into a **PostgreSQL database**. This project aims to showcase how to orchestrate and automate data workflows using Airflow.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Pipeline Workflows](#pipeline-workflows)
4. [DAG Graph](#DAG-graphs)
5. [Requirements](#requirements)

## Project Overview

Objective:
To build an automated ETL pipeline using Apache Airflow to process web server logs, transform the data, and load it into a PostgreSQL database for further processing.

Why This Project?
- Demonstrates knowledge of ETL principles.
- Utilizes Python, PostgreSQL, and Airflow, key tools for data engineering.
- Practical application for log analysis and database management.

## Key Features
1. *Automated ETL Pipeline*: Uses Apache Airflow to automatically process web server logs, transform the data, and load it into a PostgreSQL database.
2. *Error Handling & Retries*: Includes task retries and failure handling to ensure the pipeline runs smoothly even if tasks fail.
3. *PostgreSQL Integration*: Transformed data is loaded into PostgreSQL for easy querying and analysis.
4. *Data Monitoring*: Airflow logs task execution, providing detailed information for monitoring and debugging.
5. *Modular & Scalable Design*: The pipeline is designed to be easily extendable and scalable, allowing for future enhancements or additional data sources.

## Pipeline Workflows

The pipeline performs the following tasks:
1. *Download*: Fetch log data from a URL.
2. *Extract*: Extract relevant fields from the raw log file.
3. *Transform*: Convert data into uppercase and change delimiters to CSV format.
4. *Load*: Insert transformed data into a PostgreSQL database.

## DAG Graph
![DAG Graph](assets/dag_graph.jpeg)

## Requirements

- **Apache Airflow** 2.10.2
- **PostgreSQL**
- **Python** 3.10.11
- **Python libraries**:
  - `requests`
  - `psycopg2`
  - `python-dotenv`
  - `airflow`
