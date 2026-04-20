## Problem Statement

The objective of this project is to build a **data pipeline in Snowflake** that processes large-scale taxi trip data and transforms it into structured, analytics-ready datasets.

The dataset contains detailed information about taxi rides in New York City, including pickup and dropoff timestamps, trip distance, passenger count, and fare information. Raw transportation datasets often contain **missing values, incorrect records, or inconsistent data types**, which makes them unsuitable for direct analysis.

To solve this problem, the project implements a **Bronze–Silver–Gold architecture**:

* **Bronze layer** stores the raw dataset exactly as ingested from the source.
* **Silver layer** cleans and validates the data by filtering incorrect or incomplete records.
* **Gold layer** creates aggregated and business-ready tables used for analytics.

The goal is to transform raw taxi trip data into **clean, reliable, and structured datasets** that can be used for analytical insights and decision-making.

---

## Potential Data Quality Risks

### Missing Values

Some records may contain missing values in important fields such as pickup time, dropoff time, trip distance, or passenger count. Missing values can lead to incorrect calculations and unreliable analytical results.

### Invalid or Zero Values

Certain records may contain unrealistic values, for example:

* trip distance equal to **0**
* passenger count equal to **0**
* negative or incorrect fare amounts

These records must be filtered during the **data cleaning stage**.

### Duplicate Records

Large datasets can contain duplicate rows caused by ingestion errors or logging issues. Duplicate records may artificially increase metrics such as the total number of trips or total revenue.

### Timestamp Inconsistencies

Pickup and dropoff timestamps may contain anomalies such as:

* dropoff time occurring before pickup time
* incorrect timestamp formats
* unrealistic trip durations

These inconsistencies must be validated during data transformation.

### Data Type Issues

When importing data from CSV files, some columns may initially be interpreted as incorrect data types (for example numeric values stored as text). This can cause errors in calculations and aggregations if not corrected.

### Outliers

Some trips may contain unusually high values for trip distance, trip duration, or fare amount. These outliers may represent data errors or rare cases and should be evaluated during the cleaning process.

## Technologies Used

This project was built using the following technologies:

* **Snowflake** – cloud data warehouse used for storing and processing the dataset
* **SQL** – used for data ingestion, cleaning, and transformations
* **GitHub** – version control and project documentation
* **CSV dataset** – raw taxi trip data imported from Kaggle
* **Medallion Architecture (Bronze–Silver–Gold)** – data pipeline design pattern used to structure the data processing workflow

---

## Data Architecture

The project follows the **Medallion Architecture**, which organizes the data pipeline into three layers: **Bronze, Silver, and Gold**. Each layer progressively improves data quality and usability.

### Bronze Layer – Raw Data

The Bronze layer contains the **raw dataset exactly as it was ingested from the source**.
No transformations are applied at this stage.

Characteristics:

* raw CSV data loaded into Snowflake tables
* original dataset structure is preserved
* serves as a data ingestion layer
* used as the source for further transformations

Purpose:

* maintain a reliable copy of the original data
* allow reproducibility of the pipeline

---

### Silver Layer – Cleaned Data

The Silver layer contains **cleaned and validated data** derived from the Bronze tables.

Data transformations include:

* removing invalid records (e.g. trip distance = 0)
* filtering unrealistic timestamps
* removing duplicates
* standardizing data types
* validating passenger counts and trip duration

Purpose:

* improve data quality
* ensure the dataset is consistent and reliable
* prepare the data for analytical processing

---

### Gold Layer – Aggregated Data

The Gold layer contains **business-ready datasets** created from the cleaned Silver tables.

Typical transformations include:

* aggregations
* summary statistics
* analytical datasets

Examples of analyses:

* number of taxi trips per day
* average trip distance
* average fare amount
* passenger trends over time

Purpose:

* provide datasets optimized for analytics
* support dashboards and reporting
* enable business insights

## Data Orchestration

Data pipeline orchestration is implemented using Apache Airflow, which manages the execution, scheduling, and monitoring of the entire ELT process.

###  Orchestration Design

The project follows a **single-entry orchestration pattern**, where Airflow triggers a single Spark pipeline script responsible for executing all transformation layers.

### What was added:
- Kafka producer for sending raw data as events
- Kafka consumer for ingesting data into Snowflake Bronze layer
- Topic-based data streaming (`raw-data`)

### Why:
- Decouples data ingestion from processing
- Enables real-time / near real-time data flow
- Improves scalability and fault tolerance
- Allows replaying data (event sourcing capability)

## Event-Driven Orchestration (Airflow)

The pipeline is now orchestrated using **Apache Airflow** with trigger-based execution.

### What was added:
- Airflow DAG for Bronze → Silver transformation
- PythonOperator to execute transformation logic
- Trigger-based execution (`schedule_interval=None`)

### Why:
- Enables automation of data processing
- Supports event-driven pipelines instead of manual execution
- Improves pipeline maintainability and observability

## Diagram 
<img width="1024" height="1536" alt="ChatGPT Image Apr 20, 2026, 12_52_08 PM" src="https://github.com/user-attachments/assets/bc25f448-9282-43ea-999a-9d8bfb65d815" />


