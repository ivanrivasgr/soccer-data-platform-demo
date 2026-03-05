# Soccer Data Platform Demo

Example of a modern data pipeline for soccer tracking data.

This project simulates a production-style sports analytics pipeline with data validation, transformation, and analytics layers.

## Architecture

Pipeline stages:

Raw Data → Validation → Transform → Analytics

- Synthetic tracking data generation
- Data quality validation
- Quarantine of anomalous records
- Feature engineering (distance travelled)
- Player-level analytics tables

## Data Layers

### Bronze
Raw tracking data

### Silver
Validated and cleaned tracking data stored in Parquet

### Gold
Analytics-ready tables for dashboards

Example metrics:

- total distance per player
- average speed
- max speed
- session samples

## Data Quality Checks

- missing timestamps
- duplicate records
- speed spikes
- schema validation

Invalid records are moved to quarantine.

## Running locally

# Generate data

 python src/generate_sample_data.py

# Validate

 python src/validate.py

# Transform

 python src/transform.py

# Build Analytics

 python src/build_analytics.py


## CI/CD

GitHub Actions automatically runs the pipeline on every commit.

## Example output

 data/analytics/player_session_metrics.csv


Example metrics table:

| player_id | session_id | total_distance_m | avg_speed | max_speed |
|-----------|-----------|-----------------|-----------|-----------|
| P001 | S001 | 10234 | 2.7 | 8.1 |

## Purpose

This project demonstrates:

- data engineering pipeline design
- data quality monitoring
- reproducible analytics pipelines
- CI/CD automation

The architecture can be deployed in cloud platforms such as AWS using services like:

- S3
- Athena
- ECS / Lambda
- EventBridge

## Architecture

![Pipeline Architecture](architecture/soccer_pipeline_architecture.png)

## Infrastructure as Code

This project includes an example Terraform configuration to demonstrate how the pipeline could be deployed in AWS.

The Terraform configuration provisions a simple data lake structure using Amazon S3:

- raw data layer
- processed data layer
- analytics layer

Example structure:

raw data → S3 bucket  
processed data → S3 bucket  
analytics tables → S3 bucket

This infrastructure would support running the pipeline using services such as:

- AWS Lambda or ECS for transformations
- EventBridge for scheduling
- Athena for analytics queries
- Metabase or PowerBI for dashboards

 ## Production Deployment Architecture

In a real production environment this pipeline could run as:

1. GPS tracking data is uploaded to the raw S3 bucket.
2. EventBridge detects new files and triggers a processing job.
3. A compute layer (Lambda or ECS) runs validation and transformations.
4. Clean datasets are stored in the processed layer as Parquet files.
5. Analytics tables are built for downstream consumption.
6. BI tools such as Metabase or PowerBI query the data through Athena.

This architecture follows a modern Bronze → Silver → Gold data lake design.

## Pipeline Orchestration

In a production environment this pipeline would typically be orchestrated using a workflow scheduler such as Apache Airflow or Prefect.

Each stage of the pipeline would run as an independent task in a DAG:

1. ingest_tracking_data  
2. validate_tracking_data  
3. transform_tracking_data  
4. build_player_metrics  
5. publish_analytics_tables  

The scripts in this repository (`generate_sample_data.py`, `validate.py`, `transform.py`, `build_analytics.py`) represent the individual tasks that would be executed by the orchestration layer.

Airflow would manage:

- scheduling
- task dependencies
- retries
- failure monitoring

## Example Airflow DAG

An example DAG is included in the repository to demonstrate how the pipeline could be orchestrated using Apache Airflow.

Location:

soccer_pipeline_dag.py

The DAG defines tasks corresponding to each pipeline stage and sets their execution order.

## Running the Full Pipeline

You can run the entire pipeline locally using the Makefile:

make pipeline

This will execute the full workflow:

1. generate synthetic data
2. validate raw data
3. transform datasets
4. build analytics tables