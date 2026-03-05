from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-platform",
    "retries": 1
}

with DAG(
    dag_id="soccer_data_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    generate_data = BashOperator(
        task_id="generate_sample_data",
        bash_command="python src/generate_sample_data.py"
    )

    validate_data = BashOperator(
        task_id="validate_tracking_data",
        bash_command="python src/validate.py"
    )

    transform_data = BashOperator(
        task_id="transform_tracking_data",
        bash_command="python src/transform.py"
    )

    build_metrics = BashOperator(
        task_id="build_player_metrics",
        bash_command="python src/build_analytics.py"
    )

    generate_data >> validate_data >> transform_data >> build_metrics