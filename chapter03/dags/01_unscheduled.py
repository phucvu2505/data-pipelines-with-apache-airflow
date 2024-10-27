from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled", 
    start_date=datetime(2019, 1, 1), # define the start date for the DAG
    schedule_interval=None # Specific that this is an unscheduled DAG => defailt vaule = None
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events.json http://events_api:5000/events" # Fetch and store the events from API
    ),
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""


    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index() # Load the events and calulate the required statistics
    
    Path(output_path).parent.mkdir(exist_ok=True) # Make sure the output directory exits and write reults to CSV
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats
