from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from api.video_stats import ( 
    get_playlist_id, 
    get_video_ids, 
    extract_video_data, 
    save_to_json
)

from datawarehouse.dwh import staging_table, core_table 
from dataquality.soda import yt_elt_data_quality


# Define the local timezone
local_tz = pendulum.timezone("Africa/Lagos")

# Default Args
default_args = {
    'owner': 'presydon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2025, 12, 31, tzinfo=local_tz),
}

#variables for data quality checks
staging_schema = "staging"
core_schema = "core"


# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="A DAG to extract video stats from YouTube and save to JSON",
    schedule='0 14 * * *',  # Daily at 2 PM local time
    catchup=False,
) as dag_produce:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_video_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # define dependencies
    playlist_id >> video_ids >> extracted_video_data >> save_to_json_task >> trigger_update_db


# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON data and insert into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:
    
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality"
    )

    # define dependencies
    update_staging >> update_core >> trigger_data_quality


# DAG 3:data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check data quality on both layers in the db",
    catchup=False,
    schedule=None,  
) as dag_quality:
    
    # Define tasks
    soda_validation_staging = yt_elt_data_quality(staging_schema)
    soda_validation_core = yt_elt_data_quality(core_schema) 

    # define dependencies
    soda_validation_staging >> soda_validation_core