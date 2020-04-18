from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime(2020, 1, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "udac_example_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    path_s3="s3://udacity-dend/log_data",
    json_fmt="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    path_s3="s3://udacity-dend/song_data",
    json_fmt="auto",
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="songplays",
    query_select=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dimension_table",
    dag=dag,
    table="users",
    query_select=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dimension_table",
    dag=dag,
    table="songs",
    query_select=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dimension_table",
    dag=dag,
    table="artists",
    query_select=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dimension_table",
    dag=dag,
    table="time",
    query_select=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    dict_checks={
        "songplays": ["playid", "sessionid", "start_time"],
        "users": ["userid"],
        "songs": ["songid"],
        "artists": ["artistid"],
        "time": ["start_time"],
    },
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

# Stagging tables
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Dim tables
stage_events_to_redshift >> load_user_dimension_table
stage_songs_to_redshift >> load_song_dimension_table
stage_songs_to_redshift >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Data quality
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Dummy end
run_quality_checks >> end_operator
