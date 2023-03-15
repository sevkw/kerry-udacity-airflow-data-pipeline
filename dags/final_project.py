from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'udacity',
    'schedule_interval' :'@hourly',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_stage_events_table = PostgresOperator(
        task_id="create_stage_events_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.CREATE_STAGE_EVENTS_TABLE_SQL
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        redshift_conn_id  = "redshift",
        aws_credentials_id = "aws_credentials",
        table="stage_events",
        s3_bucket = "udacity-dend",
        s3_key = "log-data",
        region = "us-west-2",
        data_format = 'JSON',
        jsonpath = "'s3://udacity-dend/log_json_path.json'",
    )

    create_stage_songs_table = PostgresOperator(
        task_id="create_stage_songs_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.CREATE_STAGE_SONGS_TABLE_SQL
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="stage_songs",
        redshift_conn_id  = "redshift",
        aws_credentials_id = "aws_credentials",
        table="stage_songs",
        s3_bucket = "udacity-dend",
        s3_key = "song_data",
        region = "us-west-2",
        data_format = "JSON",
        jsonpath="'auto'"
    )

    create_fct_songplays_table = PostgresOperator(
        task_id="create_fct_songplays_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.SONGPLAY_TABLE_CREATE
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        fct_table = "fct_songplays",
        select_sql = "songplay_table_insert"

    )

    create_dim_users_table = PostgresOperator(
        task_id="create_dim_users_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.DIM_USERS_TABLE_CREATE
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        dim_table = "dim_users",
        select_sql = "user_table_insert"
    )

    create_dim_songs_table = PostgresOperator(
        task_id="create_dim_songs_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.DIM_SONGS_TABLE_CREATE
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        dim_table = "dim_songs",
        select_sql = "song_table_insert"
    )

    create_dim_artists_table = PostgresOperator(
        task_id="create_dim_artists_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.DIM_ARTISTS_TABLE_CREATE
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        dim_table = "dim_artists",
        select_sql = "artist_table_insert"
    )

    create_dim_time_table = PostgresOperator(
        task_id="create_dim_time_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.DIM_TIME_TABLE_CREATE
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        dim_table = "dim_time",
        select_sql = "time_table_insert"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        tables = ['dim_users', 'dim_songs', 'dim_artists', 'dim_time']
    )

    start_operator >> create_stage_events_table
    start_operator >> create_stage_songs_table

    create_stage_events_table >> stage_events_to_redshift
    create_stage_songs_table >> stage_songs_to_redshift

    stage_events_to_redshift >> create_fct_songplays_table
    stage_songs_to_redshift >> create_fct_songplays_table
    create_fct_songplays_table >> load_songplays_table

    load_songplays_table >> create_dim_users_table
    create_dim_users_table >> load_user_dimension_table

    load_songplays_table >> create_dim_songs_table
    create_dim_songs_table >> load_song_dimension_table

    load_songplays_table >> create_dim_artists_table
    create_dim_artists_table >> load_artist_dimension_table

    load_songplays_table >> create_dim_time_table
    create_dim_time_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

final_project_dag = final_project()