import airflow
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor

from config.settings import ProjectDetails, AIRFLOW_DEFAULT_ARGS
from scripts.sql_scripts import JOIN_TRACKID_RIGHTHOLDERID, JOIN_TRACKID_TRACKTITLE, JOIN_RIGHTSHOLDER_PAYOUT, JOIN_TRACKID_RIGHTHOLDER_PAYOUT
from scripts.custom_macros import *

with DAG(
    'googlebigquery',
    user_defined_macros={
        "retrieve_start_date": retrieve_start_date,
        "retrieve_start_timestamp": retrieve_start_timestamp,
        "retrieve_end_date": retrieve_end_date,
        "retrieve_end_timestamp": retrieve_end_timestamp,
    },
    default_args=AIRFLOW_DEFAULT_ARGS,
    description='Soundcloud challenge',
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval=timedelta(hours=24)) as dag:

    check_payout = BigQueryTableSensor(
        task_id='check_for_table_existence',
        project_id=ProjectDetails.PROJECT_ID,
        dataset_id=ProjectDetails.DATASET_ID,
        table_id="payout_{{retrieve_start_date(ds)}}",
        bigquery_conn_id="scchallenge-gcp-conn",
    )

    weekly_listens = BigQueryOperator(
        task_id="construct_listen_rightsholder",
        sql=JOIN_TRACKID_RIGHTHOLDERID,
        destination_dataset_table="scchallenge.weekly_track_rightsholder_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=False,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    weekly_track_title = BigQueryOperator(
        task_id="construct_track_information",
        sql=JOIN_TRACKID_TRACKTITLE,
        destination_dataset_table="scchallenge.weekly_track_title_rightsholder_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    weekly_payouts = BigQueryOperator(
        task_id="construct_rightsholder_payout",
        sql=JOIN_RIGHTSHOLDER_PAYOUT,
        destination_dataset_table="scchallenge.weekly_rightsholder_payout_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=False,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    weekly_report = BigQueryOperator(
        task_id="construct_rightsholder_reporting",
        sql=JOIN_TRACKID_RIGHTHOLDER_PAYOUT,
        destination_dataset_table="scchallenge.weekly_reporting_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

check_payout >> weekly_listens >> weekly_track_title >> weekly_payouts >> weekly_report
