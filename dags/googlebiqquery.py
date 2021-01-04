import airflow
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor

from config.settings import ProjectDetails as PD, AIRFLOW_DEFAULT_ARGS
from scripts.sql_scripts import JOIN_TRACKID_RIGHTHOLDERID, JOIN_TRACKID_TRACKTITLE, JOIN_RIGHTSHOLDER_PAYOUT, JOIN_TRACKID_RIGHTHOLDER_PAYOUT
from scripts.custom_macros import *

with DAG(
    'googlebigquery',
    user_defined_macros={
        "retrieve_start_date": retrieve_start_date,
        "retrieve_start_timestamp": retrieve_start_timestamp,
        "retrieve_end_date": retrieve_end_date,
        "retrieve_end_timestamp": retrieve_end_timestamp,
        "retrieve_date": retrieve_date,
        "dataset_id": f"{PD.DATASET_ID}",
        "relevant_plays": f"{PD.DATASET_ID}.{PD.TN_RELEVANT_PLAYS}",
        "plays_rightsholder": f"{PD.DATASET_ID}.{PD.TN_PLAYS_RIGHTSHOLDER}",
        "plays_titles_rightsholder": f"{PD.DATASET_ID}.{PD.TN_PLAYS_TRACKS_RIGHTSHOLDER}",
        "weekly_rightsholder_payout": f"{PD.DATASET_ID}.{PD.TN_WEEKLY_RIGHTSHOLDER_PAYOUT}",
        "weekly_reporting": f"{PD.DATASET_ID}.{PD.TN_WEEKLY_REPORTING}"
    },
    default_args=AIRFLOW_DEFAULT_ARGS,
    description='Soundcloud challenge',
    start_date=datetime(2020, 12, 21),
    schedule_interval=timedelta(hours=24)) as dag:

    cpe = BigQueryTableSensor(
        task_id='check_payout_existencs',
        project_id=PD.PROJECT_ID,
        dataset_id=PD.DATASET_ID,
        table_id="payout_{{retrieve_start_date(ds)}}",
        bigquery_conn_id="scchallenge-gcp-conn",
    )

    cwl = BigQueryOperator(
        task_id="collect_weekly_listens",
        sql=SELECT_RELEVANT_PLAYS,
        destination_dataset_table="{{ relevant_plays }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    mlr = BigQueryOperator(
        task_id="merge_listens_rightsholder",
        sql=JOIN_TRACKID_RIGHTHOLDERID,
        destination_dataset_table="{{ plays_rightsholder }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=False,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    mtt = BigQueryOperator(
        task_id="merge_trackid_tracktitle",
        sql=JOIN_TRACKID_TRACKTITLE,
        destination_dataset_table="{{ plays_titles_rightsholder }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    crp = BigQueryOperator(
        task_id="construct_rightsholder_payout",
        sql=JOIN_RIGHTSHOLDER_PAYOUT,
        destination_dataset_table="{{ weekly_rightsholder_payout }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=False,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

    weekly_report = BigQueryOperator(
        task_id="construct_rightsholder_reporting",
        sql=JOIN_TRACKID_RIGHTHOLDER_PAYOUT,
        destination_dataset_table="{{ weekly_reporting }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id="scchallenge-gcp-conn",
        use_legacy_sql=False,
        location="europe-west3",
    )

cpe >> cwl >> mlr >> mtt >> crp >> weekly_report
