import airflow
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor

from config.settings import ProjectDetails as PD
from scripts.sql_scripts import *
from scripts.custom_macros import *

# Collect a number of default configurations for the dag
AIRFLOW_DEFAULT_ARGS = {
    'owner': 'johnjanedoe',
    'depends_on_past': False,
    'email': ['johnjane@doe.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'googlebigquery',
    user_defined_macros={
        "retrieve_start_date": retrieve_start_date,
        "retrieve_start_timestamp": retrieve_start_timestamp,
        "retrieve_end_date": retrieve_end_date,
        "retrieve_end_timestamp": retrieve_end_timestamp,
        "retrieve_date": retrieve_date,
        "output_dataset_id": f"{PD.OUTPUT_DATASET_ID}",
        "input_dataset_id": f"{PD.INPUT_DATASET_ID}",
        "relevant_plays": f"{PD.OUTPUT_DATASET_ID}.{PD.TN_RELEVANT_PLAYS}",
        "plays_rightsholder": f"{PD.OUTPUT_DATASET_ID}.{PD.TN_PLAYS_RIGHTSHOLDER}",
        "plays_titles_rightsholder": f"{PD.OUTPUT_DATASET_ID}.{PD.TN_PLAYS_TITLE_RIGHTSHOLDER}",
        "weekly_rightsholder_payout": f"{PD.OUTPUT_DATASET_ID}.{PD.TN_WEEKLY_RIGHTSHOLDER_PAYOUT}",
        "weekly_reporting": f"{PD.OUTPUT_DATASET_ID}.{PD.TN_WEEKLY_REPORTING}"
    },
    default_args=AIRFLOW_DEFAULT_ARGS,
    description='Soundcloud challenge',
    start_date=datetime.strptime(PD.START_DATE, '%Y-%m-%d'),
    end_date=datetime.strptime(PD.END_DATE, '%Y-%m-%d'),
    schedule_interval=PD.SCHEDULE_INTERVAL) as dag:

    # This Sensor checks that we do in fact have a table containing payout numbers
    # for the week we are currently building the report for. This can be extended
    # to other required input tables.
    cpe = BigQueryTableSensor(
        task_id='check_payout_existencs',
        project_id=PD.PROJECT_ID,
        dataset_id="{{ input_dataset_id }}",
        table_id="payout_{{retrieve_start_date(ds)}}",
        bigquery_conn_id=PD.CONNECTION_ID,
    )

    # This Operator collects the relevant daily listen tables and concatenates them
    # into a single table. This approach can handle missing tables and reduces the
    # resource consumption.
    cwl = BigQueryOperator(
        task_id="collect_weekly_listens",
        sql=SELECT_RELEVANT_PLAYS,
        destination_dataset_table="{{ relevant_plays }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id=PD.CONNECTION_ID,
        use_legacy_sql=False,
        location="europe-west3",
    )

    # This operator joins track listens with rightsholder ids. It produces the count
    # of plays the current week and ensures that the rightholder id was valid at
    # the time of the play. I.e. if the track changed rightsholder within the week
    # this will be reflected here.
    mlr = BigQueryOperator(
        task_id="merge_listens_rightsholder",
        sql=JOIN_TRACKID_RIGHTHOLDERID,
        destination_dataset_table="{{ plays_rightsholder }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=False,
        bigquery_conn_id=PD.CONNECTION_ID,
        use_legacy_sql=False,
        location="europe-west3",
    )

    # This operator joins a track title to the the track_id. Theoretically, the track
    # title can change within the week. In that case we would have multiple track
    # titles for a single track id. Seeing that this is not a identifier in the final table
    # described in the task sheet I simple match the first track title that appears to the
    # track id.
    mtt = BigQueryOperator(
        task_id="merge_trackid_tracktitle",
        sql=JOIN_TRACKID_TRACKTITLE,
        destination_dataset_table="{{ plays_titles_rightsholder }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id=PD.CONNECTION_ID,
        use_legacy_sql=False,
        location="europe-west3",
    )

    # This operator computes the unit payout for any given rightsholder id. It does so
    # by simply dividing the amount paid out in that week by the total number of songs
    # played for that rightsholder id. For hypothetical cases where a rightsholder
    # owns multiple songs within a given week this does not allow for different
    # unit prices across the rightsholders portfolio.
    crp = BigQueryOperator(
        task_id="construct_rightsholder_payout",
        sql=JOIN_RIGHTSHOLDER_PAYOUT,
        destination_dataset_table="{{ weekly_rightsholder_payout }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=False,
        bigquery_conn_id=PD.CONNECTION_ID,
        use_legacy_sql=False,
        location="europe-west3",
    )

    # This operator adds the unit price as computed above the final reporting table.
    # It also does some formatting to better comply with the table descriptions as
    # set out in the task sheet.
    weekly_report = BigQueryOperator(
        task_id="construct_rightsholder_reporting",
        sql=JOIN_TRACKID_RIGHTHOLDER_PAYOUT,
        destination_dataset_table="{{ weekly_reporting }}_{{retrieve_start_date(ds)}}",
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        bigquery_conn_id=PD.CONNECTION_ID,
        use_legacy_sql=False,
        location="europe-west3",
    )

cpe >> cwl >> mlr >> mtt >> crp >> weekly_report
