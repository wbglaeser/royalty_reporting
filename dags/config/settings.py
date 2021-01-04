import json
import airflow
from datetime import timedelta

CONFIG_FILE_PATH="dags/config/configs.json"
with open(CONFIG_FILE_PATH) as jsonfile:
    data = json.load(jsonfile)

class ProjectDetails:
    PROJECT_ID = data["PROJECT_ID"]
    DATASET_ID = data["DATASET_ID"]
    TN_RELEVANT_PLAYS = data["TN_RELEVANT_PLAYS"]
    TN_PLAYS_RIGHTSHOLDER = data["TN_PLAYS_RIGHTSHOLDER"]
    TN_PLAYS_TITLE_RIGHTSHOLDER = data["TN_PLAYS_TITLE_RIGHTSHOLDER"]
    TN_WEEKLY_RIGHTSHOLDER_PAYOUT = data["TN_WEEKLY_RIGHTSHOLDER_PAYOUT"]
    TN_WEEKLY_REPORTING = data["TN_WEEKLY_REPORTING"]
    END_DATE = data["END_DATE"]
    START_DATE = data["START_DATE"]

AIRFLOW_DEFAULT_ARGS = {
    'owner': 'benglaeser',
    'depends_on_past': False,
    'email': ['ben.glaeser@tuta.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
