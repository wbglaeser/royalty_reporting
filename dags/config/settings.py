import airflow
from datetime import timedelta

class ProjectDetails:
    PROJECT_ID = "rnr-data-eng-challenge"
    OUTPUT_DATASET_ID = "inr006"
    INPUT_DATASET_ID = "challenge_dataset"
    TN_RELEVANT_PLAYS = "weekly_plays"
    TN_PLAYS_RIGHTSHOLDER = "weekly_plays_rightsholder"
    TN_PLAYS_TITLE_RIGHTSHOLDER = "weekly_plays_title_rightsholder"
    TN_WEEKLY_RIGHTSHOLDER_PAYOUT = "weekly_rightsholder_payout"
    TN_WEEKLY_REPORTING = "weekly_reporting"
    END_DATE = "2019-05-19"
    START_DATE = "2019-05-06"
    SCHEDULE_INTERVAL= timedelta(hours=48)
