import os
import sys

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable

from pipelines.reddit_pipeline import reddit_pipeline
from utils.constants import AWS_BUCKET_NAME

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


default_args = {
    "owner": "Aritra Ganguly",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 7),
    "email": ["aritraganguly.msc@protonmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

file_postfix = datetime.now().strftime("%Y%m%d")


@dag(
    dag_id="reddit_etl_pipeline",
    description="ETL Pipeline Design from Reddit API to AWS.",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["reddit", "etl", "pipeline"],
)
def reddit_dag():
    filename = reddit_pipeline(
        file_name=f"reddit_data_{file_postfix}",
        subreddit=Variable.get("subreddit"),
        time_filter="day",
        limit=100,
    )

    @task
    def upload_data_s3(src_path: str) -> None:
        dst = ObjectStoragePath(f"s3://{AWS_BUCKET_NAME}/raw/")
        dst.mkdir(parents=True, exist_ok=True)

        src = ObjectStoragePath(src_path)
        src.copy(dst)

    upload_data_s3(filename)


reddit_dag()
