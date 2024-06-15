import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from tasks import score
from tasks.load import load_from_postgres, save_csv, upload_to_postgres
from ycloud.gpt import GPT

dag_id = 'score'
tmp_dir = "/tmp/newsletters"
dag = DAG(
    dag_id,
    "Score news",
    default_args={"owner": "airflow"},
    schedule_interval='@monthly',
    start_date=datetime(2024, 5, 30),
)
yandexcreds = Variable.get("yandexapisecret", deserialize_json=True)


def load_news(**kwargs):
    df = load_from_postgres(
        f"SELECT * FROM news WHERE( is_summarized = False AND is_requested_for_summarization = True) or (is_requested_for_scoring = TRUE AND is_scored = FALSE)")
    save_csv(df, f'{tmp_dir}/{kwargs["run_id"]}/news.csv')


# def score_and_remove_duplicates(**kwargs):
#     df = pd.read_csv(f'{tmp_dir}/{kwargs["run_id"]}/news.csv',
#                      index_col="id")
#     df =

task_cleanup_before = PythonOperator(
    task_id='cleanup_before',
    python_callable=lambda **kwargs: os.makedirs(
        f'/tmp/newsletters/{kwargs["run_id"]}',
        exist_ok=True),
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

task_load_news = PythonOperator(
    task_id="load_news",
    python_callable=load_news,
    dag=dag,
)


def get_score(**kwargs):
    df = pd.read_csv(f'{tmp_dir}/{kwargs["run_id"]}/news.csv',
                     index_col="id")
    df["rate"] = ""
    to_get_score = df[df["is_requested_for_scoring"] & ~df["is_scored"]]
    gpt = GPT(yandexcreds)
    to_get_score["rate"] = gpt.get_result_series(
        df["score_request_id"])
    to_get_score["is_scored"] = to_get_score["score_request_id"].notna()
    df.update(to_get_score)
    df["score"] = score.score(df)
    save_csv(df, f'{tmp_dir}/{kwargs["run_id"]}/news.csv')


task_get_score = PythonOperator(
    task_id="get_score",
    python_callable=get_score,
    dag=dag,
)
task_upload_to_postgres = PythonOperator(
    task_id="upload_to_postgres",
    python_callable=upload_to_postgres,
    op_kwargs={"path": f'{tmp_dir}/{{run_id}}/news.csv',
               "if_exist": "replace"},
    dag=dag
)
task_cleanup_before >> task_load_news >> task_get_score >> task_upload_to_postgres
