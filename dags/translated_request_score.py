from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from tasks.cleanup import cleanup_files, create_temp_dir
from tasks.load import load_from_postgres, save_csv, upload_to_postgres
from ycloud.gpt import GPT

dag = DAG(
    "translated_request_score",
    "Get translated and summarized news and request score",
    default_args={"owner": "airflow"},
    schedule_interval='@monthly',
    start_date=datetime(2024, 5, 30),
)

postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
engine = postgres_hook.get_sqlalchemy_engine()

tmp_dir = "/tmp/newsletters"

yandexcreds = Variable.get("yandexapisecret", deserialize_json=True)

themes = Variable.get("themes", deserialize_json=False)


def s(x):
    return f"Определи, к какой из предложенных тем: {themes} относится этот текст: " + x + " и дай численный ответ в виде кода темы, если ни одна тема не подходит, то дай ответ 0"


def vectorize_ans_score(**kwargs):
    df = pd.read_csv(f'/tmp/newsletters/{kwargs["run_id"]}/news.csv',
                     index_col="id")
    gpt = GPT(yandexcreds)
    df['summary'] = gpt.get_result_series(df['summarization_request_id'])
    df['is_summarized'] = df['summary'].notna()
    to_vectorize = df[(~df["is_vectorized"]) & (
            df['is_translated'] | (df['lang'] == 'ru'))]
    to_vectorize["vectorized_content"] = gpt.vectorize_series(
        to_vectorize["summary"], "doc")
    to_vectorize["is_vectorized"] = ~to_vectorize["vectorized_content"].isna()
    df.update(to_vectorize)
    to_score = df[df["is_translated"] & ~df["is_scored"]]
    to_score["score_request_id"] = gpt.generate_series(
        to_score["content"].apply(s),
        "pro",
        async_=True)
    to_score["is_scored"] = False
    to_score["is_requested_for_scoring"] = to_score["score_request_id"].notna()
    df.update(to_score)
    save_csv(df, f'{tmp_dir}/{kwargs["run_id"]}/news.csv')


def load_news(**kwargs):
    df = load_from_postgres(
        "SELECT * FROM news WHERE (is_summarized = False AND is_requested_for_summarization = True) or (is_requested_for_scoring = TRUE AND is_scored = FALSE)")
    save_csv(df, f'{tmp_dir}/{kwargs["run_id"]}/news.csv')


task_load_news = PythonOperator(
    task_id="load_news",
    python_callable=load_news,
    dag=dag,
)

task_vectorize_ans_score = PythonOperator(
    task_id="vectorize_ans_score",
    python_callable=vectorize_ans_score,
    dag=dag
)

task_upload_to_db = PythonOperator(
    task_id="upload_to_db",
    python_callable=upload_to_postgres,
    dag=dag,
    op_kwargs={"if_exist": "replace",
               "path": f'{tmp_dir}/{{run_id}}/news.csv'}
)

task_cleanup_before = PythonOperator(
    task_id='cleanup_before',
    python_callable=create_temp_dir,
    trigger_rule=TriggerRule.ALWAYS,
    dag=dag
)

task_cleanup_after = PythonOperator(
    task_id="cleanup_after",
    python_callable=cleanup_files,
    dag=dag
)

task_cleanup_before >> task_load_news >> task_vectorize_ans_score >> task_upload_to_db >> task_cleanup_after
