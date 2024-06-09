import asyncio
import glob
import os
from datetime import datetime
from os.path import join

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from ycloud.translator import Translator
from ycloud.gpt import GPT

dag = DAG(
    "translate_and_vectorize",
    "Translate and vectorize data from the web",
    default_args={"owner": "airflow"},
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 30),
)

postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
engine = postgres_hook.get_sqlalchemy_engine()

tmp_dir = "/tmp/newsletters"


def load_news(**kwargs):
    df = pd.read_sql("SELECT * FROM news", con=engine)
    df.to_csv(f'/tmp/newsletters/{kwargs["run_id"]}/news.csv',
              index_label="index")


max_vectorize = 200
max_translate = int(1e9)

ycloudcreds = Variable.get("yandexapisecret", deserialize_json=True)


def translate_ans_vectorize_news(**kwargs):
    translator = Translator(ycloudcreds)
    gpt = GPT(ycloudcreds)
    df = pd.read_csv(f'/tmp/newsletters/{kwargs["run_id"]}/news.csv',
                     index_col="index")
    df['is_translated'] = df['is_translated'] | (df['lang'] == 'ru')
    to_translate = df[~df["is_translated"]].head(max_vectorize)
    to_translate["content"] = asyncio.run(
        translator.translate_series(to_translate["content"], 'ru',
                                    is_html=False))
    to_translate["is_translated"] = True
    df.update(to_translate)
    df['is_vectorized'] = df['is_vectorized'].astype(bool)
    to_vectorize = df[(~df["is_vectorized"]) & (
            df['is_translated'] | (df['lang'] == 'ru'))].head(
        max_vectorize)
    to_vectorize["vectorized_content"] = asyncio.run(
        gpt.vectorize_series(to_vectorize["content"], "doc"))
    to_vectorize["is_vectorized"] = True
    df.update(to_vectorize)
    df.to_csv(f"/tmp/newsletters/{kwargs['run_id']}/news_translated.csv",
              index_label="index")


def upload_news_to_db(**kwargs):
    df = pd.read_csv(
        f'/tmp/newsletters/{kwargs["run_id"]}/news_translated.csv')
    df.to_sql("news", con=engine, if_exists="replace")


task_load_news = PythonOperator(
    task_id="load_news",
    python_callable=load_news,
    dag=dag,
)

task_translate_and_vectorize_news = PythonOperator(
    task_id="translate_and_vectorize_news",
    python_callable=translate_ans_vectorize_news,
    dag=dag,
)

task_upload_news_to_db = PythonOperator(
    task_id="upload_news_to_db",
    python_callable=upload_news_to_db,
    dag=dag,
)


def cleanup_files(**kwargs):
    temp_dir = f'/tmp/newsletters/{kwargs["run_id"]}'
    files = glob.glob(join(temp_dir, "*"))
    for f in files:
        os.remove(f)
    os.rmdir(temp_dir)


task_cleanup_after = PythonOperator(
    task_id='cleanup_after',
    python_callable=cleanup_files,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)
task_cleanup_before = PythonOperator(
    task_id='cleanup_before',
    python_callable=lambda **kwargs: os.makedirs(
        f'/tmp/newsletters/{kwargs["run_id"]}',
        exist_ok=True),
    trigger_rule=TriggerRule.ALWAYS,
    dag=dag
)

task_cleanup_before >> task_load_news >> task_translate_and_vectorize_news >> task_upload_news_to_db >> task_cleanup_after
