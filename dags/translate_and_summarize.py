import asyncio
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from tasks.cleanup import create_temp_dir
from parce import cleanup_files
from tasks.load import load_from_postgres, save_csv, upload_to_postgres
from ycloud.translator import Translator
from ycloud.gpt import GPT

dag = DAG(
    "translate_and_summarize",
    "Translate and get summaries of news",
    default_args={"owner": "airflow"},
    schedule_interval='@monthly',
    start_date=datetime(2024, 5, 30),
)

postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
engine = postgres_hook.get_sqlalchemy_engine()

tmp_dir = "/tmp/newsletters"

max_vectorize = int(1e9)
max_translate = 200

ycloudcreds = Variable.get("yandexapisecret", deserialize_json=True)



def translate_and_summarize_news(**kwargs):
    df = load_from_postgres(
        "SELECT * FROM news WHERE (is_translated = False AND lang != 'ru') or is_requested_for_summarization = False")
    translator = Translator(ycloudcreds)
    gpt = GPT(ycloudcreds)
    df['content'] = df['content'].fillna('')
    df['is_translated'] = df['is_translated'] | (df['lang'] == 'ru')
    to_translate = df[
        ~df["is_translated"] & (df['content'].str.len() > 0)].head(
        max_translate)
    to_translate["content"] = asyncio.run(
        translator.translate_series(to_translate["content"], 'ru',
                                    is_html=False))
    to_translate["is_translated"] = True
    df.update(to_translate)
    to_summarize = df[df["is_translated"]]
    to_summarize['summarization_request_id'] = gpt.generate_series(
        to_summarize["content"], "summary", async_=True)
    to_summarize['is_requested_for_summarization'] = ~(to_summarize[
                                                           'summarization_request_id'] == None)
    to_summarize['summarization_request_id'] = to_summarize[
        'summarization_request_id'].fillna('')
    df.update(to_summarize)
    df['is_vectorized'] = df['is_vectorized'].astype(bool)
    save_csv(df, f'{tmp_dir}/{kwargs["run_id"]}/news_translated.csv')


task_translate_and_summarize_news = PythonOperator(
    task_id="translate_and_summarize_news",
    python_callable=translate_and_summarize_news,
    dag=dag,
)

task_upload_news_to_db = PythonOperator(
    task_id="upload_news_to_db",
    python_callable=upload_to_postgres,
    dag=dag,
    op_kwargs={"if_exist": "replace",
               "path": f'{tmp_dir}/{{run_id}}/news_translated.csv'}
)

task_cleanup_after = PythonOperator(
    task_id='cleanup_after',
    python_callable=cleanup_files,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)
task_cleanup_before = PythonOperator(
    task_id='cleanup_before',
    python_callable=create_temp_dir,
    trigger_rule=TriggerRule.ALWAYS,
    dag=dag
)

task_cleanup_before >> task_translate_and_summarize_news >> task_upload_news_to_db >> task_cleanup_after
