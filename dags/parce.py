import glob
import importlib
import os
from datetime import datetime, timedelta
from os.path import basename, dirname, isfile, join

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator  # noqa
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG(
    "parce",
    description="DAG that parses data from the web",
    default_args={"owner": "airflow"},
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 30),
)


def import_parsers():
    parser_functions = {}
    modules = glob.glob(join(dirname(__file__), 'parsers', "*.py"))
    module_names = [basename(f)[:-3] for f in modules if
                    isfile(f) and not f.endswith('__init__.py')]
    for file in module_names:
        mod = importlib.import_module(f"parsers.{file}")
        parser_functions[file] = getattr(mod, 'fetch_news')
    return parser_functions


def parse_and_save_to_csv(func, start_date, end_date, file_name):
    res = func(start_date, end_date)
    data = []
    for newsletter in res[0]:
        data.append({
            'title': newsletter.title,
            'content': newsletter.content,
            'url': newsletter.url,
            'lang': newsletter.language,
            'source': newsletter.source,
            'published_at': newsletter.date,
        })

    df = pd.DataFrame(data)
    df.to_csv(f'{file_name}.csv', index=False)


parsers = import_parsers()
dag_id = 'parce'


def get_last_monday():
    today = datetime.today()
    last_monday = today - timedelta(days=today.weekday())
    return last_monday


def get_start_date(task_id):
    last_monday_date = get_last_monday()
    dates = [last_monday_date]
    return max(dates)


def get_end_of_yesterday():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    end_of_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day,
                                23, 59, 59)
    return end_of_yesterday


start_task = DummyOperator(task_id='start', dag=dag)

with TaskGroup(group_id='parse_and_save_group',
               dag=dag) as parse_and_save_group:
    for name, fetch_news_func in parsers.items():
        parse_and_save_task = PythonOperator(
            task_id=f'parse_and_save_{name}',
            python_callable=parse_and_save_to_csv,
            op_kwargs={
                'func': fetch_news_func,
                'file_name': f'/tmp/newsletters/{{{{ run_id }}}}/{name}.csv',
                'start_date': get_start_date(f'parse_and_save_{name}'),
                'end_date': get_end_of_yesterday()
            },
            dag=dag,
        )
        start_task >> parse_and_save_task


def combine_csv(**kwargs):
    temp_dir = f'/tmp/newsletters/{kwargs["run_id"]}'
    files = glob.glob(join(temp_dir, "*.csv"))
    combined_csv = pd.concat([pd.read_csv(f) for f in files])
    combined_csv.to_csv(join(temp_dir, "combined_newsletters.csv"),
                        index=False)


combine_csv_task = PythonOperator(
    task_id='combine_csv',
    python_callable=combine_csv,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
engine = postgres_hook.get_sqlalchemy_engine()


def save_to_postgres(**kwargs):
    df_path = f'/tmp/newsletters/{kwargs["run_id"]}/combined_newsletters.csv'
    df = pd.read_csv(df_path)
    df.to_sql('news', engine, if_exists='append', index=False)


task_upload_to_postgres = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=save_to_postgres,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)


def cleanup_files(**kwargs):
    temp_dir = f'/tmp/newsletters/{kwargs["run_id"]}'
    files = glob.glob(join(temp_dir, "*"))
    for f in files:
        os.remove(f)
    os.rmdir(temp_dir)


# task_cleanup_after = PythonOperator(
#     task_id='cleanup_after',
#     python_callable=cleanup_files,
#     trigger_rule=TriggerRule.ONE_SUCCESS,
#     dag=dag
# )
task_cleanup_before = PythonOperator(
    task_id='cleanup_before',
    python_callable=lambda **kwargs: os.makedirs(
        f'/tmp/newsletters/{kwargs["run_id"]}',
        exist_ok=True),
    trigger_rule=TriggerRule.ALWAYS,
    dag=dag
)

task_cleanup_before >> parse_and_save_group >> combine_csv_task >> task_upload_to_postgres
# >> task_cleanup_after
