import glob
import importlib
import os
from datetime import datetime, timedelta
from os.path import basename, dirname, isfile, join

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook

from tasks.last_execution_date import last_successful
from tasks.load import save_csv, upload_to_postgres
from tasks.cleanup import cleanup_files

dag_id = 'parce'

dag = DAG(
    dag_id,
    description="DAG that parses data from the web",
    default_args={"owner": "airflow"},
    schedule_interval='@monthly',
    start_date=datetime(2024, 5, 30),
)

tmp_dir = "/tmp/newsletters"


def import_parsers():
    parser_functions = {}
    modules = glob.glob(join(dirname(__file__), 'parsers', "*.py"))
    module_names = [basename(f)[:-3] for f in modules if
                    isfile(f) and not f.endswith('__init__.py')]
    for file in module_names:
        mod = importlib.import_module(f"parsers.{file}")
        if hasattr(mod, 'fetch_news'):
            parser_functions[file] = getattr(mod, 'fetch_news')
    return parser_functions


def parse_and_save_to_csv(func, start_date, end_date, file_name):
    res = func(start_date, end_date)
    if not res:
        raise Exception('Some error occurred while fetching data')
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
    if not data:
        raise ValueError('No data to save')
    df = pd.DataFrame(data)
    save_csv(df, f'{file_name}.csv')


parsers = import_parsers()


def get_last_monday():
    today = datetime.today()
    last_monday = today - timedelta(days=today.weekday())
    return last_monday


def get_start_date(task_id):
    last_monday_date = get_last_monday()
    dates = [last_monday_date,
             last_successful(dag_id, task_id) + timedelta(days=1)]
    return max(dates)


def get_end_of_yesterday():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    end_of_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day,
                                23, 59, 59)
    return end_of_yesterday


task_cleanup_before = PythonOperator(
    task_id='cleanup_before',
    python_callable=lambda **kwargs: os.makedirs(
        f'/tmp/newsletters/{kwargs["run_id"]}',
        exist_ok=True),
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)
with TaskGroup(group_id='parse_and_save_group',
               dag=dag) as parse_and_save_group:
    for name, fetch_news_func in parsers.items():
        parse_and_save_task = PythonOperator(
            task_id=f'parse_and_save_{name}',
            python_callable=parse_and_save_to_csv,
            op_kwargs={
                'func': fetch_news_func,
                'file_name': f'/tmp/newsletters/{{{{ run_id }}}}/{name}',
                'start_date': get_start_date(f'parse_and_save_{name}'),
                'end_date': get_end_of_yesterday()
            },
            dag=dag,
        )
        task_cleanup_before >> parse_and_save_task


def combine_csv(**kwargs):
    temp_dir = f'/tmp/newsletters/{kwargs["run_id"]}'
    files = glob.glob(join(temp_dir, "*.csv"))
    combined_csv = pd.concat([pd.read_csv(f, index_col='id') for f in files])
    save_csv(combined_csv, f'{temp_dir}/combined_newsletters.csv')


combine_csv_task = PythonOperator(
    task_id='combine_csv',
    python_callable=combine_csv,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
engine = postgres_hook.get_sqlalchemy_engine()

task_upload_to_postgres = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
    op_kwargs={"if_exist": "append",
               "path": f"{tmp_dir}/{{run_id}}/combined_newsletters.csv",
               "index": False}
)

task_cleanup_after = PythonOperator(
    task_id='cleanup_after',
    python_callable=cleanup_files,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

task_cleanup_before >> parse_and_save_group >> combine_csv_task >> task_upload_to_postgres >> task_cleanup_after
