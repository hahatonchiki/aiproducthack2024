import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from tasks.load import load_from_postgres, save_csv
from tasks.last_execution_date import last_successful

dag_id = 'publish'
tmp_dir = "/tmp/newsletters"
dag = DAG(
    dag_id,
    "Publish the newsletters",
    default_args={"owner": "airflow"},
    schedule_interval='@monthly',
    start_date=datetime(2024, 5, 30),
)


def load_news(**kwargs):
    date = last_successful('publish', '*')
    df = load_from_postgres(
        f"SELECT * FROM news WHERE is_summarized = False AND is_requested_for_summarization = True AND published_at > '{date.strftime('%Y-%m-%d')}' AND is_requested_for_scoring = TRUE")
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

TOPICS = 'Технологии,Инновации,Innovations,Trends,Цифровизация,Автоматизация,Цифровая трансформация,Digital solutions,Цифровые двойники,Digital twins,ИИ ,AI,IoT ,Интернет вещей,Big Data,Блокчейн,Process mining,Облачные технологии,Квантовые вычисления,Смарт-контракты,Робототехника,VR/AR/MR ,Виртуальная и дополненная реальность,Генеративный,Распознавание,Искусственный интеллект,Машинное обучение,Глубокое обучение,Нейронные сети,Компьютерное зрение,Обработка естественного языка (NLP),Reinforcement Learning ,Low-code,No-code,Металлургический(ая),Сталь,Steel,LLM,ML,ChatGPT,IT,Кибербезопасность,Стартапы,Startups,YandexGPT,LLAMA,GPT (GPT-3, GPT-4),BERT,OpenAI,DALL·E,Transformer models,Generative Adversarial Networks (GAN),DeepFake ,Машинное зрение,Text-to-Image,Voice-to-text ,Визуализация данных ,Управление цепочками поставок,Снабжение,Технологии 5G,Суперкомпьютеры,DevOps,ФинТех,Token,Токен,Микросервисы,Kubernetes,API ,Цифровой след,Цифровая идентификация,Интеллектуальный анализ данных,Продвинутая аналитика,Северсталь,Евраз,ММК,ОМК,Nippon steel'


def task_get_score(**kwargs):
    df = pd.read_csv(f'{tmp_dir}/{kwargs["run_id"]}/news.csv')
    to_get_score = df[df["is_requested_for_score"] & ~df["is_scored"]]
    to_get_score["score"] = 0.


task_get_score
task_cleanup_before >> task_load_news
