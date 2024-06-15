import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def upload_to_postgres(path, if_exist, **kwargs):
    path = path.replace('{run_id}', kwargs['run_id'])
    df = pd.read_csv(path)
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
    engine = postgres_hook.get_sqlalchemy_engine()
    cols = ['title', 'content', 'summary', 'published_at', 'source', 'url',
            'vectorized_content', 'lang', 'is_vectorized', 'is_translated',
            'is_public', 'is_requested_for_summarization', 'is_summarized',
            'summarization_request_id', 'is_requested_for_scoring', 'score',
            'score_request_id', "is_scored"]
    cols = list(filter(lambda x: x in df.columns, cols))
    df = df[cols]
    df = df.reset_index().rename(columns={'index': 'id'})
    df.to_sql('news', engine, if_exists=if_exist, index=False)


def load_from_postgres(query="SELECT * FROM news"):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_test')
    engine = postgres_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, con=engine, index_col='id')
    return df


def save_csv(df, path):
    df.to_csv(path, index_label='id')
