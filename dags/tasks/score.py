import re

import numpy as np
import pandas as pd


def get_authorities():
    df = pd.read_csv('data/authorities.csv', header=True)
    return df['name'].tolist()


def score_source(df):
    source_score = []
    source_list = get_authorities()
    for source_name in df['source']:
        if source_name in source_list:
            source_score.append(source_list.index(source_name) + 1)
    result = []
    for j in range(len(df)):
        norm = (source_score[j] - min(source_score)) / (
                max(source_score) - min(source_score))
        result.append(norm)
    return np.array(result)


def score_date(df):
    from datetime import date
    current_date = date.today()
    data_diff = []
    data_score = []
    for i in range(len(df)):
        data_diff.append(int(str(current_date - df['published_at'][i])[:2]))
    for j in range(len(data_diff)):
        norm = (data_diff[j] - min(data_diff)) / (
            (max(data_diff) - min(data_diff)))
        data_score.append(norm)
    return np.array(data_score)


def d(x):
    if not x:
        return 0
    digit_found = re.search(r'\d+', x)
    if digit_found:
        norm_rate = int(digit_found[0])
    else:
        norm_rate = 0
    return norm_rate


def score_rates(df):
    to_scale = df['rate'].apply(d).tolist()
    norm_rates = [(rate - min(to_scale)) / (max(to_scale) - min(to_scale)) for
                  rate in to_scale]
    return np.array(norm_rates)


def score(x):
    return score_rates(x) + score_date(x) + score_source(x)
