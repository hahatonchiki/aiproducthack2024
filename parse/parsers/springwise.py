from datetime import datetime
from typing import List, Tuple

import pandas as pd

from parse.news import NewsLetter


def get_from_json(json_obj):
    news = []
    for obj in map(lambda x: x['fields'], json_obj):
        title = obj['post_title']
        text = obj['post_content']
        date = datetime.fromtimestamp(int(obj['post_date_gmt']), tz=None)
        source = "Springwise"
        url = "https://springwise.com" + obj['post_url']
        parsed_at = datetime.now()
        news.append(
            NewsLetter(title, text, "en", source, date, url, parsed_at)
        )
    return news


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    news = []
    i = 0
    while True:
        import requests

        url = "https://5gcwhjrqq2.execute-api.us-east-1.amazonaws.com/prod"

        payload = f'q=-DEFAULT_NO_ARGUMENT_SEARCH&fq=(and%20blog_id%3A%20\'1\'%20site_id%3A%20\'1\'%20(or%20post_type%3A%20\'post\'%20)%20)&size=20&sort=post_date%20desc&start={i * 20}&facet=%7B%22ct_sector%22%3A%7Bsort%3A%22bucket%22%2C%20size%3A%20300%7D%2C%22ct_country%22%3A%7Bsort%3A%22bucket%22%2C%20size%3A%20300%7D%2C%22ct_business_model%22%3A%7Bsort%3A%22bucket%22%2C%20size%3A%20300%7D%2C%22ct_technology%22%3A%7Bsort%3A%22bucket%22%2C%20size%3A%20300%7D%2C%22ct_topic%22%3A%7Bsort%3A%22bucket%22%2C%20size%3A%20300%7D%7D'
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,ru;q=0.8,es;q=0.7',
            'cache-control': 'no-cache',
            'content-type': 'application/x-www-form-urlencoded'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if not response.ok:
            break
        resp_text = response.json()['hits']['hit']
        newsletters = get_from_json(resp_text)
        if not newsletters:
            break
        newsletters_filter = list(
            filter(lambda x: min_date <= x.date, newsletters))
        news.extend(newsletters_filter)
        if len(newsletters_filter) != len(newsletters):
            break
        i += 1
    news = list(filter(lambda x: x.date <= max_date, news))
    return news, len(news) > 0


if __name__ == "__main__":
    start_date = datetime.strptime("2024-05-24", "%Y-%m-%d")
    end_date = datetime.strptime("2024-06-01", "%Y-%m-%d")
    res = fetch_news(start_date, end_date)
    data = []
    for newsletter in res[0]:
        data.append({
            'Title': newsletter.title,
            'Content': newsletter.content,
            'Language': newsletter.language,
            'Source': newsletter.source,
            'Date': newsletter.date,
            'URL': newsletter.url,
            'Parsed_At': newsletter.parsed_at
        })

    df = pd.DataFrame(data)
    df.to_csv('newsletter_data.csv', index=False)
