import re
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
import requests

from parse.news import NewsLetter


def parse(url):
    response = requests.get(url)
    if not response.ok:
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    title = soup.find('meta', attrs={
        'name': 'title'
    })['content']
    date = datetime.strptime(
        soup.find('meta', attrs={
            'name': 'datePublished'
        })['content'],
        "%Y-%m-%dT%H:%M:%S%z")
    source = "Business Insider"
    res = soup.find('div', class_="content-lock-content").get_text()
    content = re.sub(r"\n+", "\n", res).strip()
    content = re.sub(" +", " ", content)
    return NewsLetter(title, content, "en", source, date, url, datetime.now())


def get_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    return list(
        map(lambda x: parse('https://businessinsider.com' + x.get('href')),
            soup.find_all('a', class_='tout-title-link')))


def fetch_news(start_date, end_date):
    i = 1
    url = "https://www.businessinsider.com/s?json=1&q=*&p=1"
    response = requests.get(url)
    if not response.ok:
        return [], False
    news = []
    while True:
        data = response.json()
        newsletters = get_from_html(data['rendered'])
        if len(newsletters) != 0:
            start_date = start_date.replace(tzinfo=newsletters[0].date.tzinfo)
            end_date = end_date.replace(tzinfo=newsletters[0].date.tzinfo)
        newsletters_filtered = list(filter(lambda x: x.date >= start_date,
                                           newsletters))
        news.extend(newsletters_filtered)
        if len(newsletters) != len(newsletters_filtered):
            break
        i += 1
        response = requests.get(
            f"https://www.businessinsider.com/s?json=1&q=*&p={i}")
        if not response.ok:
            break
    news = list(filter(lambda x: end_date >= x.date, news))
    return news, len(news) > 0


if __name__ == '__main__':
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
