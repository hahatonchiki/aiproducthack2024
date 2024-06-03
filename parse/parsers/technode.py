from datetime import datetime
from typing import List, Tuple
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from parse.news import NewsLetter


def get_newsletter_text(url):
    response = requests.get(url)
    if not response.ok:
        return ""
    soup = BeautifulSoup(response.text, 'html.parser')
    soup = soup.find("div", class_="entry-content")
    tt = soup.find("h2", string="Support TechNode")
    if tt is not None:
        tt.parent.parent.decompose()
    tt = soup.find(class_="jp-relatedposts")
    if tt is not None:
        tt.decompose()
    resp = soup.get_text()
    resp = re.sub(r"\n+", "\n", resp)
    return resp


def get_from_html(json_obj: list) -> List[NewsLetter]:
    news = []
    for html in map(lambda x: x['html'], json_obj):
        soup = BeautifulSoup(html, 'html.parser')
        soup = soup.find("div", class_="entry-wrapper")
        title = soup.find("a").text
        url = soup.find("a").get("href")
        date = soup.find("time", class_="entry-date published").get(
            "datetime")[:10]
        date = datetime.strptime(date, "%Y-%m-%d")
        news.append(
            NewsLetter(title, get_newsletter_text(url), "en", "TechNode", date,
                       url, datetime.now())
        )
    return news


def urls():
    i = 1
    while True:
        yield f"https://technode.com/wp-json/newspack-blocks/v1/articles?deduplicate=1&page={i}&postsToShow=20"
        i += 1


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    news = []
    for url in urls():
        response = requests.get(url)
        if not response.ok:
            break
        resp_text = response.json()['items']
        newsletters = get_from_html(resp_text)
        if not newsletters:
            break
        newsletters_filter = list(
            filter(lambda x: min_date <= x.date, newsletters))
        news.extend(newsletters_filter)
        if len(newsletters_filter) != len(newsletters):
            break
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
