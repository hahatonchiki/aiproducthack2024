from datetime import datetime
from typing import List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from parse.news import NewsLetter


def get_newsletter_text(url):
    response = requests.get(url)
    if not response.ok:
        return ""
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.find("div", class_="single-post-content").get_text()


def get_from_html(html: str) -> List[NewsLetter]:
    soup = BeautifulSoup(html, 'html.parser')
    news = []
    for html in soup.find_all("div",
                              class_="post post-card type-post post-card-m-f-image"):
        title = html.find("h2").text
        url = html.find("div", class_="post-title").find("a").get("href")
        date = html.find("span", class_="post-date").text
        date = datetime.strptime(date, "%d %B %Y")
        news.append(
            NewsLetter(title, get_newsletter_text(url), "en", "Tech EU", date,
                       url, datetime.now())
        )
    return news


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    URL = "https://tech.eu/news"
    resp = requests.get(URL)
    if not resp.ok:
        return [], False
    resp_text = resp.text
    i = 2
    news = []
    while True:
        news_collected = get_from_html(resp_text)
        if not news_collected:
            if i == 2:
                return [], False
            break
        news_filter = list(
            filter(lambda x: min_date <= x.date, news_collected))
        news.extend(news_filter)
        if len(news_filter) is not len(news_collected):
            break
        URL = f"https://tech.eu/news?page={i}&infinite=true"
        response = requests.get(URL)
        if not response.ok:
            break
        resp_text = response.json()['html']
        i += 1
    news = list(filter(lambda x: x.date <= max_date, news))
    return news, True


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
