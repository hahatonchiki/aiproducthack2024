import datetime
from typing import List, Tuple
from concurrent.futures import as_completed, ThreadPoolExecutor

import requests
import feedparser
from bs4 import BeautifulSoup

from . import NewsLetter


def get_newsletter_text(url):
    try:
        response = requests.get(url, timeout=10)
        if not response.ok:
            print(f"Failed to fetch URL: {url}")
            return ""
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.find("div", class_="article-boxes-list article__boxes")
        if text:
            paragraph = text.find('p', class_='box-paragraph__text')
            if paragraph:
                return paragraph.get_text()
        print(f"Failed to find article text for URL: {url}")
        return ""
    except Exception as e:
        print(f"Exception fetching URL {url}: {e}")
        return ""


def get_from_feed(feed) -> List[NewsLetter]:
    news = []
    for entry in feed.entries:
        category = entry.get('category', None)
        if category:
            print(f"Article category: {category}")
            if category in ['Технологии', 'Бизнес', 'Инвестиции',
                            'Финансы / Банки']:
                title = entry.title
                url = entry.link
                date = entry.published
                date = datetime.datetime.strptime(date,
                                                  "%a, %d %b %Y %H:%M:%S %z")
                date = date.replace(tzinfo=None)
                news.append(
                    NewsLetter(title, None, "ru", "Ведомости", date, url,
                               datetime.datetime.now()))
    return news


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    URL = "https://www.kommersant.ru/RSS/news.xml"
    response = requests.get(URL)
    if not response.ok:
        print("Failed to fetch the RSS feed.")
        return [], False

    print("Fetched and parsed the RSS feed.")

    feed = feedparser.parse(response.text)
    news_collected = get_from_feed(feed)
    if not news_collected:
        print("No news collected in the first attempt.")
        return [], False

    print(f"Collected {len(news_collected)} news articles.")

    # Фильтрация по дате
    news_filter = [x for x in news_collected if min_date <= x.date <= max_date]
    print(f"Filtered news count: {len(news_filter)}")

    return news_filter, True


def fetch_newsletter_texts(newsletters: List[NewsLetter],
                           max_workers: int = 10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_newsletter = {
            executor.submit(get_newsletter_text, newsletter.url): newsletter
            for newsletter in
            newsletters}
        for future in as_completed(future_to_newsletter):
            newsletter = future_to_newsletter[future]
            try:
                newsletter_text = future.result()
                newsletter.content = newsletter_text
            except Exception as e:
                print(f"Exception fetching newsletter text: {e}")
