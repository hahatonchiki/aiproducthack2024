import datetime
from typing import List, Tuple
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup
from final.news import NewsLetter  # Убедитесь, что этот импорт работает корректно
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_newsletter_text(url):
    try:
        response = requests.get(url, timeout=10)
        if not response.ok:
            print(f"Failed to fetch URL: {url}")
            return ""
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.find("article", itemprop_="articleBody")
        if text:
            paragraph = text.find('p')
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
            if category == 'Экономика':
                title = entry.title
                url = entry.link
                date = entry.published
                # Изменение формата даты
                date = datetime.datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %z")
                news.append(NewsLetter(title, None, "ru", "Interfax", date, url, datetime.datetime.now()))
    return news


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    URL = "https://www.interfax.ru/rss.asp"
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


def fetch_newsletter_texts(newsletters: List[NewsLetter], max_workers: int = 10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_newsletter = {executor.submit(get_newsletter_text, newsletter.url): newsletter for newsletter in
                                newsletters}
        for future in as_completed(future_to_newsletter):
            newsletter = future_to_newsletter[future]
            try:
                newsletter_text = future.result()
                newsletter.content = newsletter_text
            except Exception as e:
                print(f"Exception fetching newsletter text: {e}")


if __name__ == "__main__":
    start_date = datetime.datetime.strptime("2024-05-30", "%Y-%m-%d").replace(tzinfo=pytz.timezone('Europe/Moscow'))
    end_date = datetime.datetime.strptime("2024-06-05", "%Y-%m-%d").replace(tzinfo=pytz.timezone('Europe/Moscow'))
    res, success = fetch_news(start_date, end_date)

    if not success:
        print("Failed to fetch news within the specified date range.")
    else:
        fetch_newsletter_texts(res)
        data = []
        for newsletter in res:
            data.append({
                'Title': newsletter.title,
                'Content': newsletter.content,
                'Language': newsletter.language,
                'Source': newsletter.source,
                'Date': newsletter.date.strftime("%Y-%m-%d %H:%M:%S%z"),  # Форматирование даты
                'URL': newsletter.url,
                'Parsed_At': newsletter.parsed_at
            })

        # Выводим данные в консоль для проверки
        for item in data:
            print(item)

        df = pd.DataFrame(data)
        try:
            file = pd.read_csv('ru_news.csv')
            df.to_csv('ru_news.csv', mode='a', index=False, header=False)
        except Exception as e:
            print(f"Failed to append to CSV file: {e}")
            df.to_csv('ru_news.csv', mode='a', index=False, header=True)

