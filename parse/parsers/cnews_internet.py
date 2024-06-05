import datetime
from typing import List, Tuple
import pandas as pd
import requests
from bs4 import BeautifulSoup
from final.news import NewsLetter
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_newsletter_text(url):
    try:
        response = requests.get(url, timeout=10)
        if not response.ok:
            print(f"Не удалось получить URL: {url}")
            return ""
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.find("article", class_="news_container").find('p')
        if text:
            return text.get_text()
        print(f"Не удалось найти текст статьи по URL: {url}")
        return ""
    except Exception as e:
        print(f"Исключение при получении URL {url}: {e}")
        return ""


def fetch_newsletter_texts(newsletters: List[NewsLetter], max_workers: int = 10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_newsletter = {executor.submit(get_newsletter_text, newsletter.url): newsletter for newsletter in newsletters}
        for future in as_completed(future_to_newsletter):
            newsletter = future_to_newsletter[future]
            try:
                newsletter_text = future.result()
                newsletter.content = newsletter_text
            except Exception as e:
                print(f"Исключение при получении текста новостной рассылки: {e}")


def get_from_html(html: str) -> List[NewsLetter]:
    soup = BeautifulSoup(html, 'xml')
    news = []
    articles = soup.find_all('item')
    for article in articles:
        title = article.find('title').get_text()
        url = article.find('link').get_text()
        date = article.find('pubDate').get_text()
        date = datetime.datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %z")
        news.append(NewsLetter(title, None, "ru", "CNews", date, url, datetime.datetime.now()))
    return news


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    URL = "https://www.cnews.ru/inc/rss/internet.xml"
    response = requests.get(URL)
    if not response.ok:
        print("Не удалось получить RSS ленту.")
        return [], False
    print("Получено и обработано содержимое RSS-ленты.")
    news_collected = get_from_html(response.text)
    if not news_collected:
        print("Новости не были собраны при первой попытке.")
        return [], False
    print(f"Собрано {len(news_collected)} новостей.")
    news_filter = [x for x in news_collected if min_date <= x.date <= max_date]
    print(f"Количество отфильтрованных новостей: {len(news_filter)}")
    return news_filter, True


if __name__ == "__main__":
    start_date = datetime.datetime.strptime("2024-05-30", "%Y-%m-%d").replace(tzinfo=pytz.timezone('Europe/Moscow'))
    end_date = datetime.datetime.strptime("2024-06-05", "%Y-%m-%d").replace(tzinfo=pytz.timezone('Europe/Moscow'))
    res, success = fetch_news(start_date, end_date)

    if not success:
        print("Не удалось получить новости в указанном диапазоне дат.")
    else:
        fetch_newsletter_texts(res)
        data = []
        for newsletter in res:
            data.append({
                'Title': newsletter.title,
                'Content': newsletter.content,
                'Language': newsletter.language,
                'Source': newsletter.source,
                'Date': newsletter.date.strftime("%Y-%m-%d %H:%M:%S%z"),
                'URL': newsletter.url,
                'Parsed_At': newsletter.parsed_at
            })
        for item in data:
            print(item)
        df = pd.DataFrame(data)
        try:
            file = pd.read_csv('ru_news.csv')
            df.to_csv('ru_news.csv', mode='a', index=False, header=False)
        except Exception as e:
            print(f"Не удалось добавить данные в CSV-файл: {e}")
            df.to_csv('ru_news.csv', mode='a', index=False, header=True)
