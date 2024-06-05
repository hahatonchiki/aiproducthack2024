import requests
from bs4 import BeautifulSoup
import csv
import time

# Словарь сайтов и соответствующих URL
sites = {
    "CNews_Telecom": "https://www.cnews.ru/inc/rss/telecom.xml",
    "CNews_Biz": "https://www.cnews.ru/inc/rss/biz.xml",
    "CNews_Internet": "https://www.cnews.ru/inc/rss/internet.xml",
    "TAdviser": "https://www.tadviser.ru/xml/tadviser.xml",
    "Habr": "https://habr.com/ru/rss/news/rated25/?fl=ru&limit=100",
    "Kommersant": "https://www.kommersant.ru/RSS/news.xml",
    "Vedomosti": "https://www.vedomosti.ru/rss/news",
    "Interfax": "https://www.interfax.ru/rss.asp"
}


# Функция для парсинга новостей с каждого сайта


def parse_news(site_name, url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'xml')  # Используем lxml парсер для XML документов
    news_list = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    if site_name in ["CNews_Internet", "CNews_Telecom", "CNews_Biz"]:
        articles = soup.find_all('item')
        for article in articles:
            title = article.find('title')
            link = article.find('link')
            date = article.find('pubDate')
            text = article.find('description')
            if title and link and date and text:
                news_list.append([title.get_text(), link.get_text(), date.get_text(), text.get_text().strip()])

    elif site_name == "TAdviser":
        articles = soup.find_all('item')
        for article in articles:
            title = article.find('title')
            link = article.find('link')
            date = article.find('pubDate')
            if title and link and date:
                tad_response = requests.get(link.get_text(), headers=headers)
                tad_soup = BeautifulSoup(tad_response.content, 'html.parser')
                text = tad_soup.find('div', class_='pub_body')
                if text:
                    text = text.find('p')
                    if text:
                        news_list.append(
                            [title.get_text(), link.get_text(), date.get_text(), text.get_text(strip=True)])

    elif site_name == "Habr":
        articles = soup.find_all('item')
        for article in articles:
            title = article.find('title')
            link = article.find('link')
            date = article.find('pubDate')
            text = article.find('description')
            if title and link and date and text:
                news_list.append([title.get_text(), link.get_text(), date.get_text(), text.get_text().strip()])

    elif site_name == "Kommersant":
        articles = soup.find_all('item')
        for article in articles:
            category = article.find('category')
            if category and category.get_text() in ['Телекоммуникации', 'Бизнес', 'Hi-Tech']:
                title = article.find('title')
                link = article.find('link')
                date = article.find('pubDate')
                text = article.find('description')
                if title and link and date and text:
                    news_list.append([title.get_text(), link.get_text(), date.get_text(), text.get_text().strip()])

    elif site_name == 'Interfax':
        articles = soup.find_all('item')
        for article in articles:
            category = article.find('category')
            if category and category.get_text() == 'Экономика':
                title = article.find('title')
                link = article.find('link')
                date = article.find('pubDate')
                text = article.find('description')
                if title and link and date and text:
                    news_list.append([title.get_text(), link.get_text(), date.get_text(), text.get_text().strip()])

    elif site_name == "Vedomosti":
        articles = soup.find_all('item')
        for article in articles:
            category = article.find('category')
            if category and category.get_text() in ['Технологии', 'Бизнес', 'Инвестиции']:
                title = article.find('title')
                link = article.find('link')
                date = article.find('pubDate')
                if title and link and date:
                    ved_response = requests.get(link.get_text(), headers=headers)
                    ved_soup = BeautifulSoup(ved_response.content, 'html.parser')
                    text = ved_soup.find('div', class_='article-boxes-list article__boxes')
                    if text:
                        text = text.find('div', class_='article-boxes-list__item')
                        if text:
                            text = text.find('p', class_='box-paragraph__text')
                            if text:
                                news_list.append(
                                    [title.get_text(), link.get_text(), date.get_text(), text.get_text(strip=True)])

    return news_list


def load_existing_news(filename):
    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            existing_news = set(tuple(row) for row in reader)
    except FileNotFoundError:
        existing_news = set()
    return existing_news


def save_news(filename, news):
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for item in news:
            writer.writerow(item)


# Имя CSV файла
csv_file = 'news.csv'
existing_news = load_existing_news(csv_file)

print("Starting news parser...")
while True:
    new_news = []
    for site_name, url in sites.items():
        try:
            news = parse_news(site_name, url)
            for item in news:
                news_tuple = (site_name, item[0], item[1], item[2], item[3])
                if news_tuple not in existing_news:
                    new_news.append(news_tuple)
                    existing_news.add(news_tuple)
        except Exception as e:
            print(f"Error parsing {site_name}: {e}")

    if new_news:
        save_news(csv_file, new_news)
        print(f"Added {len(new_news)} new articles.")

    time.sleep(600)  # Пауза 10 минут (600 секунд)
