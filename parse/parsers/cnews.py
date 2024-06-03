import requests
from bs4 import BeautifulSoup
import csv
import os
import time


def fetch_latest_news():
    url = 'https://www.cnews.ru/news'  # URL раздела

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch the telecom page. Status code: {response.status_code}")

    soup = BeautifulSoup(response.text, 'html.parser')

    latest_news_section = soup.find('div', class_='news_main_outline').find('section', class_='d-flex flex-wrap justify-content-between').find('a', class_='newstoplist_main d-flex flex-column col-5-5 p-0 flex-shrink-0')

    if latest_news_section is None:
        raise Exception("Could not find the latest news section. Check 'kommersant_telecom_page.html' for debugging.")

    news_url = latest_news_section['href']

    if news_url is None:
        raise Exception("Could not find the news link")

    response = requests.get(news_url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch the news page. Status code: {response.status_code}")

    soup = BeautifulSoup(response.text, 'html.parser')
    latest_news = soup.find('article', class_='news_container')

    if latest_news is None:
        raise Exception("Could not find the news content body")

    title = latest_news.find('h1').get_text(strip=True)
    date = latest_news.find('div', 'article_date').find('time', class_='article-date-desktop').get_text(strip=True)
    text = latest_news.find('p').get_text(strip=True)

    return {
        'title': title,
        'link': news_url,
        'date': date,
        'text': text
    }


def write_to_csv(news):
    file_exists = os.path.isfile('news.csv')
    with open('news.csv', 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'link', 'date', 'text']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerow(news)


def main():
    last_news_url = None

    while True:
        try:
            latest_news = fetch_latest_news()
            if latest_news['link'] != last_news_url:
                write_to_csv(latest_news)
                last_news_url = latest_news['link']
                print(latest_news)
        except Exception as e:
            print(f"An error occurred: {e}")

        # Wait for 10 minutes before checking for new news
        time.sleep(300)


if __name__ == "__main__":
    main()
