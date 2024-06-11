import asyncio
import re
from datetime import datetime
from typing import List, Tuple
import ssl

import aiohttp
from bs4 import BeautifulSoup

from parsers import NewsLetter


async def get_content(session, url):
    async with session.get(url, ssl=False) as response:
        if not response.ok:
            return ""
        text = await response.text()
        soup = BeautifulSoup(text, "html.parser")
        content = soup.find("div", class_="news-detail")
        content = content.get_text() if content else ""
        content = re.sub(r" +", " ", content)
        content = re.sub(r"[\r\n]+", "\n", content).strip()
        return content


async def parse_page(session, soup):
    news = []
    tasks = []
    for article in soup.find_all("a", class_="news-list-card"):
        title = article.find("h3").text
        title = re.sub(r" +", " ", title)
        title = re.sub(r"[\r\n]+", "\n", title).strip()
        url = "https://severstal.com" + article.get("href")
        date = parse_date(
            article.find("p", class_="text-2 news-list-card__date").text)
        tasks.append((title, date, url, get_content(session, url)))
    for title, date, url, content_task in tasks:
        content = await content_task
        news.append(NewsLetter(title, content, "ru", "Северсталь", date, url,
                               datetime.now()))
    return news


def get_max_page(soup):
    el = soup.find("button",
                   class_="page-pagination__item page-pagination__item_total")
    if el:
        return int(el.text)
    return 1


def parse_date(date):
    date = date.strip()
    months = {
        'января': 'January', 'февраля': 'February', 'марта': 'March',
        'апреля': 'April', 'мая': 'May', 'июня': 'June', 'июля': 'July',
        'августа': 'August', 'сентября': 'September', 'октября': 'October',
        'ноября': 'November', 'декабря': 'December'
    }
    day, month, year = date.split()
    month = months[month]
    new_date_str = f"{day} {month} {year}"
    date_obj = datetime.strptime(new_date_str, "%d %B %Y")
    return date_obj


async def fetch_page(session, url):
    async with session.get(url, ssl=False) as response:
        if not response.ok:
            return []
        text = await response.text()
        soup = BeautifulSoup(text, "html.parser")
        return await parse_page(session, soup)


def fetch_news(min_date: datetime, max_date: datetime) -> Tuple[List[NewsLetter], bool]:
    min_date_str = min_date.strftime("%Y-%m-%d")
    max_date_str = max_date.strftime("%Y-%m-%d")
    base_url = f"https://severstal.com/rus/media/archive/?dateFrom={min_date_str}&dateTo={max_date_str}"

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async def async_fetch_news():
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(base_url, ssl=False) as response:
                if not response.ok:
                    return [], False
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                max_page = get_max_page(soup)
                tasks = [fetch_page(session, base_url)]
                for i in range(2, max_page + 1):
                    url = f"{base_url}&PAGEN_1={i}"
                    tasks.append(fetch_page(session, url))
                results = await asyncio.gather(*tasks)
                news = [item for sublist in results for item in sublist]
                return news, True

    news, success = asyncio.run(async_fetch_news())
    return news, success
