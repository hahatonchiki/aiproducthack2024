import asyncio
import re
from datetime import datetime
from typing import List, Tuple

import aiohttp
import requests
from bs4 import BeautifulSoup

from parsers import NewsLetter


async def get_content(session, url):
    async with session.get(url, ssl=False) as response:
        if not response.ok:
            return ""
        text = await response.text()
        soup = BeautifulSoup(text, "html.parser")
        soup = soup.find("div", class_="article-page__content-wrapper")
        content = ""
        for p in soup.find_all("p"):
            content += p.get_text() + "\n"
        content = content if content else ""
        content = re.sub(r" +", " ", content)
        content = re.sub(r"[\r\n]+", "\n", content).strip()
        return content


def parse_date(date):
    date_obj = datetime.strptime(date.strip(), "%d.%m.%Y")
    return date_obj


def urls():
    i = 1
    while True:
        yield f"https://evraz.market/about/news/?PAGEN_3={i}"
        i += 1


async def _fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    news = []
    for url in urls():
        response = requests.get(url, verify=False)
        if not response.ok:
            break
        text = response.text
        soup = BeautifulSoup(text, "html.parser")
        fl_break = False
        for article in soup.find_all("a", class_="news-page__item"):
            date = parse_date(
                article.find("div", class_="news-page__item-date").text)
            if date < min_date:
                fl_break = True
                break
            title = article.find("div", class_="font-bold").text.strip()
            url = "https://evraz.market" + article.get("href")
            news.append(NewsLetter(title, "", "ru", "Евраз", date, url))
        if fl_break:
            break
    news = list(filter(lambda x: x.date <= max_date, news))

    cookie_jar = aiohttp.CookieJar()
    async with aiohttp.ClientSession(cookie_jar=cookie_jar) as session:
        tasks = [get_content(session, item.url) for item in news]
        contents = await asyncio.gather(*tasks)
        for i, content in enumerate(contents):
            news[i].content = content
    return news, len(news) > 0


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    return asyncio.run(_fetch_news(min_date, max_date))
