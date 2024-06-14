import asyncio
import re
from datetime import datetime
from typing import List, Tuple

import aiohttp
import requests
from bs4 import BeautifulSoup

from parsers import NewsLetter


def parse_date(date):
    date_obj = datetime.strptime(date.strip(), "%M %d, %Y")
    return date_obj


async def get_content_and_date(session, url):
    async with session.get(url) as response:
        if not response.ok:
            return ""
        text = await response.text()
        soup = BeautifulSoup(text, "html.parser")
        date = parse_date(soup.find("h2", class_="subtitle").text)
        soup = soup.find("div", class_="article-page__content-wrapper")
        content = ""
        for p in soup.find_all("p"):
            content += p.get_text() + "\n"
        content = content if content else ""
        content = re.sub(r" +", " ", content)
        content = re.sub(r"[\r\n]+", "\n", content).strip()
        return content


def urls():
    i = 0
    while True:
        yield f"https://vml.com/about/news/?xhr=1&page={i}"
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
        for article in soup.find_all("div", class_="card vertical light "):
            if date < min_date:
                fl_break = True
                break
            title = article.find("div", class_="font-bold").text.strip()
            url = "https://vml.com" + article.get("href")
            news.append(NewsLetter(title, "", "ru", "Евраз", date, url))
        if fl_break:
            break
    news = list(filter(lambda x: x.date <= max_date, news))

    cookie_jar = aiohttp.CookieJar()
    async with aiohttp.ClientSession(cookie_jar=cookie_jar) as session:
        tasks = [get_content_and_date(session, item.url) for item in news]
        contents = await asyncio.gather(*tasks)
        for i, content in enumerate(contents):
            news[i].content = content
    return news, len(news) > 0


def fetch_news(min_date, max_date) -> Tuple[List[NewsLetter], bool]:
    return asyncio.run(_fetch_news(min_date, max_date))


if __name__ == "__main__":
    start_date = datetime(2024, 6, 2)
    end_date = datetime(2024, 6, 8)
    print(fetch_news(start_date, end_date))
