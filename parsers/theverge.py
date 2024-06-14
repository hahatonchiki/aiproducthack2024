import datetime

import requests
from bs4 import BeautifulSoup

from parse.news import NewsLetter


class TheVergeParser:
    def parce(self, html) -> [NewsLetter]:
        soup = BeautifulSoup(html, "html.parser")
        news = []
        for article in soup.find_all("div",
                                     class_="c-entry-box--compact"):
            print(article)
            title = article.find("h2").find("a").text
            url = "https://theverge.com" + article.find("h2").find("a").get(
                "href")
            date = datetime.datetime.strptime(
                article.find("time").get("datetime"), "%Y-%m-%dT%H:%M:%S")
            parsed_at = datetime.datetime.now()
            news = NewsLetter(title, "", "en", "The Verge", date, url,
                              parsed_at)
        return news


if __name__ == "__main__":
    html = requests.get(
        "https://www.theverge.com/archives/tech/2024/6").text
    parser = TheVergeParser()
    parser.parce(html)
