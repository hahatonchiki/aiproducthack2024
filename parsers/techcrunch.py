import datetime

import requests
from bs4 import BeautifulSoup

from parse.news import NewsLetter


class TheVergeParser:
    def parce(self, html) -> [NewsLetter]:
        soup = BeautifulSoup(html, "html.parser")
        news = []
        for article in soup.find_all("div",
                                     class_="wp-block-tc23-post-picker"):
            title = article.find("h2").find("a").text
            url = "https://theverge.com" + article.find("h2").find("a").get(
                "href")
            parsed_at = datetime.datetime.now()
            content = ""
            # content = ' '.join(
            #     p.get_text() for p in soup.find_all('p'))
            news = NewsLetter(title, content, "en", "TechCrunch",
                              datetime.datetime.now(), url,
                              parsed_at)
        return news


if __name__ == "__main__":
    html = requests.get(
        "https://techcrunch.com/category/artificial-intelligence/page/1/").text
    parser = TheVergeParser()
    parser.parce(html)
