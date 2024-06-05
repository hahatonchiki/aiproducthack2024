import datetime
import sys
import pandas as pd
import pytz

name = ''

if __name__ == "__main__":
    if name == 'cnews_biz':
        from cnews_biz import fetch_news, fetch_newsletter_texts
    elif name == 'cnews_internet':
        from cnews_internet import fetch_news, fetch_newsletter_texts
    elif name == 'cnews_telecom':
        from cnews_telecom import fetch_news, fetch_newsletter_texts
    elif name == 'kommersant':
        from kommersant import fetch_news, fetch_newsletter_texts
    elif name == 'habr':
        from habr import fetch_news, fetch_newsletter_texts
    elif name == 'vedomosti':
        from vedomosti import fetch_news, fetch_newsletter_texts
    elif name == 'interfax':
        from interfax import fetch_newsletter_texts, fetch_news
    else:
        print('Неверно введено имя файла')
        sys.exit()

    start_date = datetime.datetime.strptime("2024-05-30", "%Y-%m-%d").replace(tzinfo=None)
    end_date = datetime.datetime.strptime("2024-06-05", "%Y-%m-%d").replace(tzinfo=None)
    res, success = fetch_news(start_date, end_date)

    if not success:
        print("Failed to fetch news within the specified date range.")
    else:
        fetch_newsletter_texts(res)
        data = []
        for newsletter in res:

            data.append([newsletter.title,
                         newsletter.content,
                         newsletter.language,
                         newsletter.source,
                         newsletter.date.strftime("%Y-%m-%d %H:%M:%S%z"),
                         newsletter.url,
                         newsletter.parsed_at
                         ]
                        )

            # data.append({
            #     'Title': newsletter.title,
            #     'Content': newsletter.content,
            #     'Language': newsletter.language,
            #     'Source': newsletter.source,
            #     'Date': newsletter.date.strftime("%Y-%m-%d %H:%M:%S%z"),  # Форматирование даты
            #     'URL': newsletter.url,
            #     'Parsed_At': newsletter.parsed_at
            # })

        # Выводим данные в консоль для проверки
        # for item in data:
        #     print(item)
        #
        # df = pd.DataFrame(data)

        print(data)

        # Добавление результата в csv файл

        # try:
        #     file = pd.read_csv('ru_news.csv')
        #     df.to_csv('ru_news.csv', mode='a', index=False, header=False)
        # except Exception as e:
        #     print(f"Failed to append to CSV file: {e}")
        #     df.to_csv('ru_news.csv', mode='a', index=False, header=True)

