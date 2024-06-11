class NewsLetter:
    def __init__(self, title, content, language, source, date, url, parsed_at):
        if content is None:
            content = ""
        self.title = title
        self.content = content
        self.language = language
        self.source = source
        self.date = date
        self.url = url

    def __str__(self):
        return f"{self.title} ({self.date})"
