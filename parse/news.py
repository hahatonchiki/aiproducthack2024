class NewsLetter:
    def __init__(self, title, content, language, source, date, url, parsed_at):
        self.title = title
        self.content = content
        self.language = language
        self.source = source
        self.date = date
        self.url = url
        self.parsed_at = parsed_at

    def __str__(self):
        return f"{self.title} ({self.date})"
