import requests

from ycloud.api import YandexCloud


class Translate:
    def __init__(self, service_account_file):
        self.yc = YandexCloud(service_account_file)

    def translate(self, text, lang, is_html=False, source_lang="en"):
        url = "https://translate.api.cloud.yandex.net/translate/v2/translate"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        data = {
            'folder_id': self.yc.data['folder_id'],
            'texts': [text],
            'targetLanguageCode': lang,
            'sourceLanguageCode': source_lang,
            'format': 'PLAIN_TEXT' if not is_html else 'HTML'
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()['translations'][0]['text']


if __name__ == "__main__":
    t = Translate("service_account.json")
    print(t.translate("<p><a href=\"google.com\">Hello, Google</a>", "ru", source_lang="en", is_html=True))
