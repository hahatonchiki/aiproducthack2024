import asyncio

import aiohttp
import numpy as np

from .api import YandexCloud


class Translator:
    counter = 0

    def __init__(self, creds):
        self.yc = YandexCloud(creds)

    def translate(self, text, lang, is_html=False, source_lang="en"):
        async def _translate_text_async():
            async with aiohttp.ClientSession() as session:
                return await self._translate_text(session, text, lang, is_html,
                                                  source_lang)

        return asyncio.run(_translate_text_async())

    async def translate_series(self, series, lang, is_html=False,
                               source_lang_series=None):
        if source_lang_series is None:
            source_lang_series = [None] * len(series)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._translate_text(session, text, lang, is_html, source_lang)
                for text, source_lang in
                zip(series, source_lang_series)
            ]
            translations = await asyncio.gather(*tasks)
            return np.array(translations)

    async def _decrease_counter_after_delay(self):
        await asyncio.sleep(1.2)
        self.counter -= 1

    async def _translate_text(self, session, text, lang, is_html, source_lang):
        while self.counter >= 18:
            await asyncio.sleep(0.1)
        self.counter += 1
        await self._decrease_counter_after_delay()
        url = "https://translate.api.cloud.yandex.net/translate/v2/translate"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        data = {
            'folder_id': self.yc.data['folder_id'],
            'texts': [text],
            'targetLanguageCode': lang,
            'format': 'PLAIN_TEXT' if not is_html else 'HTML'
        }
        if source_lang:
            data['sourceLanguageCode'] = source_lang
        async with session.post(url, headers=headers, json=data) as response:
            response.raise_for_status()
            result = await response.json()
            return result['translations'][0]['text']
