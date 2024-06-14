import asyncio

import aiohttp
import numpy as np
import pandas as pd
import requests

from .api import YandexCloud


class GPT:
    emb_counter = 0
    counter = 0
    get_response_counter = 0

    def __init__(self, creds):
        self.yc = YandexCloud(creds)
        folder_id = self.yc.get_folder_id()
        self.MODELS = {
            "pro": f"gpt://{folder_id}/yandexgpt/latest",
            "lite": f"gpt://{folder_id}/yandexgpt-lite/latest",
            "literc": f"gpt://{folder_id}/yandexgpt-lite/rc",
            "summary": f"gpt://{folder_id}/summarization/latest",
        }
        self.EMB_MODELS = {
            "doc": f"emb://{folder_id}/text-search-doc/latest",
            "query": f"emb://{folder_id}/text-search-query/latest"
        }

    async def _generate(self, text, model, max_tokens, async_, temperature):
        while self.counter >= 8:
            await asyncio.sleep(0.1)
        self.counter += 1
        await self._decrease_counter_after_delay()
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        if async_:
            url += "Async"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        data = {
            "modelUri": self.MODELS[model],
            "completionOptions": {
                "maxTokens": max_tokens,
                "stream": False,
                "temperature": temperature
            },
            "messages": [
                {
                    "role": "user",
                    "text": text
                }
            ]
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers,
                                    json=data) as response:
                response.raise_for_status()
                response_json = await response.json()
                return response_json['id'] if async_ else \
                    response_json['result'][
                        'alternatives'][0]['message']['text']

    def vectorize(self, text, model):
        return asyncio.run(self._vectorize(text, model))

    async def _vectorize_series(self, series, model):
        tasks = [
            self._vectorize(text, model)
            for text in series
        ]
        vectors = await asyncio.gather(*tasks)
        return np.array(vectors)

    def vectorize_series(self, series, model):
        return asyncio.run(self._vectorize_series(series, model))

    async def _decrease_emb_counter_after_delay(self):
        await asyncio.sleep(1.2)
        self.emb_counter -= 1

    async def _decrease_counter_after_delay(self):
        await asyncio.sleep(1.2)
        self.counter -= 1

    async def _decrease_get_response_counter_after_delay(self):
        await asyncio.sleep(1.2)
        self.get_response_counter -= 1

    async def _vectorize(self, text, model):
        """
        Get the vector representation of the text using the specified model
        :param text: The text to vectorize
        :param model: The model to use(doc or query)
        :return:
        """
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/textEmbedding"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        data = {
            "modelUri": self.EMB_MODELS[model],
            "text": text
        }
        while self.emb_counter >= 7:
            await asyncio.sleep(0.1)
        self.emb_counter += 1
        await self._decrease_emb_counter_after_delay()
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers,
                                    json=data) as response:
                response.raise_for_status()
                response_json = await response.json()
                return pd.array(response_json['embedding'])

    async def _get_result(self, task_id):
        while self.get_response_counter >= 8:
            asyncio.sleep(0.1)
        self.get_response_counter += 1
        await self._decrease_get_response_counter_after_delay()
        url = f"https://operation.api.cloud.yandex.net/operations/{task_id}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        response = requests.get(url, headers=headers)
        if not response.ok:
            return False, None
        response_json = response.json()
        if response_json['done']:
            return True, \
                response_json['response']['alternatives'][0]['message'][
                    'text']
        return True, None

    def get_result(self, task_id):
        return asyncio.run(self._get_result(task_id))

    def generate(self, text, model, max_tokens=100, temperature=0.3):
        return asyncio.run(
            self._generate(text, model, max_tokens, async_=False,
                           temperature=temperature))

    async def _get_result(self, task_id):
        url = f"https://operation.api.cloud.yandex.net/operations/{task_id}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                response_json = await response.json()
                if response_json['done']:
                    return response_json['response']['alternatives'][0][
                        'message']['text']
                return None

    async def _get_result_series(self, task_ids):
        tasks = [
            self._get_result(task_id)
            for task_id in task_ids
        ]
        results = await asyncio.gather(*tasks)
        return results

    async def _generate_series(self, series, model, max_tokens, async_,
                               temperature):
        tasks = [
            self._generate(text, model, max_tokens, async_=async_,
                           temperature=temperature)
            for text in series
        ]
        response = await asyncio.gather(*tasks)
        return response

    def generate_series(self, series, model, max_tokens=100, async_=False,
                        temperature=0.3):
        return asyncio.run(
            self._generate_series(series, model, max_tokens, async_,
                                  temperature))

    def get_result_series(self, task_ids):
        return asyncio.run(self._get_result_series(task_ids))
