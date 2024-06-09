import asyncio

import aiohttp
import numpy as np
import requests

from .api import YandexCloud


class GPT:
    emb_counter = 0

    def __init__(self, service_account_file):
        self.yc = YandexCloud(service_account_file)
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

    def generate(self, text, model, max_tokens=100, async_=False,
                 temperature=0.3):
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
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        if not response.ok:
            return False, response_json['error']['message']
        if async_:
            return response.ok, response_json['id']
        return (True,
                response_json['result']['alternatives'][0]['message']['text'])

    def vectorize(self, text, model):
        return self._vectorize(text, model)

    async def vectorize_series(self, series, model):
        tasks = [
            self._vectorize(text, model)
            for text in series
        ]
        vectors = await asyncio.gather(*tasks)
        return np.array(vectors)

    async def _decrease_emb_counter_after_delay(self):
        await asyncio.sleep(1.2)
        self.emb_counter -= 1

    async def _decrease_counter_after_delay(self):
        await asyncio.sleep(1.2)
        self.counter -= 1

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
        while self.emb_counter >= 8:
            await asyncio.sleep(0.1)
        self.emb_counter += 1
        await self._decrease_emb_counter_after_delay()
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers,
                                    json=data) as response:
                response.raise_for_status()
                response_json = await response.json()
                return np.array(response_json['embedding'])

    def get_result(self, task_id):
        """
        Get the result of the task

        :raises Exception: Task not found
        :raises Exception: Fetching task failed
        :param task_id: The task id
        :return: A tuple with the first element being a boolean indicating if the task is done and the second element being the result
        """
        url = f"https://operation.api.cloud.yandex.net/operations/{task_id}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            raise Exception("Task not found")
        if not response.ok:
            raise Exception("Fetching task failed")
        response_json = response.json()
        if response_json['done']:
            return True, \
                response_json['response']['alternatives'][0]['message'][
                    'text']
        return False, None
