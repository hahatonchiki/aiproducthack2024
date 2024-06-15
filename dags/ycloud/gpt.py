import time

import numpy as np
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

    def _generate(self, text, model, max_tokens, async_, temperature):
        while self.counter >= 5:
            time.sleep(0.1)
        self.counter += 1
        self._decrease_counter_after_delay()
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
        response.raise_for_status()
        response_json = response.json()
        return response_json['id'] if async_ else \
            response_json['result']['alternatives'][0]['message']['text']

    def vectorize(self, text, model):
        return self._vectorize(text, model)

    def _vectorize_series(self, series, model):
        vectors = [self._vectorize(text, model) for text in series]
        return np.array(vectors)

    def vectorize_series(self, series, model):
        return self._vectorize_series(series, model)

    def _decrease_emb_counter_after_delay(self):
        time.sleep(1.2)
        self.emb_counter -= 1

    def _decrease_counter_after_delay(self):
        time.sleep(1.2)
        self.counter -= 1

    def _decrease_get_response_counter_after_delay(self):
        time.sleep(1.2)
        self.get_response_counter -= 1

    def _vectorize(self, text, model):
        text = str(text).strip()
        if not text:
            return []
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/textEmbedding"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.yc.get_token()}"
        }
        data = {
            "modelUri": self.EMB_MODELS[model],
            "text": text
        }
        while self.emb_counter >= 3:
            time.sleep(0.1)
        self.emb_counter += 1
        self._decrease_emb_counter_after_delay()
        response = requests.post(url, headers=headers, json=data)
        try:
            response.raise_for_status()
        except:
            print(response.text)
            response.raise_for_status()
        response_json = response.json()
        return ','.join(np.array(response_json['embedding']).astype(str))

    def _get_result(self, task_id):
        while self.get_response_counter >= 8:
            time.sleep(0.1)
        self.get_response_counter += 1
        self._decrease_get_response_counter_after_delay()
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
                response_json['response']['alternatives'][0]['message']['text']
        return True, None

    def get_result(self, task_id):
        return self._get_result(task_id)

    def generate(self, text, model, max_tokens=100, temperature=0.3):
        return self._generate(text, model, max_tokens, async_=False,
                              temperature=temperature)

    def _get_result_series(self, task_ids):
        results = [self._get_result(task_id) for task_id in task_ids]
        return results

    def _generate_series(self, series, model, max_tokens, async_, temperature):
        response = [self._generate(text, model, max_tokens, async_=async_,
                                   temperature=temperature) for text in series]
        return response

    def generate_series(self, series, model, max_tokens=100, async_=False,
                        temperature=0.3):
        return self._generate_series(series, model, max_tokens, async_,
                                     temperature)

    def get_result_series(self, task_ids):
        return self._get_result_series(task_ids)
