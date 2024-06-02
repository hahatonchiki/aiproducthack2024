from time import sleep

import requests

from ycloud.api import YandexCloud


class GPT:

    def __init__(self, service_account_file):
        self.yc = YandexCloud(service_account_file)
        self.MODELS = {
            "pro": f"gpt://{self.yc.get_folder_id()}/yandexgpt/latest",
            "lite": f"gpt://{self.yc.get_folder_id()}/yandexgpt-lite/latest",
            "literc": f"gpt://{self.yc.get_folder_id()}/yandexgpt-lite/rc",
            "summary": f"gpt://{self.yc.get_folder_id()}/summarization/latest",
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


if __name__ == '__main__':
    gpt = GPT("service_account.json")
    ok, response = gpt.generate(
        "(не)Наша (не)гениальная статья которую мы (наверное) хотим самаризировать",
        "summary", async_=False)
    print(ok, response)
    ok, task_id = gpt.generate(
        "Что делать если Яндекс использует рабский труд детей?",
        "pro", async_=True)
    sleep(5)
    print(gpt.get_result(task_id))
