import time

import jwt
import requests


class YandexCloud:
    def __init__(self, creds):
        self.data = creds
        self.jwt_valid_until = 0
        self.iam_valid_until = 0
        self.jwt_value = None
        self.iam_value = None
        self._update_jwt()

    def _update_jwt(self):
        now = int(time.time())
        if self.jwt_valid_until > now:
            return
        payload = {
            'aud': 'https://iam.api.cloud.yandex.net/iam/v1/tokens',
            'iss': self.data['service_account_id'],
            'iat': now,
            'exp': now + 360
        }

        encoded_token = jwt.encode(
            payload,
            self.data['private_key'],
            algorithm='PS256',
            headers={'kid': self.data['id']})

        self.jwt_value = encoded_token
        self.jwt_valid_until = now + 360

    def get_token(self):
        now = int(time.time())
        if self.iam_valid_until > now:
            return self.iam_value
        self._update_jwt()
        url = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
        headers = {
            'Content-Type': 'application/json'
        }
        data = {
            'jwt': self.jwt_value
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        self.iam_value = response.json()['iamToken']
        self.iam_valid_until = now + 3600
        return self.iam_value

    def get_folder_id(self):
        return self.data['folder_id']
