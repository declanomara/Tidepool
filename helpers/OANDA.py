import json
import time

import requests


def stream_prices(token, id, instruments, url='https://stream-fxpractice.oanda.com/'):
    headers = {'Authorization': f'Bearer {token}'}
    request_url = f'{url}/v3/accounts/{id}/pricing/stream?instruments={"".join([instrument + "," for instrument in instruments])}'

    with requests.get(request_url, headers=headers, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                datapoint = json.loads(line.decode('utf-8'))
                yield datapoint


class API:
    REFRESH_INTERVAL = 10

    def __init__(self, token, live=False):
        self.token = token
        self.url = 'https://api-fxtrade.oanda.com' if live else 'https://api-fxpractice.oanda.com'
        self.stream_url = 'https://stream-fxtrade.oanda.com' if live else 'https://stream-fxpractice.oanda.com'
        self.accounts = self.get_accounts()

        self.last_refresh = time.time()

    def get_accounts(self):
        endpoint = f'{self.url}/v3/accounts'
        headers = {'Authorization': f'Bearer {self.token}'}

        r = requests.get(endpoint, headers=headers)

        if 'errorMessage' in r.json():
            return False

        accounts = []
        for account in r.json()['accounts']:
            r = requests.get(f'{endpoint}/{account["id"]}', headers=headers)
            accounts.append(r.json()['account'])

        return accounts

    def get_account(self, alias):
        if self.last_refresh < (time.time() - API.REFRESH_INTERVAL):
            self.accounts = self.get_accounts()

        for account in self.accounts:
            if account['alias'] == alias:
                return account

    def stream_prices(self, alias, instruments):
        id = self.get_account(alias)['id']
        return stream_prices(self.token, id, instruments, self.stream_url)

    def get_instruments(self, alias):
        id = self.get_account(alias)['id']

        endpoint = f'{self.url}/v3/accounts/{id}/instruments'
        headers = {'Authorization': f'Bearer {self.token}'}

        r = requests.get(endpoint, headers=headers)

        if 'errorMessage' in r.json():
            return False

        return [instrument['name'] for instrument in r.json()['instruments']]

