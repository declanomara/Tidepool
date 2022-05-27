import json
import time
import requests

from typing import Generator, Optional


def stream_prices(
    token: str,
    id: str,
    instruments: list,
    url: str = "https://stream-fxpractice.oanda.com",
) -> Generator[dict, None, None]:
    """
    :param token: Authorization token for OANDA account
    :param id: OANDA account ID
    :param instruments: List of instruments to stream prices for
    :param url: URL to stream prices from
    :rtype: dict
    """

    headers = {"Authorization": f"Bearer {token}"}
    request_url = f'{url}/v3/accounts/{id}/pricing/stream?instruments={"".join([instrument + "," for instrument in instruments])}'

    with requests.get(request_url, headers=headers, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                datapoint = json.loads(line.decode("utf-8"))
                yield datapoint


class API:
    REFRESH_INTERVAL = 10

    def __init__(self, token: str, live: bool = False) -> None:
        """
        :param token: OANDA authorization token
        :param live: Whether to use live or practice endpoint
        :rtype: None
        """
        self.token = token
        self.url = (
            "https://api-fxtrade.oanda.com"
            if live
            else "https://api-fxpractice.oanda.com"
        )
        self.stream_url = (
            "https://stream-fxtrade.oanda.com"
            if live
            else "https://stream-fxpractice.oanda.com"
        )
        self.accounts = self.get_accounts()

        self.last_refresh = time.time()

    def get_accounts(self) -> list:
        """
        :return: Returns a list of subaccounts for given OANDA account
        :rtype: list
        """
        endpoint = f"{self.url}/v3/accounts"
        headers = {"Authorization": f"Bearer {self.token}"}

        r = requests.get(endpoint, headers=headers)

        if "errorMessage" in r.json().keys():
            return r.json()["errorMessage"]

        try:
            accounts = []
            for account in r.json()["accounts"]:
                r = requests.get(f'{endpoint}/{account["id"]}', headers=headers)
                accounts.append(r.json()["account"])

            return accounts

        except KeyError:
            print(r.json())
            raise

    def get_account(self, alias: str) -> dict:
        """
        :param alias: Alias for the desired OANDA subaccount
        :return: Returns a dictionary containing account information
        :rtype: dict
        """
        if self.last_refresh < (time.time() - API.REFRESH_INTERVAL):
            self.accounts = self.get_accounts()

        for account in self.accounts:
            if account["alias"] == alias:
                return account

    def stream_prices(
        self, alias: str, instruments: list
    ) -> Generator[dict, None, None]:
        """
        :rtype: Generator[dict]
        :param alias: Alias of the desired OANDA subaccount
        :param instruments: List of instruments to stream prices for
        :return: Returns a generator for dict objects containing price data
        """
        id = self.get_account(alias)["id"]
        return stream_prices(self.token, id, instruments, self.stream_url)

    def get_instruments(self, alias: str) -> Optional[list]:
        """
        :param alias: Alias for desired OANDA subaccount
        :return: Returns a list of instruments available to trade for the specified subaccount
        :rtype: list
        """
        id = self.get_account(alias)["id"]

        endpoint = f"{self.url}/v3/accounts/{id}/instruments"
        headers = {"Authorization": f"Bearer {self.token}"}

        r = requests.get(endpoint, headers=headers)

        if "errorMessage" in r.json():
            return None

        return [instrument["name"] for instrument in r.json()["instruments"]]
