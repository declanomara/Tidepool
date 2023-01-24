import time
import requests

from typing import Generator, Optional


class Instrument:
    def __init__(self, instrument_data: dict) -> None:
        """
        :param instrument_data: Dictionary containing instrument information
        :rtype: None
        """

        self.unpack_data(instrument_data)

    def unpack_data(self, data: dict) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        return f"Instrument({self.displayName})"

    def __str__(self) -> str:
        return f"{self.displayName}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Instrument):
            return NotImplemented

        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


class Account:
    def __init__(self, account_data: dict, token: str, url: str, stream_url: str) -> None:
        """
        :param account_info: Dictionary containing account information
        :rtype: None
        """

        self.unpack_data(account_data)

        self.token = token
        self.url = url
        self.stream_url = stream_url

    def unpack_data(self, data: dict) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        return f"Account({self.alias}, {self.id})"

    def __str__(self) -> str:
        return f"{self.alias} ({self.id})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Account):
            return NotImplemented

        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def get_instruments(self) -> list[Instrument]:
        """
        :return: Returns a list of instruments available to trade for the specified subaccount
        :rtype: list
        """
        endpoint = f"{self.url}/v3/accounts/{self.id}/instruments"
        headers = {"Authorization": f"Bearer {self.token}"}

        r = requests.get(endpoint, headers=headers)

        if "errorMessage" in r.json():
            print(r.json()["errorMessage"])
            return None
        
        return [Instrument(instrument) for instrument in r.json()["instruments"]]

    def stream_prices(self, instruments: list[str]) -> Generator[dict, None, None]:
        """
        :param instruments: List of instruments to stream prices for
        :return: Generator yielding price data
        :rtype: Generator
        """
        endpoint = f"{self.stream_url}/v3/accounts/{self.id}/pricing/stream"
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {"instruments": ",".join(instruments)}

        with requests.get(
            endpoint, headers=headers, params=params, stream=True
        ) as r:
            for line in r.iter_lines():
                if line:
                    yield line


class OANDA:
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

    def get_accounts(self) -> list[Account]:
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
                # pprint.pprint(r.json())
                accounts.append(Account(r.json()["account"], self.token, self.url, self.stream_url))

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
        if self.last_refresh < (time.time() - __class__.REFRESH_INTERVAL):
            self.accounts = self.get_accounts()
            self.last_refresh = time.time()

        for account in self.accounts:
            if account.alias == alias:
                return account