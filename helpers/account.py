import requests

def gather_account_id(alias, token, live=False):
    if live:
        url = 'https://api-fxtrade.oanda.com'

    else:
        url = 'https://api-fxpractice.oanda.com'

    endpoint = f'{url}/v3/accounts'
    headers = {'Authorization': f'Bearer {token}'}

    r = requests.get(endpoint, headers=headers)

    if 'errorMessage' in r.json():
        return False

    for account in r.json()['accounts']:
        r = requests.get(f'{endpoint}/{account["id"]}', headers=headers)

        try:
            if r.json()['account']['alias'] != alias:
                continue

            else:
                return r.json()["account"]["id"]

        except:
            print(r.json())

    return False

def gather_acct_instruments(id, token, live=False):
    if live:
        url = 'https://api-fxtrade.oanda.com'

    else:
        url = 'https://api-fxpractice.oanda.com'

    endpoint = f'{url}/v3/accounts/{id}/instruments'
    headers = {'Authorization': f'Bearer {token}'}

    r = requests.get(endpoint, headers=headers)

    if 'errorMessage' in r.json():
        return False

    return [instrument['name'] for instrument in r.json()['instruments']]
