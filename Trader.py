import json

import requests
from config import *

HEADERS={'APCA-API-KEY-ID': api_key_id, "APCA-API-SECRET-KEY": secret_key}

def main():
    r = create_order("AAPL", 1, "buy", "market", "gtc")
    print(r)

def get_account():
    acc_return = requests.get(base_url + account_endpoint, HEADERS)
    for i in acc_return:
        print(i)

def create_order(symbol, qty, side, type, time_in_force):
    DATA = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": type,
        "time_in_force": time_in_force
    }
    r = requests.post(base_url + orders_endpoint, json=DATA, headers=HEADERS)
    return json.loads(r.content)

if __name__ == "__main__":
    main()
