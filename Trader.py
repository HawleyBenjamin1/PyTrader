import json
import requests
import numpy as np
import pandas as pd
import math
from config import *

HEADERS = {'APCA-API-KEY-ID': api_key_id, "APCA-API-SECRET-KEY": secret_key}


def main():
    print(get_mkt_data("AAPL", 100, "2020-01-20T09:30:00-04:00"))
    # assets_dataframe = pd.read_json(get_assets())
    # assets_dataframe = assets_dataframe.sort_values(by=["exchange", "symbol"])
    #
    # assets_csv = assets_dataframe.to_csv()
    # f = open("pandas_test.csv", "w", encoding="utf-8", errors="ignore")
    # f.write(assets_csv)
    # f.close()


def get_account():
    acc_return = requests.get(base_paper_url + account_endpoint, headers=HEADERS)
    for i in acc_return:
        print(i)


def create_order(symbol, qty, side, order_type, time_in_force):
    DATA = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": order_type,
        "time_in_force": time_in_force
    }
    r = requests.post(base_paper_url + orders_endpoint, json=DATA, headers=HEADERS)
    return json.loads(r.content)


def get_positions():
    return requests.get(base_paper_url + positions_endpoint, headers=HEADERS).content


def get_assets():
    return requests.get(base_paper_url + assets_endpoint, headers=HEADERS).content


# No more than 200 symbols allowed
# Limit: max number of bars for each symbol between 1 and 1000, default: 100
# Time stamps in format: '2019-04-15T09:30:00-04:00'
def get_mkt_data(symbols, limit, after, time_frame="5Min", until=None):
    if until is None:
        DATA = {
            "symbols": symbols,
            "limit": limit,
            "after": after
        }
    else:
        DATA = {
            "symbols": symbols,
            "limit": limit,
            "after": after,
            "until": until
        }

    return requests.get(base_mkt_data_url + bars_endpoint + '/' + time_frame, params=DATA, headers=HEADERS).content


if __name__ == "__main__":
    main()
