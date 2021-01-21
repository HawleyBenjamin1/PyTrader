import json
import requests
import numpy as np
import pandas as pd
import math


class Trader:
    def __init__(self, config_file_name):
        with open(config_file_name) as f:
            self.CONFIG = json.load(f)
        self.HEADERS = {'APCA-API-KEY-ID': self.CONFIG["api_key_id"], "APCA-API-SECRET-KEY": self.CONFIG["secret_key"]}
        self.BASE_PAPER_URL = self.CONFIG["base_paper_url"]
        self.BASE_MKT_DATA_URL = self.CONFIG["base_mkt_data_url"]

    def get_account(self):
        return requests.get(self.CONFIG["base_paper_url"] + self.CONFIG["account_endpoint"], headers=self.HEADERS)

    def create_order(self, symbol, qty, side, order_type, time_in_force):
        DATA = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force
        }
        return requests.post(self.BASE_PAPER_URL + self.CONFIG["orders_endpoint"], json=DATA, headers=self.HEADERS)

    def get_positions(self):
        return requests.get(self.BASE_PAPER_URL + self.CONFIG["positions_endpoint"], headers=self.HEADERS)

    def get_assets(self):
        return requests.get(self.BASE_PAPER_URL + self.CONFIG["assets_endpoint"], headers=self.HEADERS)

    # No more than 200 symbols allowed
    # Limit: max number of bars for each symbol between 1 and 1000, default: 100
    # Time stamps in format: '2019-04-15T09:30:00-04:00'
    def get_mkt_data(self, symbols, limit, after, time_frame="5Min", until=None):
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

        return requests.get(self.BASE_MKT_DATA_URL + self.CONFIG["bars_endpoint"] + '/' + time_frame, params=DATA,
                            headers=self.HEADERS)


def main():
    trader = Trader("config.json")
    print(trader.get_account().text)


if __name__ == "__main__":
    main()
