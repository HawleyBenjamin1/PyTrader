import datetime
import json
import requests
import numpy as np
import pandas as pd
import math
import sqlalchemy


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


def get_bar_data(tickers, max_bars, after_date):
    trader = Trader("config.json")
    config = json.load(open("config.json"))

    for ticker in tickers:
        bars = trader.get_mkt_data(ticker, max_bars, after_date, '1D')

        df = pd.DataFrame.from_dict(bars.json()[ticker])
        df.columns = ['epoch', 'open', 'high', 'low', 'close', 'vol']
        df['epoch'] = df['epoch'].apply(lambda t: datetime.datetime.fromtimestamp(t))
        df['avg'] = round((df['high'] + df['low']) / 2, 2)
        df['vol_diff'] = df['vol'].diff()

        conn = sqlalchemy.create_engine(config["db_connection_string"])

        df.to_sql(ticker, schema="bar_data", con=conn)


def update_positions():
    trader = Trader("config.json")
    config = json.load(open("config.json"))
    df = pd.read_json(trader.get_positions().text)

    conn = sqlalchemy.create_engine(config["db_connection_string"])
    df.to_sql("open_positions", schema="portfolio", con=conn)


# TODO: Access Twitter API
def main():
    update_positions()


if __name__ == "__main__":
    main()
