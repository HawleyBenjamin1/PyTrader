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
        self.PAPER_HEADERS = {'APCA-API-KEY-ID': self.CONFIG["paper_api_key_id"], "APCA-API-SECRET-KEY": self.CONFIG["paper_secret_key"]}
        self.BASE_PAPER_URL = self.CONFIG["base_paper_url"]
        self.BASE_URL = self.CONFIG["base_url"]
        self.BASE_MKT_DATA_URL = self.CONFIG["base_mkt_data_url"]

    def get_account(self):
        return requests.get(self.BASE_URL + self.CONFIG["account_endpoint"], headers=self.PAPER_HEADERS)

    def create_order(self, symbol, qty, side, order_type, time_in_force):
        DATA = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force
        }
        return requests.post(self.BASE_PAPER_URL + self.CONFIG["orders_endpoint"], json=DATA, headers=self.PAPER_HEADERS)

    def get_positions(self):
        return requests.get(self.BASE_PAPER_URL + self.CONFIG["positions_endpoint"], headers=self.PAPER_HEADERS)

    def get_assets(self):
        return requests.get(self.BASE_PAPER_URL + self.CONFIG["assets_endpoint"], headers=self.PAPER_HEADERS)

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


def get_bar_data(tickers, max_bars, after_date="2019-01-01"):
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

        df.to_sql(ticker, schema="bar_data", con=conn, if_exists="replace")


def update_positions():
    trader = Trader("config.json")
    config = json.load(open("config.json"))
    df = pd.read_json(trader.get_positions().text)

    conn = sqlalchemy.create_engine(config["db_connection_string"])
    df.to_sql("open_positions", schema="portfolio", con=conn, if_exists="replace")
    return df


def sellable(unrealized_plpc):
    if unrealized_plpc > .05 or unrealized_plpc < -.05:
        return True
    else:
        return False


# TODO: Create trading strategy
def main():
    config = json.load(open("config.json"))
    positions = update_positions()
    # get_bar_data(tickers=["TWTR"], max_bars=1000)

    trader = Trader("config.json")
    account_dict = trader.get_account().json()

    conn = sqlalchemy.create_engine(config["db_connection_string"])
    twtr = pd.read_sql_table(con=conn, table_name="TWTR", schema="bar_data", index_col="index")

    # TODO: Check for positions to sell
    sell_positions = positions.apply(lambda x:
                                     [x["symbol"], x["qty"], "sell"]
                                     if sellable(x["unrealized_plpc"]) else [x["symbol"], x["qty"], "hold"], axis=1)

    order_response = sell_positions.apply(lambda x:
                                          trader.create_order(x[0], x[1], "sell", "mkt", "gtc").text
                                          if x[2] is "sell" else x[0] + ": hold")
    print(order_response)

    # TODO: Check for positions to buy


if __name__ == "__main__":
    main()
