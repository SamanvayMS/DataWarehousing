import requests
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sys
import asyncio
import pytz
import logging
import time


sys.path.append("/home/samanvayms/DataWarehousing")
from BQUtils import *
from gcloud import get_project_id, get_credentials
from Stream import WebSocket

# Configure logging
logging.basicConfig(
    filename='BinanceDump.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Adding a StreamHandler to flush logs to the console immediately
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
console.setFormatter(formatter)

BASE_URL = "https://api.binance.us"

INSTRUMENT_TABLE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("baseAsset", "STRING"),
    bigquery.SchemaField("baseAssetPrecision", "INT64"),
    bigquery.SchemaField("quoteAsset", "STRING"),
    bigquery.SchemaField("quotePrecision", "INT64"),
    bigquery.SchemaField("quoteAssetPrecision", "INT64"),
    bigquery.SchemaField("baseCommissionPrecision", "INT64"),
    bigquery.SchemaField("quoteCommissionPrecision", "INT64"),
    bigquery.SchemaField("orderTypes", "STRING"),
    bigquery.SchemaField("icebergAllowed", "BOOLEAN"),
    bigquery.SchemaField("ocoAllowed", "BOOLEAN"),
    bigquery.SchemaField("quoteOrderQtyMarketAllowed", "BOOLEAN"),
    bigquery.SchemaField("allowTrailingStop", "BOOLEAN"),
    bigquery.SchemaField("cancelReplaceAllowed", "BOOLEAN"),
    bigquery.SchemaField("isSpotTradingAllowed", "BOOLEAN"),
    bigquery.SchemaField("isMarginTradingAllowed", "BOOLEAN"),
    bigquery.SchemaField("filters", "STRING"),
    bigquery.SchemaField("permissions", "STRING"),
    bigquery.SchemaField("defaultSelfTradePreventionMode", "STRING"),
    bigquery.SchemaField("allowedSelfTradePreventionModes", "STRING"),
]

DEPTH_DATA_SCHEMA = [
]

TRADE_DATA_SCHEMA = [
]

class BinanceDump():
    def __init__(self):
        self.websocket = WebSocket(logger=logging)
        self.bq = BQ(get_project_id())
        self.timezone = pytz.timezone("UTC")
        self.pricing_df = pd.DataFrame()
        # self.build_orderbook_df()
        
    def get_instrument_list_from_api(self):
        endpoint = "/api/v3/exchangeInfo"
        response = requests.get(BASE_URL + endpoint).json()['symbols']
        data = pd.DataFrame(response)
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].astype(str)
        print(data.info())

    def get_data_from_bq(self):
        dataset = "Binance"
        table = "instruments"
        tables = self.bq.list_tables(dataset)
        if "instruments" in tables:
            logging.info("Table already exists")
            return self.bq.query_table(f"SELECT * FROM `{dataset}.{table}`")
        else:
            logging.info("Creating Table")
            data = self.get_instrument_list_from_api()
            self.bq.create_table(dataset,table,INSTRUMENT_TABLE_SCHEMA)
            self.bq.insert_dataframe(dataset,table,data)
            return data
        
    def check_instrument(self,instruments):
        if not hasattr(self,'instrument_list'):
            self.instrument_list = self.get_data_from_bq()['name'].tolist()
        for instrument in instruments:
            if instrument not in self.instrument_list:
                raise ValueError(f"{instrument} not found in the instrument list")

    def _get_orderbook_snapshot(self, symbol: str) -> dict:
        endpoint = "https://api/v3/depth"
        params = {
            'symbol': symbol,
            'limit': 100
        }
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def _build_orderbook(self, symbol: str) -> None:
        snapshot = self._get_orderbook_snapshot(symbol)
        self.instruments[symbol].orderbook = {
            'engine_timestamp': snapshot['lastUpdateId'],
            'recieve_timestamp': datetime.now(pytz.utc),
            'bids': {float(price): float(quantity) for price, quantity in snapshot['bids']},
            'asks': {float(price): float(quantity) for price, quantity in snapshot['asks']}
        }
        self.logger.info(f"Snapshot for {symbol} retrieved and processed.")
        self._process_buffered_events(symbol)

    def _process_depth_data(self, data: dict) -> None:
        symbol = data['s'].lower()
        self.instruments[symbol].orderbook['last_update_id'] = data['u']
        for bid in data['b']:
            price = float(bid[0])
            quantity = float(bid[1])
            if quantity == 0:
                if price in self.instruments[symbol].orderbook['bids']:
                    del self.instruments[symbol].orderbook['bids'][price]
            else:
                self.instruments[symbol].orderbook['bids'][price] = quantity

        for ask in data['a']:
            price = float(ask[0])
            quantity = float(ask[1])
            if quantity == 0:
                if price in self.instruments[symbol].orderbook['asks']:
                    del self.instruments[symbol].orderbook['asks'][price]
            else:
                self.instruments[symbol].orderbook['asks'][price] = quantity

    def _process_depth(self, data: dict) -> None:
        symbol = data['s'].lower()
        orderbook = self.instruments[symbol].orderbook
        if 'last_update_id' not in orderbook:
            self.event_buffers[symbol].append(data)
        else:
            if data['u'] <= orderbook['last_update_id']:
                return
            if data['U'] == orderbook['last_update_id'] + 1:
                self._process_depth_data(data)
            else:
                self.event_buffers[symbol].append(data)
                self._process_buffered_events(symbol)
                
    def _process_buffered_events(self, symbol: str) -> None:
        buffer = self.event_buffers[symbol]
        orderbook = self.instruments[symbol].orderbook
        while buffer:
            event = buffer.popleft()
            if event['U'] <= orderbook['last_update_id'] + 1 <= event['u']:
                self._process_depth_data(event)
            elif event['u'] < orderbook['last_update_id']:
                continue
            else:
                buffer.appendleft(event)
                break

    def process_pricing_data(self, data):
        symbol = data['s']
        self.update_orderbook(symbol, data)
        
        # Initialize lists for bid and ask prices and volumes
        bid_prices = []
        bid_volumes = []
        ask_prices = []
        ask_volumes = []

        # Get the best 5 bids
        for i in range(5):
            if i < len(data['bids']):
                bid_prices.append(float(data['bids'][i]['price']))
                bid_volumes.append(int(data['bids'][i]['liquidity']))
            else:
                bid_prices.append(np.nan)
                bid_volumes.append(np.nan)
        
        # Get the best 5 asks
        for i in range(5):
            if i < len(data['asks']):
                ask_prices.append(float(data['asks'][i]['price']))
                ask_volumes.append(int(data['asks'][i]['liquidity']))
            else:
                ask_prices.append(np.nan)
                ask_volumes.append(np.nan)
                
        # Create a DataFrame
        df = pd.DataFrame({
            'name': [name],
            'exchange_time': [exchange_time],
            'received_time': [datetime.now(self.timezone)],
            'bid_px0': [bid_prices[0]], 'bid_v0': [bid_volumes[0]],
            'bid_px1': [bid_prices[1]], 'bid_v1': [bid_volumes[1]],
            'bid_px2': [bid_prices[2]], 'bid_v2': [bid_volumes[2]],
            'bid_px3': [bid_prices[3]], 'bid_v3': [bid_volumes[3]],
            'bid_px4': [bid_prices[4]], 'bid_v4': [bid_volumes[4]],
            'ask_px0': [ask_prices[0]], 'ask_v0': [ask_volumes[0]],
            'ask_px1': [ask_prices[1]], 'ask_v1': [ask_volumes[1]],
            'ask_px2': [ask_prices[2]], 'ask_v2': [ask_volumes[2]],
            'ask_px3': [ask_prices[3]], 'ask_v3': [ask_volumes[3]],
            'ask_px4': [ask_prices[4]], 'ask_v4': [ask_volumes[4]]
        })
        self.pricing_df = pd.concat([self.pricing_df, df])
        logging.info(f"Received data for {name}")
    
    def call_back_for_dump(self, data, streamtype):
        if streamtype == "pricing":
            if data['type'] == "PRICE":
                print(data)
                self.process_pricing_data(data)
        elif streamtype == "transactions":
            pass
        else:
            logging.info('not supported stream type')
        if len(self.pricing_df) > 1000:
            self.add_to_table()

    def add_to_table(self):
        if "depth_data" not in self.bq.list_tables("Binance"):
            logging.info("Creating Table")
            self.bq.create_table("Binance","depth_data",DEPTH_DATA_SCHEMA)
        if "trade_data" not in self.bq.list_tables("Binance"):
            logging.info("Creating Table")
            self.bq.create_table("Binance","trade_data",TRADE_DATA_SCHEMA)
        self.bq.insert_dataframe("Binance","depth_data",self.depth_df)
        self.bq.insert_dataframe("Binance","trade_data",self.trade_df)
        self.build_depth_df()
        logging.info("Updated Table")
        
    def build_streams(self,instruments):
        if not hasattr(self,'instrument_list'):
            self.instrument_list = self.get_data_from_bq()['name'].tolist()
        streams = []
        for instrument in instruments:
            if instrument not in self.instrument_list:
                raise ValueError(f"{instrument} not found in the instrument list")
            else:
                streams.append(f"{instrument.lower()}@depth")
                streams.append(f"{instrument.lower()}@trade")
        return streams
                
    async def start_stream_dump(self, instruments, end_time = None, days = 7):
        streams = self.build_streams(instruments)
        await self.websocket.start_stream(streams, self.call_back_for_dump)
        current_time = datetime.now(pytz.utc)
        end_time = end_time.replace(tzinfo=pytz.utc) if end_time else current_time + timedelta(days=days)
        sleep_time = (end_time - current_time).total_seconds()
        logging.info(f"Sleeping for {sleep_time} seconds")
        await asyncio.sleep(sleep_time)
        await self.websocket.stop_stream()
    
if __name__ == "__main__":
    dump = BinanceDump()
    dump.get_instrument_list_from_api()
    # instruments = ['BTCUSDT']
    # try:
    #     asyncio.run(dump.start_stream_dump(instruments, days=1))
    # except KeyboardInterrupt:
    #     dump.websocket.stop_stream()
    #     sys.exit(0)