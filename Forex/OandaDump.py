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
from Stream import StreamClient

# Configure logging
logging.basicConfig(
    filename='OandaDump.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s',
    filemode='w'
)

logger = logging.getLogger(__name__)

practice = False
key = "oanda-api-key-practice" if practice else "oanda-api-key"
acc_id = "oanda-account-id-practice" if practice else "oanda-account-id"
API_KEY = get_credentials(get_project_id(), key, "latest")
ACCOUNT_ID = get_credentials(get_project_id(), acc_id, "latest")

OANDA_URL = "https://api-fxpractice.oanda.com/v3" if practice else "https://api-fxtrade.oanda.com/v3"

SECURE_HEADER = {
    'Authorization': f'Bearer {API_KEY}'
}

DAY_RANGES = {
    'S5': 6/24,
    'S10': 12/24,
    'S15': 18/24,
    'S30': 1.5,
    'M1': 3,
    'M2': 6,
    'M4': 13,
    'M5': 17,
    'M10': 34,
    'M15': 52,
    'M30': 104,
    'H1': 208,
    'H2': 416,
    'H3': 625,
    'H4': 833,
    'H6': 1250,
    'H8': 1666,
    'H12': 2500,
    'D': 5000,
    'W': 35000,
    'M': 152187
}

INSTRUMENT_TABLE_SCHEMA = [
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("displayName", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pipLocation", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("displayPrecision", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("tradeUnitsPrecision", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("minimumTradeSize", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("maximumTrailingStopDistance", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("minimumTrailingStopDistance", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("maximumPositionSize", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("maximumOrderUnits", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("marginRate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("guaranteedStopLossOrderMode", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("longRate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("shortRate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("financing_MONDAY", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("financing_TUESDAY", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("financing_WEDNESDAY", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("financing_THURSDAY", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("financing_FRIDAY", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("financing_SATURDAY", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("financing_SUNDAY", "INT64", mode="REQUIRED"),
]
'''
name                             USD_CNH
type                            CURRENCY
displayName                      USD/CNH
pipLocation                           -4
displayPrecision                       5
tradeUnitsPrecision                    0
minimumTradeSize                       1
maximumTrailingStopDistance      1.00000
minimumTrailingStopDistance      0.00050
maximumPositionSize                    0
maximumOrderUnits              100000000
marginRate                          0.05
guaranteedStopLossOrderMode     DISABLED
longRate                          0.0275
shortRate                        -0.0667
financing_MONDAY                       1
financing_TUESDAY                      1
financing_WEDNESDAY                    1
financing_THURSDAY                     1
financing_FRIDAY                       1
financing_SATURDAY                     0
financing_SUNDAY                       0
'''
TICK_DATA_SCHEMA = [
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("exchange_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("received_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("bid_px0", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_v0", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_px1", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_v1", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_px2", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_v2", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_px3", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_v3", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_px4", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("bid_v4", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_px0", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_v0", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_px1", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_v1", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_px2", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_v2", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_px3", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_v3", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_px4", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ask_v4", "INT64", mode="REQUIRED"),
]

class OandaDump():
    def __init__(self):
        self.session = requests.Session()
        self.bq = BQ(get_project_id(), logger=logger)
        self.timezone = pytz.timezone("UTC")
        self.pricing_df = pd.DataFrame()
        self.pricing_buffer = []
        
    def get_instrument_list_from_api(self):
        url=f"{OANDA_URL}/accounts/{ACCOUNT_ID}/instruments"
        self.session.headers.update(SECURE_HEADER)
        response = self.session.get(url, params=None,headers=SECURE_HEADER)
        if response.status_code != 200:
            raise ValueError(response.text)
        data = pd.DataFrame(response.json()['instruments'])
        data['longRate'] = data['financing'].apply(lambda x: x['longRate'])
        data['shortRate'] = data['financing'].apply(lambda x: x['shortRate'])
        data['financingDaysOfWeek'] = data['financing'].apply(lambda x: x['financingDaysOfWeek'])
        for day in ['MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY','SUNDAY']:
            data.loc[:, f"financing_{day}"] = data.loc[:, 'financingDaysOfWeek'].apply(lambda x: next((a['daysCharged'] for a in x if a['dayOfWeek'] == day), None))
        data.drop(columns=['financing','financingDaysOfWeek','tags'], inplace=True)
        return data
    
    def get_data_from_bq(self):
        dataset = "Oanda"
        table = "instruments"
        tables = self.bq.list_tables(dataset)
        if "instruments" in tables:
            logger.info("Table already exists")
            return self.bq.query_table(f"SELECT * FROM `{dataset}.{table}`")
        else:
            logger.info("Creating Table")
            data = self.get_instrument_list_from_api()
            self.bq.create_table(dataset,table,INSTRUMENT_TABLE_SCHEMA)
            self.bq.insert_dataframe(dataset,table,data)
            return data
    
    def check_granularity(self,granularity):
        if granularity not in DAY_RANGES.keys():
            raise ValueError("Invalid Granularity")
        
    def check_instrument(self,instruments):
        if not hasattr(self,'instrument_list'):
            self.instrument_list = self.get_data_from_bq()['name'].tolist()
        for instrument in instruments:
            if instrument not in self.instrument_list:
                raise ValueError(f"{instrument} not found in the instrument list")

    def input_checks(self, instrument, granularity):
        self.check_granularity(granularity)
        self.check_instrument(instrument)
        
    def get_data(self, instrument ,start, end, granularity):
        url = f"{OANDA_URL}/instruments/{instrument}/candles"
        params = {
            "granularity": granularity,
            "from": start,
            "to": end,
            "price": "MBA",
            "units": self.units
        }
        response = self.session.get(url, headers=SECURE_HEADER, params=params)
        data = response.json()
        return data
    
    def get_data_for_instruments(self, instruments, start, end, granularity):
        for instrument in instruments:
            self.input_checks(instruments, granularity)
        dfs = {}
        for instrument in instruments:
            segments = self.break_into_segments(start, end, granularity)
            full_df = pd.DataFrame()
            for segment in segments:
                data = self.get_data(instrument,segment[0],segment[1],granularity)
                df = self.get_candles_ohlc(data)
                if df is None:
                    continue
                full_df = pd.concat([full_df,df])
            dfs[instrument] = full_df
        return dfs
    
    def get_candles_ohlc(self, json_response):
        prices = ['mid','bid','ask']
        ohlc = ['o','h','l','c']
        our_data=[]
        if 'candles' not in json_response.keys():
            return None
        for candle in json_response['candles']:
            if candle['complete'] == False:
                continue
            new_dict={}
            new_dict['time']=candle['time']
            new_dict['volume']=candle['volume']
            for price in prices:
                for oh in ohlc:
                    new_dict[f"{price}_{oh}"]=candle[price][oh]
            our_data.append(new_dict)
        return pd.DataFrame.from_dict(our_data)
    
    def break_into_segments(self,start_date, end_date, granularity):
        start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
        
        max_duration = DAY_RANGES[granularity]
        
        segments = []
        while start < end:
            segment_end = min(start + timedelta(days=max_duration), end)
            segments.append((start.strftime("%Y-%m-%dT%H:%M:%SZ"), segment_end.strftime("%Y-%m-%dT%H:%M:%SZ")))
            start = segment_end
        return segments
    
    def DumpData(self, pairs, start, end, granularity, dump_path = None):
        if dump_path is None:
            dump_path = os.path.join(os.getcwd(),'Data')
        year_pairs = self.get_year_pairs(start,end)
        for pair in pairs:
            self.input_checks(pairs, granularity)
        for instrument in pairs:
            for pair in year_pairs:
                start = pair[0]
                end = pair[1]
                if not os.path.exists(os.path.join(dump_path,f"{instrument}_{granularity}_{start.split('-')[0]}.pkl")):
                    df_list = []
                    segments = self.break_into_segments(start, end, granularity)
                    for segment in segments:
                        data = self.get_data(instrument,segment[0],segment[1],granularity)
                        df = self.get_candles_ohlc(data)
                        df_list.append(df)
                    full_df = pd.concat(df_list)
                    if not full_df.empty:
                        full_df.reset_index(drop=True, inplace=True)
                        full_df['time'] = pd.to_datetime(full_df['time'])
                        full_df.set_index('time', inplace=True)
                        full_df.sort_index(inplace=True)
                        full_df.to_pickle(f"{self.path}{instrument}_{granularity}_{start.split('-')[0]}.pkl")
                        logger.info(f"Saved {instrument} for {start.split('-')[0]}")
                    else:
                        logger.info(f"Empty DataFrame for {instrument} for {start.split('-')[0]}")
            
    def get_year_pairs(self,start,end):
        years = range(start,end+1)
        year_pairs = [(f"{year}-01-01T00:00:00Z",f"{year+1}-01-01T00:00:00Z") for year in years]
        return year_pairs
    
    def get_data_from_dump(self, instrument, granularity, start, end):
        self.input_checks([instrument], granularity)
        start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")
        current_year = start.year
        dfs = []
        if current_year != end.year:
            df = pd.read_pickle(f"{self.path}{instrument}_{granularity}_{current_year}.pkl")
            dfs.append(df[(df['time'] >= start)])
            current_year += 1
        else:
            df = pd.read_pickle(f"{self.path}{instrument}_{granularity}_{current_year}.pkl")
            dfs.append(df[(df['time'] >= start) & (df['time'] <= end)])
        return pd.concat(dfs)
    
    def process_pricing_data(self, data):
        name = data['instrument']
        exchange_time = datetime.strptime(data['time'][:-4], "%Y-%m-%dT%H:%M:%S.%f")
        
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
    
    def call_back_for_dump(self, data, streamtype):
        if streamtype == "pricing":
            if data['type'] == "PRICE":
                self.pricing_buffer.append(data)
        elif streamtype == "transactions":
            pass
        else:
            logger.info('not supported stream type')
        current_size = len(self.pricing_buffer)
        if current_size >= self.max_df_size:
            self.add_to_table(current_size)

    def add_to_table(self, current_size):
        if "tick_data" not in self.bq.list_tables("Oanda"):
            logger.info("Creating Table")
            self.bq.create_table("Oanda","tick_data",TICK_DATA_SCHEMA)
        _ = [self.process_pricing_data(self.pricing_buffer.pop(0)) for _ in range(current_size)]
        try:
            self.bq.insert_dataframe("Oanda","tick_data",self.pricing_df)
        except:
            logger.error("Failed to insert data into table")
            raise Exception("Failed to insert data into table")
        self.pricing_df = pd.DataFrame()
        logger.info("Updated Table")
        
    async def start_stream_dump(self, instruments, max_df_size=1000):
        self.max_df_size = max_df_size
        self.stream = StreamClient(ACCOUNT_ID, API_KEY, logger=None, practice=practice)
        await self.stream.start_stream(instruments, self.call_back_for_dump)
        current_time = datetime.now(pytz.timezone('America/New_York'))
        days_to_close = 4 - current_time.weekday()
        end_time = current_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_to_close)
        sleep_time = (end_time - datetime.now(pytz.timezone('America/New_York'))).total_seconds()
        logger.info(f"Sleeping for {sleep_time} seconds")
        await asyncio.sleep(sleep_time)
        await self.stream.stop_stream()
    
if __name__ == "__main__":
    dump = OandaDump()
    instruments = dump.get_data_from_bq()['name'].tolist()[:10]
    asyncio.run(dump.start_stream_dump(instruments, 100))
    