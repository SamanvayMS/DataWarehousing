import aiohttp
import asyncio
import json
import datetime as dt
from gcloud import get_project_id, get_credentials
import sys

class StreamClient:
    LIVE_URL = "https://stream-fxtrade.oanda.com/v3/accounts"
    PRACTICE_URL = "https://stream-fxpractice.oanda.com/v3/accounts"

    def __init__(self, account_id: str, access_token: str, logger: object = None, reconnect_interval: int = 5, practice: bool = True):
        self.BASE_URL = self.PRACTICE_URL if practice else self.LIVE_URL
        self.reconnect_interval = reconnect_interval
        self.callback = None
        self.account_id = account_id
        self.access_token = access_token
        self.logger = logger
        self.subscribed_instruments = []
        self.pricing_task = None
        
    async def connect(self, stream_type: str):
        self.session = aiohttp.ClientSession() if not hasattr(self, "session") else self.session
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        if stream_type == "pricing":
            self.stream_url = f"{self.BASE_URL}/{self.account_id}/pricing/stream?instruments={'%2C'.join(self.subscribed_instruments)}"
        elif stream_type == "transactions":
            self.stream_url = f"{self.BASE_URL}/{self.account_id}/transactions/stream"
        
        self.response = await self.session.get(self.stream_url, headers=headers)
        if self.response.status == 200:
            if self.logger: self.logger.info(f"Connected to OANDA {stream_type} HTTPS stream API")
        else:
            if self.logger: self.logger.error(f"Failed to connect to OANDA {stream_type} HTTPS stream API: {self.response.status}")
            raise Exception(f"Failed to connect to OANDA {stream_type} HTTPS stream API: {self.response.status}")

    async def receive_messages(self, stream_type):
        async for line in self.response.content:
            try:
                message = line.decode('utf-8').strip()
                if message: 
                    await self.on_message(message, stream_type)
            except Exception as e:
                if self.logger: self.logger.error(f"Error receiving {stream_type} message: {e}")
                await self.reconnect(stream_type)
                break

    async def reconnect(self, stream_type):
        await asyncio.sleep(self.reconnect_interval)
        await self.connect(stream_type)
        if self.response and self.response.status == 200:
            if stream_type == "pricing":
                self.pricing_task = asyncio.create_task(self.receive_messages("pricing"))
            elif stream_type == "transactions":
                self.transactions_task = asyncio.create_task(self.receive_messages("transactions"))
            else:
                if self.logger: self.logger.error(f"Unknown stream type: {stream_type}")
            if self.logger: self.logger.info(f"Reconnected to {stream_type} stream")
        else:
            if self.logger: self.logger.error(f"Failed to reconnect to {stream_type} stream.")

    async def on_message(self, message, stream_type):
        data = json.loads(message)
        if self.callback:
            self.callback(data, stream_type)

    async def close(self):
        await self.session.close()
        if self.logger: self.logger.info("Connection closed")

    async def start_stream(self, instruments, callback=None):
        self.subscribed_instruments = instruments
        self.callback = callback
        if self.subscribed_instruments != []:
            await self.connect("pricing")
            if self.response.status == 200:
                self.pricing_task = asyncio.create_task(self.receive_messages("pricing"))
                if self.logger: self.logger.info("Started receiving pricing messages")
            else:
                if self.logger: self.logger.error("Failed to start pricing stream due to connection error")

        await self.connect("transactions")
        if self.response.status == 200:
            self.transactions_task = asyncio.create_task(self.receive_messages("transactions"))
            if self.logger: self.logger.info("Started receiving transactions messages")
        else:
            if self.logger: self.logger.error("Failed to start transactions stream due to connection error")

    async def stop_stream(self):
        if self.pricing_task is not None:
            self.pricing_task.cancel()
        self.transactions_task.cancel()
        await self.close()
        if self.pricing_task is not None:
            try:
                await self.pricing_task
            except asyncio.CancelledError:
                if self.logger: self.logger.info("Stopped receiving pricing messages")
        try:
            await self.transactions_task
        except asyncio.CancelledError:
            if self.logger: self.logger.info("Stopped receiving transactions messages")

# Run the example
if __name__ == "__main__":
    
    def call_back_example(data, streamtype):
        if streamtype == "pricing":
            print(dt.datetime.now(), data)
        elif streamtype == "transactions":
            print(dt.datetime.now(), data)
        else:
            print('not supported stream type')
        return "hahaha"

    # Usage example for public stream
    async def main_public():
        import logging
        import sys
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        logger = logging.getLogger(__name__)
        
        access_token = get_credentials(get_project_id(), "oanda-api-key-practice", "latest")
        account_id = get_credentials(get_project_id(), "oanda-account-id-practice", "latest")

        oanda_stream = StreamClient(account_id, access_token, logger, practice = True)

        # Define the instruments to subscribe to
        instruments = ["EUR_USD", "USD_CAD", "USD_JPY"]

        await oanda_stream.start_stream(instruments, call_back_example)
        
        await asyncio.sleep(20)
        
        await oanda_stream.stop_stream()

    asyncio.run(main_public())


