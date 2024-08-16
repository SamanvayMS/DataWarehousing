import asyncio
import websockets
import json
import datetime as dt

class WebSocket:
    BASE_URL = "wss://stream.binance.us:9443/ws"

    def __init__(self, logger=None, reconnect_interval=5):
        self.ws = None
        self.reconnect_interval = reconnect_interval
        self.callback = None
        self.subscribed_streams = []  # Initialize subscribed streams
        self.logger = logger

    async def connect(self):
        self.ws = await websockets.connect(self.BASE_URL)
        if self.logger is not None:
            self.logger.info("Connected to Binance WebSocket API")

    async def subscribe(self, streams):
        subscription = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        await self.ws.send(json.dumps(subscription))
        response = await self.ws.recv()
        if self.logger is not None:
            self.logger.info("Subscription Response: %s", response)

    async def reconnect(self):
        await asyncio.sleep(self.reconnect_interval)
        await self.connect()
        # Re-subscribe to the streams after reconnecting
        await self.subscribe(self.subscribed_streams)
        await self.receive_messages()
        
    async def receive_messages(self):
        while True:
            try:
                message = await self.ws.recv()
                self.on_message(message)
            except websockets.ConnectionClosed:
                if self.logger is not None:
                    self.logger.info("Connection closed, attempting to reconnect")
                await self.reconnect()
                break

    def on_message(self, message):
        data = json.loads(message)
        if self.callback:
            self.callback(data)

    async def close(self):
        await self.ws.close()
        if self.logger is not None:
            self.logger.info("Connection closed")

    async def connect_user_stream(self, listen_key):
        self.ws = await websockets.connect(f"{self.BASE_URL}/{listen_key}")
        if self.logger is not None:
            self.logger.info("Connected to Binance user data stream")

    async def start_stream(self, streams, callback=None, listen_key=None):  
        self.subscribed_streams = list(streams)
        self.callback = callback

        if listen_key is not None:
            self.subscribed_streams.append(listen_key)
        await self.connect()
        await self.subscribe(self.subscribed_streams)
        if self.logger is not None:
            self.logger.info("Subscribed to streams: %s", self.subscribed_streams)
        self.receive_task = asyncio.create_task(self.receive_messages())
        if self.logger is not None:
            self.logger.info("Started receiving messages")
        return self.receive_task

    async def stop_stream(self):
        self.receive_task.cancel()
        await self.close()
        try:
            await self.receive_task
        except asyncio.CancelledError:
            if self.logger is not None:
                self.logger.info("Stopped receiving messages")

# Run the example
if __name__ == "__main__":
    def call_back_example(data):
        print(dt.datetime.now(), data)

    # Usage example for public stream
    async def main_public():
        binance_ws = WebSocket()

        # Define the streams to subscribe to
        streams = ["btcusdt@trade", "btcusdt@depth"]

        await binance_ws.start_stream(streams, call_back_example)
        
        await asyncio.sleep(600)
        
        await binance_ws.stop_stream()

    asyncio.run(main_public())