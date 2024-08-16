from gcloud import get_project_id, get_credentials
import asyncio
import aiohttp

practice = False
key = "oanda-api-key-practice" if practice else "oanda-api-key"
acc_id = "oanda-account-id-practice" if practice else "oanda-account-id"
API_KEY = get_credentials(get_project_id(), key, "latest")
ACCOUNT_ID = get_credentials(get_project_id(), acc_id, "latest")

OANDA_URL = "https://api-fxpractice.oanda.com/v3" if practice else "https://api-fxtrade.oanda.com/v3"

SECURE_HEADER = {
    'Authorization': f'Bearer {API_KEY}'
}
class test:
    async def start_stream(self):
        session = aiohttp.ClientSession()
        session.headers.update(SECURE_HEADER)
        link = f"https://stream-fxtrade.oanda.com/v3/accounts/{ACCOUNT_ID}/pricing/stream?instruments=EUR_USD%2CUSD_CAD"
        self.response = await session.get(link, headers = SECURE_HEADER)

    async def print_response(self):
        for line in self.response:
            print(line.decode('utf-8').strip())
        
async def main():
    t = test()
    await t.start_stream()
    await t.print_response()
if __name__ == "__main__":
    asyncio.run(main())