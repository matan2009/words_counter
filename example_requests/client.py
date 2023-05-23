import asyncio
import aiohttp

text_input = "hi"
endpoint_url = 'http://localhost:8000/word_counter/'


async def send_input(session, text_input: str):
    async with session.post(endpoint_url + text_input) as response:
        await response.text()


async def main():
    async with aiohttp.ClientSession() as session:
        requests = [asyncio.create_task(send_input(session, text_input))]
        await asyncio.gather(*requests)


if __name__ == '__main__':
    asyncio.run(main())
