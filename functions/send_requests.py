import asyncio
import json
import os

import aiohttp

from functions.logger_config import logger
from functions.load_config import get_config
from dotenv import load_dotenv

load_dotenv()
g2g_seller_id = os.getenv("USER_ID")

config = get_config()
USER_AGENT = config.get("USER_AGENT")

async def fetch_from_api_with_retry(session,
                                    url,
                                    payload = None,
                                    custom_headers = None,
                                    http_method = "GET",
                                    authorization = True,
                                    replace_all_headers = False,
                                    max_retries=5,
                                    delay = 5):

    # timeout = aiohttp.ClientTimeout(total=10)  # Таймаут для всіх операцій сесії
    # async with aiohttp.ClientSession(timeout=timeout) as session:
    base_headers = my_headers(custom_headers, authorization)
    if replace_all_headers:
        base_headers = custom_headers
    # logger.info(f"url: {url}, headers: {base_headers}, payload: {payload}, http_method: {http_method}")


    for attempt in range(max_retries):
        try:
            async with session.request(
                    http_method,  # Використовуємо вказаний метод (PUT/GET/POST)
                    url,
                    headers=base_headers,
                    data=payload if http_method in ["POST", "PUT"] else None  # data замість json для PUT
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    await asyncio.sleep(0.2)
                    return result, response.status
                logger.error(f"Спроба {attempt + 1}: Помилка {response.status} - {await response.text()}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Спроба {attempt + 1}: Помилка - {e}")
        except json.JSONDecodeError:
            logger.warning(f"Не вдалося декодувати відповідь JSON. Відповідь: {response.text}")
        await asyncio.sleep(delay)
    return None


def my_headers(custom_headers = None,
               authorization = True):

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,uk;q=0.6",
        "authorization": os.getenv("AUTH_TOKEN"),
        "content-type": "application/json",
        "origin": "https://www.g2g.com",
        "priority": "u=1, i",
        "referer": "https://www.g2g.com/",
        "sec-ch-ua-platform": "\"Linux\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": USER_AGENT,
    }

    if custom_headers:
        headers.update(custom_headers)
    if not authorization:
        headers.pop("authorization")


    return headers

if __name__ == "__main__":
    url_test = f'https://sls.g2g.com/order/count-my-orders?seller_id={g2g_seller_id}'
    asyncio.run(fetch_from_api_with_retry(url_test))