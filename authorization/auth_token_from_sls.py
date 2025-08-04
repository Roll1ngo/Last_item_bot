import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Union, Dict, Any

from dotenv import load_dotenv
import requests
import json

from functions.logger_config import logger

from functions.load_config import get_config

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
env_path = Path(__file__).resolve().parent / '.env'

config = get_config()
USER_AGENT = config.get('user_agent')

load_dotenv()
YOUR_REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
USER_ID = os.getenv("USER_ID")
ACTIVE_DEVICE_TOKEN = os.getenv("ACTIVE_DEVICE_TOKEN")
LONG_LIVED_TOKEN = os.getenv("LONG_LIVED_TOKEN")

# --- Конфігурація ---

# Інтервал оновлення (раз на 5 хвилин)
INTERVAL_SECONDS = 12 * 60
current_script_dir = Path(__file__).parent

# Глобальна змінна для зберігання актуального access token
current_access_token = None


async def get_new_access_token_via_refresh():
    """
    Виконує прямий HTTP-запит для оновлення access token за допомогою refresh token.
    Деталі запиту (метод, заголовки, тіло) відповідають перехопленому трафіку.
    """
    global current_access_token
    logger.warning("Спроба оновити access token за допомогою refresh token...")

    url = "https://sls.g2g.com/user/refresh_access"

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,uk;q=0.6",
        "content-type": "application/json",
        "origin": "https://www.g2g.com",
        "referer": "https://www.g2g.com/",
        "user-agent": USER_AGENT,
    }

    payload = {
        "user_id": USER_ID,
        "refresh_token": YOUR_REFRESH_TOKEN,
        "active_device_token": ACTIVE_DEVICE_TOKEN,
        "long_lived_token": LONG_LIVED_TOKEN
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Викличе виняток для статусів 4xx/5xx

        data = response.json()

        # Access token знаходиться у data["payload"]["access_token"]
        if "payload" in data and "access_token" in data["payload"]:
            current_access_token = data["payload"]["access_token"]
            logger.info(f"Access token успішно оновлено. Нова access token: {current_access_token[-6:-1]}")
        else:
            logger.warning(f"Поле 'access_token' не знайдено у відповіді payload. Повна відповідь: {data}")
            current_access_token = None

    except requests.exceptions.HTTPError as http_err:
        logger.warning(f"HTTP помилка при оновленні токена: {http_err}")
        logger.warning(f"Відповідь сервера: {http_err.response.text}")
        current_access_token = None
    except requests.exceptions.ConnectionError as conn_err:
        logger.warning(f"Помилка підключення при оновленні токена: {conn_err}")
        current_access_token = None
    except requests.exceptions.Timeout as timeout_err:
        logger.warning(f"Таймаут при оновленні токена: {timeout_err}")
        current_access_token = None
    except requests.exceptions.RequestException as req_err:
        logger.warning(f"Загальна помилка запиту при оновленні токена: {req_err}")
        current_access_token = None
    except json.JSONDecodeError:
        logger.warning(f"Не вдалося декодувати відповідь JSON. Відповідь: {response.text}")
        current_access_token = None

    return data


async def token_refresh_scheduler_direct():
    """
    Планувальник, який запускає оновлення токена прямим запитом кожні 5 хвилин.
    """
    global current_access_token  # Додано для доступу до глобальної змінної


    while True:

        response =await get_new_access_token_via_refresh()

        if current_access_token:
            logger.info(f"Access token актуальний. Наступне оновлення через {INTERVAL_SECONDS / 60} хвилин.")

        else:
            logger.warning(f"Не вдалося оновити access token. Спроба повториться через {INTERVAL_SECONDS / 60} хвилин.")

        record_auth_token_to_env(response['payload'])
        await asyncio.sleep(INTERVAL_SECONDS)


def record_auth_token_to_env(data: Union[Dict[str, Any], str]) -> bool:
    """
    Оновлює токени у .env файлі, зберігаючи SESSION_KEY та TELEGRAM_BOT_TOKEN незмінними.

    Args:
        data: Може бути як словником, так і JSON-рядком:
            - Словник у форматі {"access_token": "...", ...}
            - JSON-рядок у такому ж форматі

    Returns:
        bool: True при успішному оновленні, False при помилці
    """
    try:
        # Якщо data - рядок, спробуємо розпарсити JSON
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                # Можливо, це вже готовий токен без JSON
                data = {"access_token": data}

        # Якщо data - не словник, створюємо базовий словник
        if not isinstance(data, dict):
            data = {"access_token": str(data)}

        # Витягуємо токени з вхідних даних
        tokens = {
            'AUTH_TOKEN': data.get("access_token"),
            'REFRESH_TOKEN': f'"{data.get("refresh_token")}"' if data.get("refresh_token") else None,
            'ACTIVE_DEVICE_TOKEN': f'"{data.get("active_device_token")}"' if data.get("active_device_token") else None,
            'LONG_LIVED_TOKEN': f'"{data.get("long_lived_token")}"' if data.get("long_lived_token") else None
        }

        # Перевіряємо наявність обов'язкових токенів
        if not tokens['AUTH_TOKEN']:
            logger.error("Відсутній обов'язковий access_token")
            return False

        # Читаємо поточний вміст .env файлу
        try:
            with open(env_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            content = ""
            logger.info("Файл .env не знайдено, буде створено новий")

        # Зберігаємо важливі змінні, які не повинні змінюватися
        preserved_vars = {
            'SESSION_KEY': re.search(r'^SESSION_KEY=(.*)$', content, re.MULTILINE),
            'TELEGRAM_BOT_TOKEN': re.search(r'^TELEGRAM_BOT_TOKEN=(.*)$', content, re.MULTILINE)
        }

        # Оновлюємо кожен токен у вмісті файлу
        updated_content = content
        for key, value in tokens.items():
            if not value:  # Пропускаємо пусті значення
                continue

            updated_content = re.sub(
                rf'^{key}=.*$',
                f'{key}={value}',
                updated_content,
                flags=re.MULTILINE
            )

            # Якщо змінна не знайдена, додаємо її
            if f'{key}={value}' not in updated_content:
                if updated_content and not updated_content.endswith('\n'):
                    updated_content += '\n'
                updated_content += f'{key}={value}\n'

        # Відновлюємо оригінальні значення важливих змінних
        for var_name, match in preserved_vars.items():
            if match:
                original_value = match.group(1)
                updated_content = re.sub(
                    rf'^{var_name}=.*$',
                    f'{var_name}={original_value}',
                    updated_content,
                    flags=re.MULTILINE
                )
                # Якщо змінна не знайдена, додаємо її
                if f'{var_name}={original_value}' not in updated_content:
                    if updated_content and not updated_content.endswith('\n'):
                        updated_content += '\n'
                    updated_content += f'{var_name}={original_value}\n'

        # Записуємо оновлений вміст
        with open(env_path, 'w') as file:
            file.write(updated_content)

        return True

    except Exception as e:
        logger.error(f"Неочікувана помилка при оновленні .env: {str(e)}", exc_info=True)
        return False


if __name__ == "__main__":
    logger.warning("Запуск планувальника оновлення токена прямим запитом...")
    asyncio.run(token_refresh_scheduler_direct())

