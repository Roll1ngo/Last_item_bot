import asyncio
import os
import sys
import threading
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import requests

from data_base.models import AuthParameters
from functions.logger_config import logger, get_config
from data_base.connection import db



sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
env_path = Path(__file__).resolve().parent / '.env'

config = get_config()
USER_AGENT = config.get('user_agent')
load_dotenv()
YOUR_REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
USER_ID = os.getenv("USER_ID")
ACTIVE_DEVICE_TOKEN = os.getenv("ACTIVE_DEVICE_TOKEN")
LONG_LIVED_TOKEN = os.getenv("LONG_LIVED_TOKEN")
INTERVAL_SECONDS = 12 * 60

current_script_dir = Path(__file__).parent

class TokenManager:
    _instance = None
    _lock = threading.Lock() # Для потокобезпечного доступу до self.access_token
    _db_lock = threading.Lock() # Для потокобезпечного доступу до БД

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock: # Захищаємо створення Singleton
                if not cls._instance:
                    cls._instance = super(TokenManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.access_token: Optional[str] = None
            self.refresh_token: Optional[str] = None
            self.active_device_token: Optional[str] = None
            self.long_lived_token: Optional[str] = None
            self.user_id: Optional[str] = USER_ID
            self._initialized = True
            self._load_tokens_from_db_or_env() # Завантажуємо токени при ініціалізації
            self.token_ready_event = threading.Event()  # Подія для сигналізації готовності токена

    def _load_tokens_from_db_or_env(self):
        """Завантажує токени з БД або, якщо їх немає, з .env."""
        with self._db_lock:
            with db.get_session() as session:
                auth_params = session.query(AuthParameters).filter_by(id="main_auth_record").first()
                if auth_params:
                    self.access_token = auth_params.access_token
                    self.refresh_token = auth_params.refresh_token
                    self.active_device_token = auth_params.active_device_token
                    self.long_lived_token = auth_params.long_lived_token
                    logger.info("Токени успішно завантажено з бази даних.")
                else:
                    # Якщо в БД немає запису, використовуємо початкові значення з .env
                    self.refresh_token = YOUR_REFRESH_TOKEN
                    self.active_device_token = ACTIVE_DEVICE_TOKEN
                    self.long_lived_token = LONG_LIVED_TOKEN
                    logger.warning("Токени не знайдено в БД, використовуються значення з .env.")
                    # Створюємо початковий запис у БД
                    try:
                        new_auth_params = AuthParameters(
                            id="main_auth_record",
                            refresh_token=self.refresh_token,
                            active_device_token=self.active_device_token,
                            long_lived_token=self.long_lived_token
                        )
                        session.add(new_auth_params)
                        session.commit()
                        logger.info("Початкові токени з .env збережено в БД.")
                    except Exception as e:
                        session.rollback()
                        logger.error(f"Помилка при збереженні початкових токенів в БД: {e}")

    def _save_tokens_to_db(self):
        """Зберігає актуальні токени в базу даних."""
        with self._db_lock: # Захищаємо доступ до БД
            with db.get_session() as session:
                try:
                    auth_params = session.query(AuthParameters).filter_by(id="main_auth_record").first()
                    if auth_params:
                        auth_params.access_token = self.access_token
                        auth_params.refresh_token = self.refresh_token
                        auth_params.active_device_token = self.active_device_token
                        auth_params.long_lived_token = self.long_lived_token
                    else:
                        # Це не повинно статися після _load_tokens_from_db_or_env, але для безпеки
                        auth_params = AuthParameters(
                            id="main_auth_record",
                            access_token=self.access_token,
                            refresh_token=self.refresh_token,
                            active_device_token=self.active_device_token,
                            long_lived_token=self.long_lived_token
                        )
                        session.add(auth_params)
                    session.commit()
                    logger.info("Токени успішно збережено в базі даних.")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Помилка при збереженні токенів в БД: {e}")

    async def refresh_access_token(self):
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

        # Використовуємо актуальні refresh, active_device, long_lived токени з пам'яті
        payload = {
            "user_id": self.user_id,
            "refresh_token": self.refresh_token,
            "active_device_token": self.active_device_token,
            "long_lived_token": self.long_lived_token
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()

            data = response.json()
            payload_data = data.get("payload", {})

            # Оновлюємо всі токени в пам'яті
            with self._lock:
                self.access_token = payload_data.get("access_token")
                # Оновлюємо refresh, active_device, long_lived токени, якщо вони повернулися
                self.refresh_token = payload_data.get("refresh_token", self.refresh_token)
                self.active_device_token = payload_data.get("active_device_token", self.active_device_token)
                self.long_lived_token = payload_data.get("long_lived_token", self.long_lived_token)

            if self.access_token:
                if not self.token_ready_event.is_set():
                    self.token_ready_event.set()  # Встановлюємо подію, як тільки токен готовий
                logger.info("Access token успішно оновлено.")
                self._save_tokens_to_db() # Зберігаємо всі актуальні токени в БД
            else:
                logger.warning(f"Поле 'access_token' не знайдено у відповіді payload. Повна відповідь: {data}")
                with self._lock:
                    self.access_token = None # Встановлюємо None, якщо токен не отримано
        except requests.exceptions.RequestException as e:
            logger.error(f"Помилка під час оновлення токена: {e}")
            with self._lock:
                self.access_token = None # Встановлюємо None при помилці
        except Exception as e:
            logger.error(f"Неочікувана помилка в refresh_access_token: {e}", exc_info=True)
            with self._lock:
                self.access_token = None

        # Ця функція більше не повертає data, оскільки вона зберігається внутрішньо
        # return data # Видаліть цей рядок або змініть його, якщо вам потрібен повертаний результат

    def get_token(self) -> Optional[str]:
        with self._lock:
            return self.access_token


async def token_refresh_scheduler_direct(token_manager: TokenManager):
    await token_manager.refresh_access_token()

    while True:
        token = token_manager.get_token()
        if token:
            logger.info(f"Token is valid. Next refresh in {INTERVAL_SECONDS / 60} minutes.")
        else:
            logger.warning("Failed to get token. Retrying in 1 minute.")
            await asyncio.sleep(60)
            await token_manager.refresh_access_token()
            continue

        await asyncio.sleep(INTERVAL_SECONDS)


async def main():
    manager = TokenManager()

    # Створення фонової задачі для оновлення токена
    asyncio.create_task(token_refresh_scheduler_direct(manager))

    # Цей цикл підтримуватиме подієвий цикл активним,
    # дозволяючи фоновій задачі працювати.
    while True:
        await asyncio.sleep(10)  # Просто чекаємо, щоб не навантажувати CPU



if __name__ == "__main__":
    logger.warning("Запуск планувальника оновлення токена прямим запитом...")
    asyncio.run(main())