import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from functions.load_config import get_config
from functions.logger_config import logger
from dotenv import load_dotenv
from data_base.base import Base

env_path = Path(__file__).parent.parent / "authorization" / ".env"
load_dotenv(dotenv_path=env_path) # Завантажуємо змінні середовища з .env файлу

config = get_config()

DATABASE_URL = os.getenv("DATABASE_URL")

class Database:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self, database_url: str = DATABASE_URL): # <-- Встановлюємо DATABASE_URL як дефолтний аргумент
        if hasattr(self, '_initialized') and self._initialized:
            return

        self.database_url = database_url

        # Перевірка, чи database_url не None
        if self.database_url is None:
            raise ValueError("DATABASE_URL не встановлено. Будь ласка, встановіть змінну середовища DATABASE_URL або передайте її в конструктор Database.")

        try:
            self.engine = create_engine(
                self.database_url,
                future=True,
                echo=False
            )

            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine,
                class_=Session
            )
            self._initialized = True
            logger.info(f"Підключення до бази даних за URL '{self.database_url}' успішно налаштовано.")

        except Exception as e:
            logger.error(f"Помилка під час ініціалізації підключення до бази даних за URL '{self.database_url}': {e}")
            raise

    def get_session(self) -> Session:
        return self.SessionLocal()

    def create_all_tables(self):
        try:
            # Важливо: Base.metadata повинна "знати" про всі твої моделі,
            # які ти хочеш створити. Переконайся, що твої моделі імпортовані
            # або визначені до виклику create_all_tables.
            Base.metadata.create_all(self.engine)
            logger.info(f"Таблиці бази даних за URL '{self.database_url}' перевірено/створено.")
        except Exception as e:
            logger.error(f"Помилка при створенні таблиць бази даних за URL '{self.database_url}': {e}")
            raise

# Створюємо єдиний екземпляр класу Database
# Він тепер автоматично використає DATABASE_URL, сформований вище
db = Database()

if __name__ == '__main__':
    # Для тестування, переконайтеся, що змінні середовища встановлені
    # або що .env файл містить POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
    # Приклад .env:
    # POSTGRES_USER=myuser
    # POSTGRES_PASSWORD=mysecretpassword
    # POSTGRES_DB=mydb
    # POSTGRES_HOST=localhost
    # POSTGRES_PORT=5432

    # Для Alembic, він буде читати DATABASE_URL з alembic.ini, який, в свою чергу,
    # читає з змінної середовища DATABASE_URL.
    # Тому, запускаючи Alembic, ти маєш встановити DATABASE_URL
    # export DATABASE_URL="postgresql+psycopg2://myuser:mysecretpassword@localhost:5432/mydb"

    # Якщо ти хочеш, щоб цей __main__ блок також використовував DATABASE_URL
    # з env.py, то не потрібно передавати його в Database(), бо він вже дефолтний.
    db.create_all_tables()

    try:
        session = db.get_session()
        logger.info("Сесія бази даних успішно отримана і може бути використана.")
    except Exception as e:
        logger.error(f"Помилка при отриманні сесії бази даних: {e}")
    finally:
        if 'session' in locals() and session:
            session.close()
            logger.info("Сесія бази даних закрита.")