import os
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base

# Припускаємо, що ці функції існують і працюють коректно
from functions.load_config import get_config
from functions.logger_config import logger

# Base для декларативних моделей SQLAlchemy
Base = declarative_base()

# Завантажуємо конфігурацію
config = get_config()

# Визначаємо шлях до локальної бази даних SQLite
# Використовуємо .parent.parent, якщо 'functions' знаходиться на рівень вище
# або .parent, якщо main.py і item_bot.sqlite в одній директорії з файлом, де цей код
# Наприклад: project_root/data/item_bot.sqlite
# Або: project_root/item_bot.sqlite
LOCAL_DATA_BASE_PATH = str(Path(__file__).resolve().parent /'item_bot.sqlite')

# Альтернативний варіант, якщо item_bot.sqlite знаходиться в тій же директорії, що і цей файл:
# LOCAL_DATA_BASE_PATH = str(Path(__file__).resolve().parent / 'item_bot.sqlite')


class Database:
    _instance = None

    def __new__(cls, *args, **kwargs):
        # Реалізація синглтона: гарантує, що існує лише один екземпляр класу Database
        if not cls._instance:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self, database_url: str = None):
        # Запобігаємо повторній ініціалізації синглтона
        if hasattr(self, '_initialized') and self._initialized:
            return

        # Використовуємо переданий URL, якщо він є, інакше - локальний шлях
        # Якщо database_url не передається, за замовчуванням береться LOCAL_DATA_BASE_PATH
        self.database_url = database_url or f"sqlite:///{LOCAL_DATA_BASE_PATH}"

        try:
            # Створюємо рушій (engine) для підключення до бази даних
            self.engine = create_engine(
                self.database_url,
                future=True,  # Використовувати майбутні API SQLAlchemy
                echo=False    # Не виводити SQL запити в консоль (для дебагу можна поставити True)
            )

            # Створюємо фабрику сесій
            self.SessionLocal = sessionmaker(
                autocommit=False,  # Відключаємо авто-коміт
                autoflush=False,   # Відключаємо авто-флаш
                bind=self.engine,  # Прив'язуємо сесії до нашого рушія
                class_=Session     # Використовуємо клас Session SQLAlchemy
            )
            self._initialized = True
            logger.info(f"Підключення до бази даних за URL '{self.database_url}' успішно налаштовано.")

        except Exception as e:
            logger.error(f"Помилка під час ініціалізації підключення до бази даних за URL '{self.database_url}': {e}")
            # Додатково можна викликати sys.exit(1) або повторно викинути виняток, якщо помилка критична
            raise

    def get_session(self) -> Session:
        """Повертає нову сесію бази даних."""
        return self.SessionLocal()

    def create_all_tables(self):
        """Створює всі таблиці, визначені в Base.metadata, якщо вони ще не існують."""
        try:
            Base.metadata.create_all(self.engine)
            logger.info(f"Таблиці бази даних за URL '{self.database_url}' перевірено/створено.")
        except Exception as e:
            logger.error(f"Помилка при створенні таблиць бази даних за URL '{self.database_url}': {e}")
            raise

# Створюємо єдиний екземпляр класу Database
# За замовчуванням він використовуватиме LOCAL_DATA_BASE_PATH
db = Database()

# Приклад використання (може бути в іншому файлі або в main.py)
if __name__ == '__main__':
    # Створити таблиці при запуску, якщо вони ще не існують
    db.create_all_tables()

    # Приклад отримання сесії
    try:
        session = db.get_session()
        # Тут можна працювати з базою даних, наприклад:
        # new_item = Item(name="Test Item")
        # session.add(new_item)
        # session.commit()
        logger.info("Сесія бази даних успішно отримана і може бути використана.")
    except Exception as e:
        logger.error(f"Помилка при отриманні сесії бази даних: {e}")
    finally:
        if 'session' in locals() and session:
            session.close() # Важливо закривати сесію після використання
            logger.info("Сесія бази даних закрита.")