import os
import shutil
from .logger_config import logger


def move_authorization_file():
    # Определение пути к папке загрузок текущего пользователя
    user_downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    source_file_path = os.path.join(user_downloads_folder, "authorization.json")

    # Определение пути к текущей директории
    destination_file_path = os.path.join(os.getcwd(), "authorization.json")

    # Проверка доступа к файлу
    def is_file_accessible(filepath, mode="r"):
        try:
            with open(filepath, mode):
                pass
        except IOError:
            return False
        return True

    # Функция для удаления файла, если он существует
    def remove_existing_file(filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    # Копирование и удаление файла
    if is_file_accessible(source_file_path, "r"):
        try:
            remove_existing_file(destination_file_path)
            shutil.copy(source_file_path, destination_file_path)
            os.remove(source_file_path)
            logger.info(f"Файл {source_file_path} успешно скопирован и удален.")
        except Exception as e:
            logger.error(f"Ошибка при копировании файла: {e}")
    else:
        pass


def start_file_check():
    move_authorization_file()
