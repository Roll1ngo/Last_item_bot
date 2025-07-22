import os
import shutil
import time

from functions.logger_config import logger


def delete_temp_folders_content():
    """Видаляє весь вміст папки без використання кошика.

    Args:
        folder_path: Шлях до папки, вміст якої потрібно видалити.
    """

    script_dir = os.path.dirname(os.path.abspath(__file__)) # Отримуємо директорію скрипту
    folder_path_list = os.path.join(script_dir, "..", "temp", "json_list")
    folder_path_product = os.path.join(script_dir, "..", "temp", "json_product")

    for folder_path in [folder_path_list, folder_path_product]:
        try:
            if os.path.exists(folder_path):  # Перевірка чи існує папка
                for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)  # Для файлів та символічних посилань
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)  # Для папок (включаючи вміст)
                    except Exception as e:
                        print(f"Помилка видалення {file_path}: {e}")
            else:
                print(f"Папка '{folder_path}' не існує.")
        except Exception as e:
            print(f"Загальна помилка при видаленні вмісту папки: {e}")

    logger.info(f"Вміст папки temp успішно видалено.")


if __name__ == '__main__':
    delete_temp_folders_content()
