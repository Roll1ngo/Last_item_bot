import requests
import os
import json
from urllib.parse import urlencode
import re
from datetime import datetime
import csv
import time

from functions.delete_temp_folders import delete_temp_folders_content
from functions.load_config import get_config
from functions.logger_config import logger
from functions.authorization import start_file_check
from price_calculate import asterisk, plus, hash_, star, double_parentheses, tilde, backtick, vertical_bar, \
    double_square_brackets, double_angle_brackets, caret, dollar, ampersand, double_percent


config = get_config()
if config:
    owner = config.get('owner')
    user_agent = config.get('user_agent')
    pause_between_runs = config.get('pause_between_runs')
    delete_temp_folders = config.get('delete_temp_folders')

else:
    logger.critical("Помилка завантаження конфігурації.")

RED = "\033[31m"
RESET = "\033[0m"

patterns = {
    r"\*([^\*]+)\*": asterisk,  # Маленька зірочка
    r"\+([^+]+)\+": plus,  # Плюс
    r"\#([^\#]+)\#": hash_,  # Решітка (хеш)
    r"★([^★]+)★": star,  # Зірка
    r"\(\(([^()]+)\)\)": double_parentheses,  # Подвійні круглі дужки"double parentheses",  # Подвійні круглі дужки
    r"~([^~]+)~": tilde,  # Тильда"tilde",  # Тильда
    r"\`([^\`]+)\`": backtick,  # З"backtick",  # Зворотний апостроф (гравіс)
    r"\|([^\|]+)\|": vertical_bar,  # "vertical bar",  # Вертикальна риска
    r"\[\[([^\[\]]+)\]\]": double_square_brackets,  # Подвійні квадратні дужки
    r"\<\<([^\<\>]+)\>\>": double_angle_brackets,  # Подвійні кутові дужки
    r"\^([^\^]+)\^": caret,  # Каретка (знак степеня)""caret",  # Каретка (знак степеня)
    r"\$([^\$]+)\$": dollar,  # Долар"dollar",  # Долар
    r"\&([^\&]+)\&": ampersand,  # Амперсанд"ampersand",  # Амперсанд
    r"%%([^%]+)%%": double_percent,  # Подвійний відсоток"double percent",  # Подвійний відсоток
}

# Створення папок
current_directory = os.getcwd()
temp_path = os.path.join(current_directory, "temp")
json_product = os.path.join(temp_path, "json_product")
json_list = os.path.join(temp_path, "json_list")

os.makedirs(temp_path, exist_ok=True)
os.makedirs(json_product, exist_ok=True)
os.makedirs(json_list, exist_ok=True)

# Налаштування logger
log_directory = os.path.expanduser("~/my_log.log")
log_file_path = os.path.join(log_directory, 'logs.log')


def get_authorization():
    start_file_check()

    destination_file_path = os.path.join(os.getcwd(), "authorization.json")
    with open(destination_file_path, "r") as file:
        config_data = json.load(file)

    authorization = config_data["Authorization"]
    return authorization


def get_offer_id():
    # Читаємо файл з IDs товарів
    all_data = []
    with open("offer_id.csv", newline="") as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            all_data.append(row[0])
    return all_data


# Отримуємо інформацію про товар
def get_product(offers_id):
    for offer_id in offers_id:
        logger.info(f'_________________________________________________________________________________________________'
                    f'____')
        authorization = get_authorization()
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            "authorization": authorization,
            "origin": "https://www.g2g.com",
            "priority": "u=1, i",
            "referer": "https://www.g2g.com/",
            "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": user_agent,
        }

        params = {
            "currency": "USD",
            "country": "UA",
            "include_out_of_stock": "1",
            "include_inactive": "1",
        }
        url = "https://sls.g2g.com/offer/"

        response = requests.get(f"{url}{offer_id}", params=params, headers=headers)
        data_json = response.json()
        filename_all_data, filename_params, pattern_name, short_title = receiving_data(data_json)

        if filename_params is None:
            continue

        filename_list = get_list_product(filename_params)
        if filename_list is None:
            continue
        # filename_list = "temp/json_list/_G1707839512558BX_list.json"
        price_study(filename_list, pattern_name, short_title, authorization)


# Парсимо інформацію про конкурентів з API
def receiving_data(data):
    try:
        json_data = data["payload"]
        offer_id = json_data["offer_id"]
        unit_price = json_data["unit_price"]
        title = json_data["title"]

        q, pattern_name = find_pattern(title)
        if q is None:
            return None, None, None, None

        brand_id = json_data["brand_id"]
        seo_term = None

        if brand_id == "lgc_game_29076":
            seo_term = "wow-classic-item"
        elif brand_id == "lgc_game_27816":
            seo_term = "wow-classic-era-item"

        region_id = json_data["region_id"]
        filter_attr_row = json_data["offer_attributes"][1]
        collection_id = filter_attr_row["collection_id"]
        dataset_id = filter_attr_row["dataset_id"]
        filter_attr = f"{collection_id}:{dataset_id}"
        all_data = {
            "offer_id": offer_id,
            "unit_price": unit_price,
        }
        params = {
            "seo_term": seo_term,
            "region_id": region_id,
            "q": q,
            "filter_attr": filter_attr,
        }
        filename_all_data = os.path.join(json_product, f"{offer_id}_all_data.json")

        with open(filename_all_data, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)

        filename_params = os.path.join(json_product, f"{offer_id}_params.json")

        if filename_params is None:
            logger.warning(f"Перевірити {offer_id}!!!")

        with open(filename_params, "w", encoding="utf-8") as f:
            json.dump(params, f, ensure_ascii=False, indent=4)

        return filename_all_data, filename_params, pattern_name, q

    except KeyError as e:
        logger.critical(f"Перевір товар: відсутній ключ {e}")
        return None, None, None, None
    except Exception as e:
        logger.critical(f"Виникла помилка у парсері: {e}")
        return None, None, None, None


def find_pattern(title):
    # title = '[20th Anniversary] * Cloudkeeper Legplates * Maladath [US] - Horde - INSTANT DELIVERY'

    for pattern, pattern_name in patterns.items():
        match = re.findall(pattern, title)
        try:
            if match:
                q = match[0]
                logger.info(f"pattern__{pattern_name.__name__.upper()}, match__{q}")
                return q, pattern_name

        except ValueError:
            logger.error(f"ValueError при обробці {title}. Паттерн: {pattern}")
            continue
    else:
        logger.warning(f" Пропускаємо: Паттерн не знайдено для title: {title}")
        return None, None


# Отримуємо список конкурентів
def get_list_product(filename_params):
    # authorization = get_authorization()
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        # "authorization": authorization,
        "origin": "https://www.g2g.com",
        "priority": "u=1, i",
        "referer": "https://www.g2g.com/",
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": user_agent,
    }
    filename = filename_params.split("\\")[-1]  # Отримуємо останню частину шляху
    identifier = filename.split("_")[0]  # Розділяємо по '_' та беремо першу частину
    with open(filename_params, encoding="utf-8") as f:
        params = json.load(f)

    # Додаємо параметри для формування url
    params["page_size"] = 48
    params["sort"] = "lowest_price"
    params["currency"] = 'USD'
    params["country"] = "UA"
    base_url = "https://sls.g2g.com/offer/search"

    # Формуємо остаточний url з параметрами сторінки
    encoded_params = urlencode(params)
    full_url = f"{base_url}?{encoded_params}"
    response = requests.get(
        full_url,
        headers=headers,
    )
    if response.status_code == 200:
        json_data = response.json()
        filename_list = os.path.join(json_list, f"{identifier}_list.json")

        with open(filename_list, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)

        return filename_list
    else:
        logger.error(f"Помилка при отриманні списку конкурентів: {response.status_code}")
        return None


# Завантажуємо інформацію про конкурентів
def price_study(filename_list, pattern_name, short_title, authorization):
    filename = filename_list.split("\\")[-1]  # Отримуємо останню частину шляху
    identifier = filename.split("_")[0]  # Розділяємо по '_' та беремо першу частину
    # filename_list = 'temp/json_list/_G1707831706525MW_list.json'
    try:
        with open(filename_list, encoding="utf-8") as f:
            data = json.load(f)

        payload = data.get("payload", {})
        results = payload.get("results", [])
        if len(results) == 0:
            logger.warning(f"Список продавців порожній на товар {short_title}")
            return

        competitors = {}
        price_before_update = 0
        for position, result in enumerate(results, start=1):
            username = result['username']

            # if username == owner and position == 1 and pattern_name.__name__ != 'asterisk':
            #     return (f"Allbestfory перший у списку на товар {short_title}"
            #             f" з ціною {RED}{result["display_price"]}{RESET}")

            if username == owner:
                price_before_update = result['display_price']
            unit_price = float(result["unit_price"])
            display_price = float(result["display_price"])
            title = result["title"]
            competitors[position] = {'username': username,
                                     'unit_price': unit_price,
                                     'display_price': display_price,
                                     'short_title': short_title,
                                     'title': title
                                         }

        new_price = pattern_name(competitors)

        if isinstance(new_price, str):
            logger.warning(new_price)
            return

        elif isinstance(new_price, int):
            logger.warning(f"Ціна змінена для {short_title} з"
                           f" {RED}{price_before_update}{RESET} на {RED}{new_price}{RESET}")

        elif isinstance(new_price, dict):
            if "warning" in new_price:
                warning_message = new_price["warning"]
                logger.warning(f"{warning_message}")
                return

            elif "critical" in new_price:
                critical_message = new_price["critical"]
                logger.critical(f"{critical_message}")
                return

        price_change_request(identifier, new_price, short_title, price_before_update, authorization)

    except FileNotFoundError:
        logger.info(f"Файл {filename_list} не знайдено.")
    except json.JSONDecodeError:
        logger.info("Помилка декодування JSON.")
    except Exception as e:
        logger.info(f"Невизначена помилка: {e}")


# Відправляємо дані на API для зміни ціни
def price_change_request(identifier, new_price, short_title, price_before_update, authorization):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        "authorization": authorization,
        "content-type": "application/json",
        "priority": "u=1, i",
        "user-agent": user_agent,
    }

    json_data = {
        "unit_price": new_price,
        "seller_id": "5688923",
    }
    logger.info(f"Ціна перед відправкою__{new_price}")
    response = requests.put(
        f"https://sls.g2g.com/offer/{identifier}", headers=headers, json=json_data
    )

    if response.status_code == 200:
        logger.info(f"Ціна змінена для {short_title} з {RED}{price_before_update}{RESET} на {RED}{new_price}{RESET}")
    else:
        now = datetime.now()
        formatted_datetime = now.strftime("%H:%M:%S %d.%m.%Y")
        logger.critical(f"Перевір товар {short_title} за {identifier}")
        logger.critical(f"{response.status_code} не вдалось відправити оновлені ціни"
                        f" на {short_title} в {formatted_datetime}")
        logger.critical("ОНОВИ authorization !!!!!")


if __name__ == "__main__":

    while True:

        logger.info("Починаємо роботу")
        # list_offer_id = ['G1707831706525MW']
        list_offer_id = get_offer_id()

        get_product(list_offer_id)

        delete_temp_folders_content() if delete_temp_folders else False

        logger.info(f"Закінчили, продовжимо через {pause_between_runs} секунд")

        time.sleep(pause_between_runs)
