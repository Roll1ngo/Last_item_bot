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
from price_calculate import general_patterns, get_random_price_range

config = get_config()
if config:
    owner = config.get('owner')
    user_agent = config.get('user_agent')
    pause_between_runs = config.get('pause_between_runs')
    delete_temp_folders = config.get('delete_temp_folders')
    test_mode_logs = config.get('test_mode_logs')
    ignore_words = config.get('ignore_words')

    display_price_change_value = config.get("price_range", {}).get("display_price_change_value")

    top_minimal_values = config.get("top_minimal")
    default_limit = config.get("default_limit")

    if top_minimal_values:
        top_minimal_asterisk = top_minimal_values.get("asterisk", default_limit)
        top_minimal_plus = top_minimal_values.get("plus", default_limit)
        top_minimal_star = top_minimal_values.get("star", default_limit)
        top_minimal_hash = top_minimal_values.get("hash", default_limit)
        top_minimal_equality = top_minimal_values.get("equality", default_limit)
        top_minimal_tilde = top_minimal_values.get("tilde", default_limit)
        top_minimal_backtick = top_minimal_values.get("backtick", default_limit)
        top_minimal_vertical_bar = top_minimal_values.get("vertical_bar", default_limit)
        top_minimal_double_exclamation = top_minimal_values.get("double_exclamation", default_limit)
        top_minimal_double_double_underscore = top_minimal_values.get("double_underscore", default_limit)
        top_minimal_caret = top_minimal_values.get("caret", default_limit)
        top_minimal_dollar = top_minimal_values.get("dollar", default_limit)
        top_minimal_ampersand = top_minimal_values.get("ampersand", default_limit)
        top_minimal_double_percent = top_minimal_values.get("double_percent", default_limit)
        top_minimal_degree = top_minimal_values.get("degree", default_limit)
        top_minimal_square = top_minimal_values.get("square", default_limit)
        top_minimal_black_circle = top_minimal_values.get("black_circle", default_limit)
        top_minimal_down_arrow = top_minimal_values.get("down_arrow", default_limit)
        top_minimal_diamond = top_minimal_values.get("diamond", default_limit)
        top_minimal_copyright = top_minimal_values.get("copyright", default_limit)
        top_minimal_sun = top_minimal_values.get("sun", default_limit)
    else:
        logger.critical("Помилка завантаження конфігурації лімітів. Перевірте файл конфігурації.")
else:
    logger.critical("Помилка завантаження конфігурації.")

# Отримання мажорної версії браузера для заголовка
search_match = re.search(r"Chrome/(\d+)", user_agent)
major_version_chrome = search_match.group(1)
logger.critical(F"major_version_chrome: {major_version_chrome}") if test_mode_logs else None

RED = "\033[31m"
RESET = "\033[0m"

patterns = {
    r"\*([^\*]+)\*": {'limit': top_minimal_asterisk, 'name': "asterisk", 'symbol': '*'},  # Маленька зірочка
    r"\+([^+]+)\+": {'limit': top_minimal_plus, 'name': "plus", 'symbol': '+'},  # Плюс
    r"\#([^\#]+)\#": {'limit': top_minimal_hash, 'name': "hash", 'symbol': '#'},  # Решітка (хеш)
    r"★([^★]+)★": {'limit': top_minimal_star, 'name': "star", 'symbol': '★'},  # Зірка
    r"=([^=]+)=": {'limit': top_minimal_equality, 'name': "equality", 'symbol': '='},  # Знак рівності (=)
    r"~([^~]+)~": {'limit': top_minimal_tilde, 'name': "tilde", 'symbol': '~'},  # Тильда
    r"\`([^\`]+)\`": {'limit': top_minimal_backtick, 'name': "backtick", 'symbol': '`'},  # Зворотний апостроф (гравіс)
    r"\|([^\|]+)\|": {'limit': top_minimal_vertical_bar, 'name': "vertical_bar", 'symbol': '|'},  # Вертикальна риска
    r"\!\!([^\!]+)\!\!": {'limit': top_minimal_double_exclamation,
                          'name': "double_exclamation", 'symbol': '!!'},  # Подвійний знак оклику (!!)
    r"__([^_]+)__": {'limit': top_minimal_double_double_underscore,
                     'name': "double_underscore", 'symbol': '__'},  # Подвійне підкреслення (__)
    r"\^([^\^]+)\^": {'limit': top_minimal_caret, 'name': "caret", 'symbol': '^'},  # Каретка (знак степеня)
    r"\$([^\$]+)\$": {'limit': top_minimal_dollar, 'name': "dollar", 'symbol': '$'},  # Долар
    r"\&([^\&]+)\&": {'limit': top_minimal_ampersand, 'name': "ampersand", 'symbol': '&'},  # Амперсанд
    r"%%([^%]+)%%": {'limit': top_minimal_double_percent,
                     'name': "double_percent", 'symbol': '%%'},  # Подвійний відсоток

    r"°([^°]+)°": {'limit': top_minimal_degree, 'name': "degree", 'symbol': '°'},  # (°) градус
    r"□([^□]+)□": {'limit': top_minimal_square, 'name': "square", 'symbol': '□'},  # (□) квадратик
    r"●([^●]+)●": {'limit': top_minimal_black_circle, 'name': "black_circle", 'symbol': '●'},  # (●) чорна крапочка
    r"▼([^▼]+)▼": {'limit': top_minimal_down_arrow, 'name': "down_arrow", 'symbol': '▼'},  # (▼) стрілка вниз
    r"◊([^◊]+)◊": {'limit': top_minimal_diamond, 'name': "diamond", 'symbol': '◊'},  # (◊) ромбик
    r"©([^©]+)©": {'limit': top_minimal_copyright, 'name': "copyright", 'symbol': '©'},  # (©) торгова марка
    r"☼([^☼]+)☼": {'limit': top_minimal_sun, 'name': "sun", 'symbol': '☼'},  # (☼) сонечко
}

# Створення папок
current_directory = os.getcwd()
temp_path = os.path.join(current_directory, "temp")
json_product = os.path.join(temp_path, "json_product")
json_list = os.path.join(temp_path, "json_list")

os.makedirs(temp_path, exist_ok=True)
os.makedirs(json_product, exist_ok=True)
os.makedirs(json_list, exist_ok=True)


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
        logger.info(
            f'_________________________________________________________________________________________________'
            f'____')

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            "origin": "https://www.g2g.com",
            "priority": "u=1, i",
            "referer": "https://www.g2g.com/",
            "sec-ch-ua": f'"Not/A)Brand";v="8",'
                         f' "Chromium";v={major_version_chrome}, "Google Chrome";v={major_version_chrome}',
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

        response = get_response(url=url, offer_id=offer_id, headers=headers, params=params)
        if response is None:
            logger.error("Не вдалося підключитись до сервера. Спробуємо пізніше")
            continue

        data_json = response.json()
        filename_all_data, filename_params, owner_offer_info = receiving_data(data_json)

        if filename_params is None or owner_offer_info is None:
            continue

        owner_offer_info['offer_id'] = offer_id
        filename_list = get_list_product(filename_params)
        if filename_list is None:
            logger.critical(f"Не можемо отримати список продавців для товару {owner_offer_info['short_title']}")
            continue
        price_study(filename_list, owner_offer_info)


# Парсимо інформацію про конкурентів з API
def receiving_data(data):
    try:
        json_data = data["payload"]
        offer_id = json_data["offer_id"]
        unit_price = json_data["unit_price"]
        title = json_data["title"]

        owner_offer_info = find_pattern(title)
        if owner_offer_info is None:
            return None, None, None
        q = owner_offer_info['short_title']

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
            logger.critical(f"Перевірити {offer_id}!!!")

        with open(filename_params, "w", encoding="utf-8") as f:
            json.dump(params, f, ensure_ascii=False, indent=4)

        return filename_all_data, filename_params, owner_offer_info

    except KeyError as e:
        logger.critical(f"Перевір товар: відсутній ключ {e}")
        return None, None, None
    except Exception as e:
        logger.critical(f"Виникла помилка у парсері: {e}")
        return None, None, None


def find_pattern(owner_title):
    owner_offer_info = {}
    for pattern, pattern_limit in patterns.items():
        match = re.findall(pattern, owner_title)
        try:
            if match:
                result = match[0]
                offer_type = define_offer_type(result)

                owner_offer_info['pattern_name'] = pattern_limit['name']
                owner_offer_info['limit'] = pattern_limit['limit']
                owner_offer_info['pattern_symbol'] = pattern_limit['symbol']

                owner_offer_info['title'] = owner_title
                owner_offer_info['short_title'] = result.strip()
                owner_offer_info['offer_type'] = offer_type

                return owner_offer_info

        except ValueError:
            logger.critical(f"ValueError при обробці {owner_title}. Паттерн: {pattern}")
            continue
    else:
        logger.warning(f" Пропускаємо: Паттерн не знайдено для title: {owner_title}")
        return None


def define_offer_type(title):
    exist_ignore_in_owner_title = any(word in title for word in ignore_words)
    offer_type = 'schematic_type' if exist_ignore_in_owner_title else 'item_type'
    return offer_type


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
        "sec-ch-ua": f'"Not/A)Brand";v="8",'
                     f' "Chromium";v={major_version_chrome}, "Google Chrome";v={major_version_chrome}',
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
    response = get_response(full_url=full_url, headers=headers)
    if response is None:  # Перевіряємо, чи немає відповіді
        logger.critical("Немає відповіді сервера. Ідемо далі")
        return None
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
def price_study(filename_list, owner_offer_info):
    filename = filename_list.split("\\")[-1]  # Отримуємо останню частину шляху
    identifier = filename.split("_")[0]  # Розділяємо по '_' та беремо першу частину

    owner_offer_id = owner_offer_info['offer_id']
    owner_short_title = owner_offer_info['short_title']
    owner_offer_type = owner_offer_info['offer_type']

    try:
        with open(filename_list, encoding="utf-8") as f:
            data = json.load(f)

        payload = data.get("payload", {})
        results = payload.get("results", [])

        competitors = {'owner_offer_info': owner_offer_info}
        position = 0
        price_before_update = 0

        for index, result in enumerate(results, start=1):

            username = result['username']
            competitor_unit_price = round(float(result["unit_price"]), 6)
            competitor_display_price = float(result["display_price"])
            competitor_title = result["title"]
            competitor_offer_id = result["offer_id"]
            competitor_offer_type = define_offer_type(competitor_title)
            competitor_currency = result["offer_currency"]
            change_price_coefficient = get_random_price_range()

            if competitor_offer_type != owner_offer_type:
                continue

            if username == owner and owner_offer_id != competitor_offer_id:
                logger.critical(f" skip invalid id for: {username}, currently work with_offer_id: {owner_offer_id},"
                                f" but {owner} have another lot competitor_offer_id: {competitor_offer_id}"
                                f" owner_offer_type: {owner_offer_type}, competitor_offer_type: {competitor_offer_type}"
                                f" competitor_title: {competitor_title}") if test_mode_logs else None
                continue

            position += 1

            if username == owner and owner_offer_id == competitor_offer_id:
                competitors['owner_offer_info']['previous_price'] = competitor_unit_price
                competitors['owner_offer_info']['position'] = position
                price_before_update = competitor_unit_price

            if competitor_currency != 'USD':
                previous_competitor_unit_price = competitor_unit_price
                competitor_unit_price = competitor_display_price
                change_price_coefficient = display_price_change_value
                logger.warning(f"У продавця {username} валюта {RED}{competitor_currency}{RESET} не відповідає USD. "
                               f"Змінено параметр ціни з unit_price {RED}{previous_competitor_unit_price}{RESET}"
                               f" на display_price {competitor_unit_price}"
                               f" у товарі {competitor_title}") if test_mode_logs else None

            competitors[position] = {'username': username,
                                     'unit_price': competitor_unit_price,
                                     'title': competitor_title,
                                     'offer_type': competitor_offer_type,
                                     'competitor_currency': competitor_currency,
                                     'change_price_coefficient': change_price_coefficient,
                                     }
        if len(competitors) < 2:
            logger.warning(f"Список продавців порожній на товар {owner_short_title}")
            return

        new_price = general_patterns(competitors)

        if isinstance(new_price, str):
            logger.info(new_price)
            return

        elif isinstance(new_price, (int, float)):
            price_change_request(identifier, new_price, owner_short_title, price_before_update)

        elif isinstance(new_price, dict):
            if "warning" in new_price:
                warning_message = new_price["warning"]
                logger.critical(f"{warning_message}")
                return

            elif "critical" in new_price:
                critical_message = new_price["critical"]
                logger.critical(f"{critical_message}")
                return
    except FileNotFoundError:
        logger.critical(f"Файл {filename_list} не знайдено.")
    except json.JSONDecodeError:
        logger.critical("Помилка декодування JSON.")
    except Exception as e:
        logger.critical(f"Невизначена помилка: {e}")


# Відправляємо дані на API для зміни ціни
def price_change_request(identifier, new_price, owner_short_title, price_before_update):
    authorization = get_authorization()
    new_price = round(new_price, 6)
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
        "seller_id": "5688923"
    }
    response = requests.put(
        f"https://sls.g2g.com/offer/{identifier}", headers=headers, json=json_data
    )

    if response.status_code != 200:
        now = datetime.now()
        formatted_datetime = now.strftime("%H:%M:%S %d.%m.%Y")
        logger.critical(f"Перевір товар {owner_short_title} за {identifier}")
        logger.critical(f"{response.status_code} не вдалось відправити оновлені ціни"
                        f" на {owner_short_title} в {formatted_datetime}")
        logger.critical("ОНОВИ authorization !!!!!")

    else:
        logger.CHANGE_PRICE(f"Ціна змінена для {owner_short_title} з"
                            f" {RED}{price_before_update}{RESET}"
                            f" на {RED}{new_price}{RESET}") if test_mode_logs else None


def get_response(url=None, offer_id=None, full_url=None, headers=None, params=None):
    if full_url:
        target_url = full_url
    elif url and offer_id:
        target_url = f"{url}{offer_id}"
    else:
        logger.error("Incorrect usage: Either 'full_url' or both 'url' and 'offer_id' must be provided.")
        return None

    max_retries = 10
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            if params:
                response = requests.get(target_url, params=params, headers=headers)
            else:
                response = requests.get(target_url, headers=headers)

            response.raise_for_status()
            logger.info(
                f"Successfully retrieved data after {attempt + 1} attempt(s).") if test_mode_logs else None
            return response
        except ConnectionError as e:
            logger.critical(
                f"Connection error: {e}. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(retry_delay)
        except requests.exceptions.HTTPError as e:
            logger.warning(
                f"HTTP error: {e}. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})"
            )
            time.sleep(retry_delay)
            continue
        except requests.exceptions.Timeout as e:
            logger.critical(
                f"Timeout error: {e}. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(retry_delay)

        except Exception as e:
            logger.critical(f"An unexpected error occurred: {e}. "
                            f"Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(retry_delay)

    logger.error(f"Failed to retrieve data after {max_retries} attempts.")
    return None


if __name__ == "__main__":

    while True:
        logger.info(f"Починаємо роботу, кількість товарів для обробки: {len(get_offer_id())}")
        # list_offer_id = ['G1707831706525MW']
        list_offer_id = get_offer_id()

        get_product(list_offer_id)

        delete_temp_folders_content() if delete_temp_folders else False

        logger.info(f"Закінчили, продовжимо через {pause_between_runs} секунд")

        time.sleep(pause_between_runs)
