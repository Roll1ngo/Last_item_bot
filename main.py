import requests
import os
import json
from urllib.parse import urlencode
import re
from datetime import datetime
import csv
import time

from functions.load_config import get_config
from functions.logger_config import logger
from price_calculate import general_patterns
from dotenv import load_dotenv

load_dotenv()
config = get_config()


if config:
    owner = config.get('owner')
    user_agent = config.get('user_agent')
    platform = config.get('platform')
    pause_between_runs = config.get('pause_between_runs')
    delete_temp_folders = config.get('delete_temp_folders')
    test_mode_logs = config.get('test_mode_logs')
    ignore_words = config.get('ignore_words')

    position_for_change_pattern_if_owner_price_over_limit =\
        config.get('position_for_change_pattern_if_owner_price_over_limit')
    position_for_change_pattern_if_owner_price_under_limit =\
        config.get('position_for_change_pattern_if_owner_price_under_limit')

    ignore_competitors_for_asterisk = config.get('ignore_competitors_for_asterisk')
    ignore_competitors_for_other_patterns = config.get('ignore_competitors_for_other_patterns')

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
        top_minimal_sun = top_minimal_values.get("sun", default_limit)
        top_minimal_triangle = top_minimal_values.get("triangle", default_limit)
        top_minimal_up_arrow = top_minimal_values.get("up_arrow", default_limit)
        top_minimal_double_arrow = top_minimal_values.get("double_arrow", default_limit)
        top_minimal_circled_one = top_minimal_values.get("circled_one", default_limit)
        top_minimal_double_cross = top_minimal_values.get("double_cross", default_limit)
        top_minimal_square_with_round_corners = top_minimal_values.get("square_with_round_corners", default_limit)
        top_minimal_dotted_circle = top_minimal_values.get("dotted_circle", default_limit)
        top_minimal_diamond_with_dot = top_minimal_values.get("diamond_with_dot", default_limit)
        top_minimal_flower = top_minimal_values.get("flower", default_limit)
        top_minimal_four_pointed_star = top_minimal_values.get("four_pointed_star", default_limit)
        top_minimal_heart = top_minimal_values.get("heart", default_limit)
        top_minimal_white_star = top_minimal_values.get("white_star", default_limit)
        top_minimal_square_with_triangle = top_minimal_values.get("square_with_triangle", default_limit)
        top_minimal_half_circle_right = top_minimal_values.get("half_circle_right", default_limit)
        top_minimal_black_diamond = top_minimal_values.get("black_diamond", default_limit)
        top_minimal_white_diamond = top_minimal_values.get("white_diamond", default_limit)
        top_minimal_diamond_with_plus = top_minimal_values.get("diamond_with_plus", default_limit)
        top_minimal_bullseye = top_minimal_values.get("bullseye", default_limit)
        top_minimal_chess_king = top_minimal_values.get("chess_king", default_limit)
        top_minimal_chess_queen = top_minimal_values.get("chess_queen", default_limit)
        top_minimal_chess_rook = top_minimal_values.get("chess_rook", default_limit)
        top_minimal_chess_bishop = top_minimal_values.get("chess_bishop", default_limit)
        top_minimal_chess_knight = top_minimal_values.get("chess_knight", default_limit)
        top_minimal_chess_pawn = top_minimal_values.get("chess_pawn", default_limit)
        top_minimal_chess_king_black = top_minimal_values.get("chess_king_black", default_limit)
        top_minimal_chess_queen_black = top_minimal_values.get("chess_queen_black", default_limit)
        top_minimal_chess_rook_black = top_minimal_values.get("chess_rook_black", default_limit)
        top_minimal_chess_bishop_black = top_minimal_values.get("chess_bishop_black", default_limit)
        top_minimal_chess_knight_black = top_minimal_values.get("chess_knight_black", default_limit)
        top_minimal_ball_number_one = top_minimal_values.get("ball_number_one", default_limit)
    else:
        logger.critical("Помилка завантаження конфігурації лімітів. Перевірте файл конфігурації.")
else:
    logger.critical("Помилка завантаження конфігурації.")

# Визначення кольору для значення "unit_price" у логах
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
    r"↑([^↑]+)↑": {'limit': top_minimal_up_arrow, 'name': "up_arrow", 'symbol': '↑'},  # (↑) стрілка вгору
    r"☼([^☼]+)☼": {'limit': top_minimal_sun, 'name': "sun", 'symbol': '☼'},
    r"∆([^∆]+)∆": {'limit': top_minimal_triangle, 'name': "triangle", 'symbol': '∆'},  # (∆) трикутник

    # New patterns
    r"⇔([^⇔]+)⇔": {'limit': top_minimal_double_arrow, 'name': "double_arrow", 'symbol': '⇔'},  # Подвійна стрілка
    r"①([^①]+)①": {'limit': top_minimal_circled_one, 'name': "circled_one", 'symbol': '①'},  # Обведена цифра 1
    r"╬([^╬]+)╬": {'limit': top_minimal_double_cross, 'name': "double_cross", 'symbol': '╬'},  # Подвійний хрест
    r"▣([^▣]+)▣": {'limit': top_minimal_square_with_round_corners, 'name': "square_with_round_corners", 'symbol': '▣'},  # Квадрат з закругленими кутами
    r"◌([^◌]+)◌": {'limit': top_minimal_dotted_circle, 'name': "dotted_circle", 'symbol': '◌'},  # Пунктирне коло
    r"❖([^❖]+)❖": {'limit': top_minimal_diamond_with_dot, 'name': "diamond_with_dot", 'symbol': '❖'},  # Діамант з точкою
    r"❊([^❊]+)❊": {'limit': top_minimal_flower, 'name': "flower", 'symbol': '❊'},  # Квітка
    r"✦([^✦]+)✦": {'limit': top_minimal_four_pointed_star, 'name': "four_pointed_star", 'symbol': '✦'},  # Чотирипроменева зірка
    r"♡([^♡]+)♡": {'limit': top_minimal_heart, 'name': "heart", 'symbol': '♡'},  # Серце
    r"☆([^☆]+)☆": {'limit': top_minimal_white_star, 'name': "white_star", 'symbol': '☆'},  # Біла зірка
    r"◩([^◩]+)◩": {'limit': top_minimal_square_with_triangle,
                   'name': "square_with_triangle", 'symbol': '◩'},  # Квадрат з трикутником
    r"◐([^◐]+)◐": {'limit': top_minimal_half_circle_right,
                   'name': "half_circle_right", 'symbol': '◐'},  # Права половина кола
    r"◆([^◆]+)◆": {'limit': top_minimal_black_diamond, 'name': "black_diamond", 'symbol': '◆'},  # Чорний діамант
    r"◇([^◇]+)◇": {'limit': top_minimal_white_diamond, 'name': "white_diamond", 'symbol': '◇'},  # Білий діамант
    r"◈([^◈]+)◈": {'limit': top_minimal_diamond_with_plus,
                   'name': "diamond_with_plus", 'symbol': '◈'},  # Діамант з плюсом
    r"◉([^◉]+)◉": {'limit': top_minimal_bullseye, 'name': "bullseye", 'symbol': '◉'},  # Яблучко (концентричні кола)
    r"♔([^♔]+)♔": {'limit': top_minimal_chess_king, 'name': "chess_king", 'symbol': '♔'},  # Шаховий король
    r"♕([^♕]+)♕": {'limit': top_minimal_chess_queen, 'name': "chess_queen", 'symbol': '♕'},  # Шахова королева
    r"♖([^♖]+)♖": {'limit': top_minimal_chess_rook, 'name': "chess_rook", 'symbol': '♖'},  # Шахова тура
    r"♗([^♗]+)♗": {'limit': top_minimal_chess_bishop, 'name': "chess_bishop", 'symbol': '♗'},  # Шаховий слон
    r"♘([^♘]+)♘": {'limit': top_minimal_chess_knight, 'name': "chess_knight", 'symbol': '♘'},  # Шаховий кінь
    r"♞([^♞]+)♞": {'limit': top_minimal_chess_knight_black,
                   'name': "chess_knight_black", 'symbol': '♞'},  # Шаховий кінь чорний

    r"♙([^♙]+)♙": {'limit': top_minimal_chess_pawn, 'name': "chess_pawn", 'symbol': '♙'},  # Шахова пішка
    r"♚([^♚]+)♚": {'limit': top_minimal_chess_king_black,
                   'name': "chess_king_black", 'symbol': '♚'},  # Чорний шаховий король
    r"♛([^♛]+)♛": {'limit': top_minimal_chess_queen_black,
                   'name': "chess_queen_black", 'symbol': '♛'},  # Чорна шахова королева
    r"♜([^♜]+)♜": {'limit': top_minimal_chess_rook_black,
                   'name': "chess_rook_black", 'symbol': '♜'},  # Чорна шахова тура
    r"♝([^♝]+)♝": {'limit': top_minimal_chess_bishop_black,
                   'name': "chess_bishop_black", 'symbol': '♝'},  # Чорна Шахова пішка
    r"❶([^❶]+)❶": {'limit': top_minimal_ball_number_one,
                   'name': "ball_number_one", 'symbol': '❶'}  # Чорна Шахова пішка
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
   return os.getenv('AUTH_TOKEN')


# Отримуємо інформацію про товар
def get_product(owner_offer_info):
    authorization = get_authorization()
    offer_id = owner_offer_info.get('offer_id')
    logger.info(
        f'_________________________________________________________________________________________________'
        f'____')

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        "origin": "https://www.g2g.com",
        "priority": "u=1, i",
        "referer": "https://www.g2g.com/",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": platform,
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
        return None, None
    data_json = response.json()
    params = receiving_data(owner_offer_info, data_json)

    if params is None or owner_offer_info is None:
        return None, None

    owner_offer_info['offer_id'] = offer_id
    competitors_list = get_list_product(params, offer_id)
    if competitors_list is None:
        logger.critical(f"Не можемо отримати список продавців для товару {owner_offer_info['short_title']}")
        return None, None
    new_price, new_title = price_study(authorization, competitors_list, owner_offer_info)
    return new_price, new_title


# Парсимо інформацію про конкурентів з API
def receiving_data(owner_offer_info, data):
    try:
        json_data = data["payload"]
        offer_id = json_data["offer_id"]
        title = json_data["title"]

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
        params = {
            "seo_term": seo_term,
            "region_id": region_id,
            "q": q,
            "filter_attr": filter_attr,
        }

        if params is None:
            logger.critical(f"Перевірити {offer_id}!!!")


        return params

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
                owner_offer_info['pattern_regular'] = pattern

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
def get_list_product(params, identifier):
    # authorization = get_authorization()
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        # "authorization": authorization,
        "origin": "https://www.g2g.com",
        "priority": "u=1, i",
        "referer": "https://www.g2g.com/",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": user_agent,
    }

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
        return response.json()
    else:
        logger.error(f"Помилка при отриманні списку конкурентів: {response.status_code}")
        return None


# Завантажуємо інформацію про конкурентів
def price_study(data, owner_offer_info):

    owner_offer_id = owner_offer_info['offer_id']
    owner_short_title = owner_offer_info['short_title']
    owner_offer_type = owner_offer_info['offer_type']
    owner_title = owner_offer_info['title']
    owner_limit = owner_offer_info['limit']
    owner_pattern_name = owner_offer_info['pattern_name']
    owner_ignore_competitors_list = ignore_competitors_for_asterisk if owner_pattern_name == 'asterisk' \
        else ignore_competitors_for_other_patterns

    try:
        payload = data.get("payload", {})
        results = payload.get("results", [])

        competitors = {'owner_offer_info': owner_offer_info}
        position = 0
        price_before_update = 0
        sellers_usernames_list = []
        append_stop_flag = False
        owner_exist_flag = False
        new_title = None

        for index, result in enumerate(results, start=1):

            username = result['username']
            competitor_unit_price = round(float(result["unit_price"]), 6)
            competitor_display_price = float(result["display_price"])
            competitor_title = result["title"]
            competitor_offer_id = result["offer_id"]
            competitor_offer_type = define_offer_type(competitor_title)
            competitor_currency = result["offer_currency"]

            if competitor_offer_type != owner_offer_type:
                continue

            if username == owner and owner_offer_id != competitor_offer_id:
                logger.warning(f" skip invalid id for: {username}, currently work with_offer_id: {owner_offer_id},"
                               f"title: {owner_title} "
                               f" but {owner} have another lot competitor_offer_id: {competitor_offer_id}"
                               f" owner_offer_type: {owner_offer_type}, competitor_offer_type: {competitor_offer_type}"
                               f" competitor_title: {competitor_title}") if test_mode_logs else None
                continue

            position += 1

            if append_stop_flag is False and username not in owner_ignore_competitors_list \
                    and competitor_unit_price < owner_limit:
                sellers_usernames_list.append(username)

            if username == owner and owner_offer_id == competitor_offer_id:
                competitors['owner_offer_info']['previous_price'] = competitor_unit_price
                competitors['owner_offer_info']['position'] = position
                price_before_update = competitor_unit_price
                owner_exist_flag = True
                append_stop_flag = True

            if competitor_currency != 'USD':
                previous_competitor_unit_price = competitor_unit_price
                competitor_unit_price = competitor_display_price

                logger.warning(f"У продавця {username} валюта {RED}{competitor_currency}{RESET} не відповідає USD. "
                               f"Змінено параметр ціни з unit_price {RED}{previous_competitor_unit_price}{RESET}"
                               f" на display_price {RED}{competitor_unit_price}{RESET}"
                               f" у товарі {competitor_title}") if test_mode_logs else None

            # Записуємо дані продавця
            competitors[position] = {'username': username,
                                     'unit_price': competitor_unit_price,
                                     'title': competitor_title,
                                     'offer_type': competitor_offer_type
                                     }

        if len(competitors) < 2:
            logger.warning(f"Список продавців порожній на товар {owner_short_title}")
            return None, None

        if owner_exist_flag is False:
            logger.warning(f" Онови сток на товар {competitor_title}")
            return None, None

        owner_info = competitors['owner_offer_info']
        owner_position = owner_info['position']
        owner_price = owner_info['previous_price']
        logger.info(f"sellers_usernames_list: {sellers_usernames_list}") if test_mode_logs else None

        # Визначаємо чи необхідна зміна патерну та ліміту
        try:
            if (owner_pattern_name != 'asterisk' and owner_price > owner_limit
                    and owner_position >= position_for_change_pattern_if_owner_price_over_limit
                    and len(sellers_usernames_list) >= position_for_change_pattern_if_owner_price_over_limit):

                logger.info(f"{owner} на позиції {owner_position} з ціною {owner_price} вищою за ліміт {owner_limit}"
                            f" та продавці {[seller for seller in sellers_usernames_list if seller != owner]}"
                            f"мають ціну нижчу за ліміт {owner_limit}")

                logger.info('Переходим до зміни патерну та ліміту')
                logger.info(f"previous_owner_info: {competitors['owner_offer_info']}") if test_mode_logs else None
                new_owner_info, new_title = define_new_title_and_owner_info(owner_info)

                # Записуємо нову інформацію про лот власника
                competitors['owner_offer_info'] = new_owner_info
                logger.info(f"new_owner_info: {competitors['owner_offer_info']}") if test_mode_logs else None

            if (owner_pattern_name != 'asterisk' and owner_price < owner_limit
                    and owner_position >= position_for_change_pattern_if_owner_price_under_limit
                    and len(sellers_usernames_list) >= position_for_change_pattern_if_owner_price_under_limit):

                logger.info(f"{owner} на позиції {owner_position} з ціною {owner_price} нижчою за ліміт {owner_limit}"
                            f" та продавці {[seller for seller in sellers_usernames_list if seller != owner]}"
                            f"мають ціну нижчу за ліміт {owner_limit}")
                logger.info('Переходим до зміни патерну та ліміту')
                logger.info(f"previous_owner_info: {competitors['owner_offer_info']}") if test_mode_logs else None
                new_owner_info, new_title = define_new_title_and_owner_info(owner_info)

                # Записуємо нову інформацію про лот власника
                competitors['owner_offer_info'] = new_owner_info
                logger.info(f"new_owner_info: {competitors['owner_offer_info']}") if test_mode_logs else None

        except Exception as e:
            logger.critical(f"Помилка при зміні патерну {e}")
            return None, None
        new_price = general_patterns(competitors)  # Визначаємо поведінку ціни

        if isinstance(new_price, str):
            logger.info(new_price)
            if new_title:
                return None, 'new_title'
            return None, None

        elif isinstance(new_price, (int, float)):
            return  new_price, 'new_title'

        elif isinstance(new_price, dict):
            if "warning" in new_price:
                warning_message = new_price["warning"]
                logger.critical(f"{warning_message}")
                return None, None

            elif "critical" in new_price:
                critical_message = new_price["critical"]
                logger.critical(f"{critical_message}")
                return None, None
    except json.JSONDecodeError:
        logger.critical("Помилка декодування JSON.")
    except Exception as e:
        logger.critical(f"Невизначена помилка: {e}")



# Відправляємо дані на API для зміни ціни
def price_change_request(authorization, identifier, new_price, owner_short_title, price_before_update, new_title=None):

    new_price = round(new_price, 6)
    logger.info(f"new_price: {new_price}, new_title: {new_title}")
    input("Для продовження натисніть Enter")
#     headers = {
#         "accept": "application/json, text/plain, */*",
#         "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
#         "authorization": authorization,
#         "content-type": "application/json",
#         "priority": "u=1, i",
#         "user-agent": user_agent,
#     }
#     if new_title:
#         json_data = {
#             "unit_price": new_price,
#             "seller_id": "5688923",
#             "title": new_title
#         }
#     else:
#         json_data = {
#             "unit_price": new_price,
#             "seller_id": "5688923"
#         }
#
#     logger.info(f"json_data: {json_data}") if test_mode_logs else None
#
#     response = requests.put(
#         f"https://sls.g2g.com/offer/{identifier}", headers=headers, json=json_data
#     )
#
#     if response.status_code != 200:
#         now = datetime.now()
#         formatted_datetime = now.strftime("%H:%M:%S %d.%m.%Y")
#         logger.critical(f"Перевір товар {owner_short_title} за {identifier}")
#         logger.critical(f"{response.status_code} не вдалось відправити оновлені ціни"
#                         f" на {owner_short_title} в {formatted_datetime}")
#         logger.critical("ОНОВИ authorization !!!!!")
#
#     else:
#         logger.CHANGE_PRICE(f"Ціна змінена для {owner_short_title} з"
#                             f" {RED}{price_before_update}{RESET}"
#                             f" на {RED}{new_price}{RESET}") if test_mode_logs else None
#
#
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


def define_new_title_and_owner_info(owner_offer_info):
    owner_title = owner_offer_info['title']
    owner_position = owner_offer_info['position']
    short_title = owner_offer_info['short_title']
    owner_pattern_regular = owner_offer_info['pattern_regular']
    owner_pattern_name = owner_offer_info['pattern_name']
    owner_pattern_limit = owner_offer_info['limit']
    owner_price = owner_offer_info['previous_price']
    owner_current_title_symbol = owner_offer_info['pattern_symbol']

    #  Відсортуємо патерни за значенням 'limit' у порядку спадання
    sorted_patterns = sorted(patterns.items(), key=lambda x: x[1]['limit'], reverse=True)

    #  Знайти позицію поточного патерну у відсортованому списку
    current_index = next((i for i, (pattern, _) in enumerate(sorted_patterns)
                          if pattern == owner_pattern_regular), None)

    #  Отримуємо наступний патерн після поточного
    if current_index is not None and current_index + 1 < len(sorted_patterns):
        next_pattern, next_data = sorted_patterns[current_index + 1]
        logger.info(f"Наступний патерн за спаданням ліміту: {next_data}") if test_mode_logs else None
    else:
        logger.СHANGE_PRICE("Поточний патерн залишається незмінним так як"
                            " є останнім у списку або невизначений.")
        return None

    next_data_symbol = next_data['symbol']
    next_data_patter_name = next_data['name']
    next_data_limit = next_data['limit']

    new_title = owner_title.replace(owner_current_title_symbol, next_data_symbol)
    logger.CHANGE_PRICE(f"Змінено патерн для {short_title} з {owner_pattern_name}({owner_current_title_symbol})"
                        f" лімітом {owner_pattern_limit}"
                        f" на {next_data_patter_name}({next_data_symbol}) лімітом {next_data_limit}"
                        f" так як {owner} знаходиться на позиції {owner_position}"
                        f" з ціною {RED}{owner_price}{RESET}")

    #  Оновлюємо інформацію про лот власника
    owner_offer_info['pattern_name'] = next_data_patter_name
    owner_offer_info['pattern_symbol'] = next_data_symbol
    owner_offer_info['pattern_regular'] = next_pattern
    owner_offer_info['limit'] = next_data_limit
    owner_offer_info['title'] = new_title

    return owner_offer_info, new_title


#
# def calculate_dynamic_coefficient(value):
#
#     if isinstance(value, float):
#         str_value = f"{value:.12f}".rstrip('0')  # Подання числа без зайвих нулів
#         decimal_part = str_value.split('.')[-1] if '.' in str_value else ''
#         num_decimal_places = len(decimal_part)
#     else:
#         num_decimal_places = 0
#
#     # Формуємо коефіцієнт із потрібною розрядністю
#     coefficient = f"0.{(num_decimal_places-1) * '0'}{display_price_change_value}"
#     return float(coefficient)



