import asyncio
import concurrent.futures
import decimal
import json
import math
import sys
import time
import os
import re
import zipfile

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from sqlalchemy.exc import IntegrityError
from urllib3 import Retry

from authorization.auth_token_from_sls import token_refresh_scheduler_direct, TokenManager

from data_base.models import OffersParameters
from data_base.connection import db  # Імпортуємо db екземпляр

from functions.load_config import get_config
from functions.logger_config import logger
from functions.util_functions import calculate_percent_difference

try:
    db.create_all_tables()
    logger.info("Таблиці бази даних перевірено/створено.")
except Exception as e:
    logger.error(f"Не вдалося створити таблиці: {e}")

class OfferProcessor:
    def __init__(self, offers_folder_path: str = 'source_offers', output_folder_name: str = 'updated_offers_xlsx'):
        self._env_path = Path(__file__).parent / "authorization" / ".env"
        load_dotenv(dotenv_path=self._env_path)
        self.config = get_config()
        self.token_manager = TokenManager()


        self.logger = logger
        if not self.config:
            self.logger.critical("Помилка завантаження конфігурації. Завершення роботи.")
            raise ValueError("Configuration could not be loaded.")

        self.offers_folder = Path(__file__).resolve().parent.joinpath(offers_folder_path)
        self.output_folder = Path(__file__).resolve().parent.joinpath(output_folder_name)
        self.red = "\033[31m"
        self.reset = "\033[0m"
        self.db = db
        self._session = None


        self._load_config_parameters()
        self._initialize_patterns()
        self._initialize_headers()
        self._initialize_requests_session()
        self._initialize_relations_ids()

    def _initialize_relations_ids(self):
        relations_ids_path = Path(__file__).resolve().parent.joinpath("utils/relations_ids.json")
        with open(relations_ids_path, "r", encoding="utf-8") as file:
            self.relations_ids = json.load(file)
        logger.info(f"Relations IDs loaded from {self.relations_ids}") if self.test_mode_logs else None

    def _load_config_parameters(self):
        """Завантажує всі параметри конфігурації в атрибути класу."""
        self.owner = self.config.get('owner')
        self.user_agent = self.config.get('user_agent')
        self.platform = self.config.get('platform')
        self.seller_id = str(self.config.get('seller_id'))
        self.delete_temp_folders = self.config.get('delete_temp_folders')
        self.test_mode_logs = self.config.get('test_mode_logs')
        self.ignore_words = [word.lower() for word in self.config.get('ignore_words')]
        self.position_for_change_pattern_if_owner_price_over_limit = self.config.get(
            'position_for_change_pattern_if_owner_price_over_limit')
        self.position_for_change_pattern_if_owner_price_under_limit = self.config.get(
            'position_for_change_pattern_if_owner_price_under_limit')
        self.ignore_competitors_for_asterisk = self.config.get('ignore_competitors_for_asterisk')
        self.ignore_competitors_for_other_patterns = self.config.get('ignore_competitors_for_other_patterns')
        self.default_limit = self.config.get("default_limit", 0)

        self.api_retries = self.config.get("api_retries", 3)
        self.api_retry_delay = self.config.get("api_retry_delay", 120)

        self.threshold_price_for_percentage_change = self.config.get("threshold_price_for_percentage_change")
        self.change_percents_before_threshold = self.config.get("change_percents_before_threshold")
        self.change_percents_after_threshold = self.config.get("change_percents_after_threshold")

        self.min_max_change_first_position = self.config.get('min_max_change_first_position')
        self.reduce_price_non_popular_item = self.config.get('reduce_price_non_popular_item')  # Виправлено self.
        self.max_limit_price_for_pull = self.config.get("max_limit_price_for_pull")
        self.config_minimal_purchase_qty =decimal.Decimal( self.config.get("config_minimal_purchase_qty"))

        self.max_difference_percent_to_reduce_the_price_between_first_and_second = (
            self.config.get("max_difference_percent_to_reduce_the_price_between_first_and_second"))
        self.min_difference_percent_to_reduce_the_price_between_first_and_second = (
            self.config.get("min_difference_percent_to_reduce_the_price_between_first_and_second"))

        self.threads_quantity = self.config.get('threads_quantity')
        self.delay_seconds_between_cycles = (self.config.get("delay_minutes_between_cycles") * 60)

        self.take_ignors_when_pulling_price = self.config.get('take_ignors_when_pulling_price')

    def _initialize_headers(self):
        self.base_headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            "origin": "https://www.g2g.com",
            "priority": "u=1, i",
            "referer": "https://www.g2g.com/",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": self.platform,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": self.user_agent
        }

        self.s3_headers = self.base_headers.copy()
        self.s3_headers["Accept-Encoding"] = "gzip, deflate, br, zstd"

    def auth_headers(self):
        headers_with_auth = self.base_headers.copy()
        headers_with_auth["authorization"] = self.token_manager.get_token()
        return headers_with_auth


    def _initialize_requests_session(self):

        self._session = requests.Session()

        # Створення об'єкта Retry
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])

        # Створення об'єкта HTTPAdapter з налаштованим пулом і повторними спробами
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)

        # Монтування одного адаптера для обох протоколів
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)


    def _initialize_patterns(self):
        """Ініціалізує словник патернів з лімітами з конфігурації."""
        top_minimal_values = self.config.get("top_minimal")

        if not top_minimal_values:
            self.logger.critical(
                "Помилка завантаження конфігурації лімітів 'top_minimal'. Використовуємо default_limit.")
            top_minimal_values = {}  # Забезпечуємо, що це словник, навіть якщо конфігурація відсутня

        # Динамічне присвоєння лімітів для кожного патерну
        self.patterns = {
            r"\*([^\*]+)\*": {'limit': top_minimal_values.get("asterisk", self.default_limit), 'name': "asterisk",
                              'symbol': '*'},
            r"\+([^+]+)\+": {'limit': top_minimal_values.get("plus", self.default_limit), 'name': "plus",
                             'symbol': '+'},
            r"\#([^\#]+)\#": {'limit': top_minimal_values.get("hash", self.default_limit), 'name': "hash",
                              'symbol': '#'},
            r"★([^★]+)★": {'limit': top_minimal_values.get("star", self.default_limit), 'name': "star", 'symbol': '★'},
            r"=([^=]+)=": {'limit': top_minimal_values.get("equality", self.default_limit), 'name': "equality",
                           'symbol': '='},
            r"~([^~]+)~": {'limit': top_minimal_values.get("tilde", self.default_limit), 'name': "tilde",
                           'symbol': '~'},
            r"\`([^\`]+)\`": {'limit': top_minimal_values.get("backtick", self.default_limit), 'name': "backtick",
                              'symbol': '`'},
            r"\|([^\|]+)\|": {'limit': top_minimal_values.get("vertical_bar", self.default_limit),
                              'name': "vertical_bar", 'symbol': '|'},
            r"\!\!([^\!]+)\!\!": {'limit': top_minimal_values.get("double_exclamation", self.default_limit),
                                  'name': "double_exclamation", 'symbol': '!!'},
            r"__([^_]+)__": {'limit': top_minimal_values.get("double_underscore", self.default_limit),
                             'name': "double_underscore", 'symbol': '__'},
            r"\^([^\^]+)\^": {'limit': top_minimal_values.get("caret", self.default_limit), 'name': "caret",
                              'symbol': '^'},
            r"\$([^\$]+)\$": {'limit': top_minimal_values.get("dollar", self.default_limit), 'name': "dollar",
                              'symbol': '$'},
            r"\&([^\&]+)\&": {'limit': top_minimal_values.get("ampersand", self.default_limit), 'name': "ampersand",
                              'symbol': '&'},
            r"%%([^%]+)%%": {'limit': top_minimal_values.get("double_percent", self.default_limit),
                             'name': "double_percent", 'symbol': '%%'},
            r"°([^°]+)°": {'limit': top_minimal_values.get("degree", self.default_limit), 'name': "degree",
                           'symbol': '°'},
            r"□([^□]+)□": {'limit': top_minimal_values.get("square", self.default_limit), 'name': "square",
                           'symbol': '□'},
            r"●([^●]+)●": {'limit': top_minimal_values.get("black_circle", self.default_limit), 'name': "black_circle",
                           'symbol': '●'},
            r"▼([^▼]+)▼": {'limit': top_minimal_values.get("down_arrow", self.default_limit), 'name': "down_arrow",
                           'symbol': '▼'},
            r"◊([^◊]+)◊": {'limit': top_minimal_values.get("diamond", self.default_limit), 'name': "diamond",
                           'symbol': '◊'},
            r"↑([^↑]+)↑": {'limit': top_minimal_values.get("up_arrow", self.default_limit), 'name': "up_arrow",
                           'symbol': '↑'},
            r"☼([^☼]+)☼": {'limit': top_minimal_values.get("sun", self.default_limit), 'name': "sun", 'symbol': '☼'},
            r"∆([^∆]+)∆": {'limit': top_minimal_values.get("triangle", self.default_limit), 'name': "triangle",
                           'symbol': '∆'},
            r"⇔([^⇔]+)⇔": {'limit': top_minimal_values.get("double_arrow", self.default_limit), 'name': "double_arrow",
                           'symbol': '⇔'},
            r"①([^①]+)①": {'limit': top_minimal_values.get("circled_one", self.default_limit), 'name': "circled_one",
                           'symbol': '①'},
            r"╬([^╬]+)╬": {'limit': top_minimal_values.get("double_cross", self.default_limit), 'name': "double_cross",
                           'symbol': '╬'},
            r"▣([^▣]+)▣": {'limit': top_minimal_values.get("square_with_round_corners", self.default_limit),
                           'name': "square_with_round_corners", 'symbol': '▣'},
            r"◌([^◌]+)◌": {'limit': top_minimal_values.get("dotted_circle", self.default_limit),
                           'name': "dotted_circle", 'symbol': '◌'},
            r"❖([^❖]+)❖": {'limit': top_minimal_values.get("diamond_with_dot", self.default_limit),
                           'name': "diamond_with_dot", 'symbol': '❖'},
            r"❊([^❊]+)❊": {'limit': top_minimal_values.get("flower", self.default_limit), 'name': "flower",
                           'symbol': '❊'},
            r"✦([^✦]+)✦": {'limit': top_minimal_values.get("four_pointed_star", self.default_limit),
                           'name': "four_pointed_star", 'symbol': '✦'},
            r"♡([^♡]+)♡": {'limit': top_minimal_values.get("heart", self.default_limit), 'name': "heart",
                           'symbol': '♡'},
            r"☆([^☆]+)☆": {'limit': top_minimal_values.get("white_star", self.default_limit), 'name': "white_star",
                           'symbol': '☆'},
            r"◩([^◩]+)◩": {'limit': top_minimal_values.get("square_with_triangle", self.default_limit),
                           'name': "square_with_triangle", 'symbol': '◩'},
            r"◐([^◐]+)◐": {'limit': top_minimal_values.get("half_circle_right", self.default_limit),
                           'name': "half_circle_right", 'symbol': '◐'},
            r"◆([^◆]+)◆": {'limit': top_minimal_values.get("black_diamond", self.default_limit),
                           'name': "black_diamond", 'symbol': '◆'},
            r"◇([^◇]+)◇": {'limit': top_minimal_values.get("white_diamond", self.default_limit),
                           'name': "white_diamond", 'symbol': '◇'},
            r"◈([^◈]+)◈": {'limit': top_minimal_values.get("diamond_with_plus", self.default_limit),
                           'name': "diamond_with_plus", 'symbol': '◈'},
            r"◉([^◉]+)◉": {'limit': top_minimal_values.get("bullseye", self.default_limit), 'name': "bullseye",
                           'symbol': '◉'},
            r"♔([^♔]+)♔": {'limit': top_minimal_values.get("chess_king", self.default_limit), 'name': "chess_king",
                           'symbol': '♔'},
            r"♕([^♕]+)♕": {'limit': top_minimal_values.get("chess_queen", self.default_limit), 'name': "chess_queen",
                           'symbol': '♕'},
            r"♖([^♖]+)♖": {'limit': top_minimal_values.get("chess_rook", self.default_limit), 'name': "chess_rook",
                           'symbol': '♖'},
            r"♗([^♗]+)♗": {'limit': top_minimal_values.get("chess_bishop", self.default_limit), 'name': "chess_bishop",
                           'symbol': '♗'},
            r"♘([^♘]+)♘": {'limit': top_minimal_values.get("chess_knight", self.default_limit), 'name': "chess_knight",
                           'symbol': '♘'},
            r"♞([^♞]+)♞": {'limit': top_minimal_values.get("chess_knight_black", self.default_limit),
                           'name': "chess_knight_black", 'symbol': '♞'},
            r"♙([^♙]+)♙": {'limit': top_minimal_values.get("chess_pawn", self.default_limit), 'name': "chess_pawn",
                           'symbol': '♙'},
            r"♚([^♚]+)♚": {'limit': top_minimal_values.get("chess_king_black", self.default_limit),
                           'name': "chess_king_black", 'symbol': '♚'},
            r"♛([^♛]+)♛": {'limit': top_minimal_values.get("chess_queen_black", self.default_limit),
                           'name': "chess_queen_black", 'symbol': '♛'},
            r"♜([^♜]+)♜": {'limit': top_minimal_values.get("chess_rook_black", self.default_limit),
                           'name': "chess_rook_black", 'symbol': '♜'},
            r"♝([^♝]+)♝": {'limit': top_minimal_values.get("chess_bishop_black", self.default_limit),
                           'name': "chess_bishop_black", 'symbol': '♝'},
            r"❶([^❶]+)❶": {'limit': top_minimal_values.get("ball_number_one", self.default_limit),
                           'name': "ball_number_one", 'symbol': '❶'}
        }

    def _receiving_data(self, concurrent_json_info: Dict, owner_offer_info: Optional[Dict]) -> Optional[Dict]:
        """Парсить інформацію про конкурентів з API відповіді."""
        try:
            json_data = concurrent_json_info["payload"]
            brand_id = json_data.get("brand_id")
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
                "q": owner_offer_info.get('short_title'),
                "filter_attr": filter_attr,
            }
            return params

        except KeyError as e:
            self.logger.error(f"Не вдалося розпарсити дані: відсутній ключ {e} у відповіді API.")
            return None
        except Exception as e:
            self.logger.error(f"Неочікувана помилка при парсингу даних: {e}")
            return None

    def get_params_from_api(self, owner_offer_info: Dict) -> Optional[dict]:
        """Отримує інформацію про товар, включаючи нову ціну та назву."""
        offer_id = owner_offer_info.get('offer_id')

        params = {
            "currency": "USD",
            "country": "UA",
            "include_out_of_stock": "1",
            "include_inactive": "1",
        }
        url = f"https://sls.g2g.com/offer/{offer_id}"

        response = self.fetch_from_api_with_retry(url=url,
                                                  headers=self.base_headers,
                                                  payload=params)
        if response is None:
            self.logger.error(f"[{offer_id}] Не вдалося підключитись до сервера для offer ID {offer_id}.")
            return None

        concurrent_json_info = response.json()
        api_params = self._receiving_data(concurrent_json_info, owner_offer_info)

        if api_params is None:
            self.logger.error(f"[{offer_id}] Не вдалося отримати параметри для запиту списку товарів.")
            return None

        return api_params

    def find_pattern(self, owner_title: str) -> Optional[Dict[str, Any]]:
        owner_offer_info = {}
        for pattern, pattern_limit in self.patterns.items():
            match = re.findall(pattern, owner_title)
            try:
                if match:
                    result = match[0]
                    offer_type = self.define_offer_type(result)

                    owner_offer_info['pattern_name'] = pattern_limit['name']
                    owner_offer_info['limit'] = pattern_limit['limit']
                    owner_offer_info['pattern_symbol'] = pattern_limit['symbol']
                    owner_offer_info['pattern_regular'] = pattern

                    owner_offer_info['title'] = owner_title
                    owner_offer_info['short_title'] = result.strip()
                    owner_offer_info['offer_type'] = offer_type

                    return owner_offer_info
            except ValueError:
                self.logger.critical(f"ValueError при обробці {owner_title}. Паттерн: {pattern}")
                continue
        else:
            return None

    def define_offer_type(self, title: str) -> str:
        exist_ignore_in_owner_title = any(word in title.lower() for word in self.ignore_words)
        offer_type = 'schematic_type' if exist_ignore_in_owner_title else 'item_type'
        return offer_type

    def get_params_from_db(self, owner_offer_info: Dict) -> Optional[dict]:
        offer_id = owner_offer_info.get('offer_id')
        try:
            with self.db.get_session() as session:
                parameter = session.query(OffersParameters).filter(OffersParameters.offer_id == offer_id).first()
                if parameter:
                    parameter_dict = {
                        "seo_term": parameter.seo_term,
                        "region_id": parameter.region_id,
                        "q": owner_offer_info.get('short_title'),
                        "filter_attr": parameter.filter_attribute,
                    }
                    self.logger.info(f"[{offer_id}]"
                                     f" Параметри отримано з бази даних.") if self.test_mode_logs else None
                    return parameter_dict
                else:
                    self.logger.info(f"[{offer_id}]"
                                     f" Параметри не знайдено в базі даних.") if self.test_mode_logs else None
                    return None
        except Exception as e:
            self.logger.error(f"[{offer_id}] Помилка при отриманні параметрів з БД: {e}", exc_info=True)
            return None

    def record_params_to_db(self, offer_id: str, params: dict):
        try:
            with self.db.get_session() as session:
                existing_param = session.query(OffersParameters).filter(OffersParameters.offer_id == offer_id).first()
                if existing_param:
                    existing_param.seo_term = params.get('seo_term')
                    existing_param.region_id = params.get('region_id')
                    existing_param.filter_attribute = params.get('filter_attr')
                    self.logger.info(f"[{offer_id}] Параметри успішно оновлено в БД.") if self.test_mode_logs else None
                else:
                    new_parameter = OffersParameters(
                        offer_id=offer_id,
                        seo_term=params.get('seo_term'),
                        region_id=params.get('region_id'),
                        filter_attribute=params.get('filter_attr'),
                    )
                    session.add(new_parameter)
                    self.logger.info(f"[{offer_id}]"
                                     f" Параметри успішно записано до БД.") if self.test_mode_logs else None
                session.commit()
        except IntegrityError as e:
            self.logger.warning(f"[{offer_id}] Запис вже існує або порушено унікальний ключ: {e}")
            session.rollback()
        except Exception as e:
            self.logger.error(f"[{offer_id}] Помилка при записі/оновленні параметрів до БД: {e}", exc_info=True)
            session.rollback()

    def get_list_competitors(self, params: Dict, offer_id: str) -> Optional[Dict]:
        params["page_size"] = 48
        params["sort"] = "lowest_price"
        params["currency"] = 'USD'
        params["country"] = "UA"
        base_url = "https://sls.g2g.com/offer/search/"


        response = self.fetch_from_api_with_retry(url=base_url,
                                                  headers=self.auth_headers(),
                                                  payload=params)
        if response is None:
            self.logger.critical(f"[{offer_id}] Немає відповіді сервера при отриманні списку конкурентів.")
            return None
        if response.status_code == 200:
            return response.json()
        else:
            self.logger.error(f"[{offer_id}] Помилка при отриманні списку конкурентів: {response.status_code}")
            return None

    def price_study(self, data: Dict, owner_offer_info: Dict):
        owner_offer_id = owner_offer_info['offer_id']
        owner_short_title = owner_offer_info['short_title']
        owner_offer_type = owner_offer_info['offer_type']
        owner_title = owner_offer_info['title']
        owner_limit = owner_offer_info['limit']
        owner_pattern_name = owner_offer_info['pattern_name']
        owner_ignore_competitors_list = self.ignore_competitors_for_asterisk if owner_pattern_name == 'asterisk' \
            else self.ignore_competitors_for_other_patterns

        try:
            payload = data.get("payload", {})
            results = payload.get("results", [])

            competitors = {'owner_offer_info': owner_offer_info}
            position = 0
            sellers_usernames_list = []
            append_stop_flag = False
            owner_exist_flag = False
            new_title = None
            new_price = None

            for index, result in enumerate(results, start=1):
                username = result['username']
                competitor_unit_price = round(float(result["unit_price"]), 6)
                competitor_display_price = float(result["display_price"])
                competitor_title = result["title"]
                competitor_offer_id = result["offer_id"]
                competitor_offer_type = self.define_offer_type(competitor_title)
                competitor_currency = result["offer_currency"]

                if competitor_offer_type != owner_offer_type:
                    continue

                if username == self.owner and owner_offer_id != competitor_offer_id:
                    self.logger.warning(
                        f"[{owner_offer_id}] Пропускаємо недійсний ID для: {username}, поточний offer_id: {owner_offer_id},"
                        f" назва: {owner_title} "
                        f" але {self.owner} має інший лот competitor_offer_id: {competitor_offer_id}"
                        f" owner_offer_type: {owner_offer_type}, competitor_offer_type: {competitor_offer_type}"
                        f" competitor_title: {competitor_title}") if self.test_mode_logs else None
                    continue

                position += 1

                if append_stop_flag is False and username not in owner_ignore_competitors_list \
                        and competitor_unit_price < owner_limit:
                    sellers_usernames_list.append(username)

                if username == self.owner and owner_offer_id == competitor_offer_id:
                    competitors['owner_offer_info']['previous_price'] = competitor_unit_price
                    competitors['owner_offer_info']['position'] = position
                    owner_exist_flag = True
                    append_stop_flag = True

                if competitor_currency != 'USD':
                    previous_competitor_unit_price = competitor_unit_price
                    competitor_unit_price = competitor_display_price

                    self.logger.warning(
                        f"[{owner_offer_id}] У продавця {username} валюта {self.red}{competitor_currency}{self.reset} не відповідає USD. "
                        f"Змінено параметр ціни з unit_price {self.red}{previous_competitor_unit_price}{self.reset}"
                        f" на display_price {self.red}{competitor_unit_price}{self.reset}"
                        f" у товарі {competitor_title}") if self.test_mode_logs else None

                competitors[position] = {'username': username,
                                         'unit_price': competitor_unit_price,
                                         'title': competitor_title,
                                         'offer_type': competitor_offer_type
                                         }

            if len(competitors) < 2:
                self.logger.warning(f"[{owner_offer_id}] Список продавців порожній на товар {owner_short_title}")
                return None, None

            if owner_exist_flag is False:
                self.logger.warning(f"{self.owner} знаходиться на позиції вище 100 на товар {owner_short_title}  id={owner_offer_id}")
                return None, None

            owner_info = competitors['owner_offer_info']
            owner_position = owner_info['position']
            owner_price = owner_info['previous_price']
            self.logger.info(
                f"[{owner_offer_id}] sellers_usernames_list: {sellers_usernames_list}") if self.test_mode_logs else None

            try:
                if (owner_pattern_name != 'asterisk' and owner_price > owner_limit
                        and owner_position >= self.position_for_change_pattern_if_owner_price_over_limit
                        and len(sellers_usernames_list) >= self.position_for_change_pattern_if_owner_price_over_limit):
                    self.logger.info(
                        f"[{owner_offer_id}] {self.owner} на позиції {owner_position} з ціною {owner_price} вищою за ліміт {owner_limit}"
                        f" та продавці {[seller for seller in sellers_usernames_list if seller != self.owner]}"
                        f" мають ціну нижчу за ліміт {owner_limit}")
                    self.logger.info(f'[{owner_offer_id}] Переходимо до зміни патерну та ліміту.')
                    self.logger.info(
                        f"[{owner_offer_id}] previous_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None
                    new_owner_info, new_title = self.define_new_title_and_owner_info(owner_info)
                    competitors['owner_offer_info'] = new_owner_info
                    self.logger.info(
                        f"[{owner_offer_id}] new_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None

                if (owner_pattern_name != 'asterisk' and owner_price < owner_limit
                        and owner_position >= self.position_for_change_pattern_if_owner_price_under_limit
                        and len(sellers_usernames_list) >= self.position_for_change_pattern_if_owner_price_under_limit):
                    self.logger.info(
                        f"[{owner_offer_id}] {self.owner} на позиції {owner_position} з ціною {owner_price} нижчою за ліміт {owner_limit}"
                        f" та продавці {[seller for seller in sellers_usernames_list if seller != self.owner]}"
                        f" мають ціну нижчу за ліміт {owner_limit}")
                    self.logger.info(f'[{owner_offer_id}] Переходимо до зміни патерну та ліміту.')
                    self.logger.info(
                        f"[{owner_offer_id}] previous_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None
                    new_owner_info, new_title = self.define_new_title_and_owner_info(owner_info)
                    competitors['owner_offer_info'] = new_owner_info
                    self.logger.info(
                        f"[{owner_offer_id}] new_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None

            except Exception as e:
                self.logger.critical(f"[{owner_offer_id}] Помилка при зміні патерну: {e}", exc_info=True)
                return None, None

            new_price_result = self.general_patterns(competitors)

            if isinstance(new_price_result, str):
                self.logger.info(f"[{owner_offer_id}] {new_price_result}") if self.test_mode_logs else None
                return None, new_title
            elif isinstance(new_price_result, (int, float)):
                new_price = new_price_result
                return new_price, new_title
            elif isinstance(new_price_result, dict):
                if "warning" in new_price_result:
                    warning_message = new_price_result["warning"]
                    self.logger.critical(f"[{owner_offer_id}] {warning_message}")
                    return None, None
                elif "critical" in new_price_result:
                    critical_message = new_price_result["critical"]
                    self.logger.critical(f"[{owner_offer_id}] {critical_message}")
                    return None, None
            return new_price, new_title
        except Exception as e:
            self.logger.critical(f"[{owner_offer_id}] Помилка при обробці пропозиції: {e}", exc_info=True)

    def define_new_title_and_owner_info(self,owner_offer_info):
        owner_title = owner_offer_info['title']
        owner_position = owner_offer_info['position']
        short_title = owner_offer_info['short_title']
        owner_pattern_regular = owner_offer_info['pattern_regular']
        owner_pattern_name = owner_offer_info['pattern_name']
        owner_pattern_limit = owner_offer_info['limit']
        owner_price = owner_offer_info['previous_price']
        owner_current_title_symbol = owner_offer_info['pattern_symbol']

        #  Відсортуємо патерни за значенням 'limit' у порядку спадання
        sorted_patterns = sorted(self.patterns.items(), key=lambda x: x[1]['limit'], reverse=True)

        #  Знайти позицію поточного патерну у відсортованому списку
        current_index = next((i for i, (pattern, _) in enumerate(sorted_patterns)
                              if pattern == owner_pattern_regular), None)

        #  Отримуємо наступний патерн після поточного
        if current_index is not None and current_index + 1 < len(sorted_patterns):
            next_pattern, next_data = sorted_patterns[current_index + 1]
            logger.info(f"Наступний патерн за спаданням ліміту: {next_data}") if self.test_mode_logs else None
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
                            f" так як {self.owner} знаходиться на позиції {owner_position}"
                            f" з ціною {self.red}{owner_price}{self.reset}")

        #  Оновлюємо інформацію про лот власника
        owner_offer_info['pattern_name'] = next_data_patter_name
        owner_offer_info['pattern_symbol'] = next_data_symbol
        owner_offer_info['pattern_regular'] = next_pattern
        owner_offer_info['limit'] = next_data_limit
        owner_offer_info['title'] = new_title

        return owner_offer_info, new_title

    def general_patterns(self,competitors):
        owner_offer_info = competitors.pop('owner_offer_info')
        logger.info(f'owner_offer_info: {owner_offer_info}') if self.test_mode_logs else None
        logger.info(f'competitors: {competitors}') if self.test_mode_logs else None

        previous_price = owner_offer_info['previous_price']
        short_title = owner_offer_info['short_title']
        pattern_name = owner_offer_info['pattern_name']
        owner_position = owner_offer_info['position']

        # Визначаємо список ігнорованих конкурентів для поточного патерну
        ignore_competitors = self.ignore_competitors_for_asterisk if pattern_name == 'asterisk' \
            else self.ignore_competitors_for_other_patterns
        limit = owner_offer_info['limit']

        # Визначаємо індикатор підтягування отримуючи булеве значення
        pull_indicator, position_competitor_for_pull = self.get_pull_indicator(owner_position, competitors, ignore_competitors)

        logger.info(f"pull_indicator: {pull_indicator},"
                    f" position_competitor_for_pull: {position_competitor_for_pull}")

        logger.warning(f" Для патерну {pattern_name} на знайдено значення ліміту."
                       f" Наразі встановлено стандартне значення {limit}"
                       f"Перевірте конфігураційний файл.") if limit == 0 else None
        logger.info(f"pattern_name: {pattern_name}") if self.test_mode_logs else None
        logger.info(f"ignore_competitors: {ignore_competitors}") if self.test_mode_logs else None

        try:
            for position, competitor in competitors.items():
                user_name = competitor['username']
                unit_price = competitor['unit_price']

                # Підбираємо коефіцієнт для перебиття поточного конкурента
                change_price_coefficient = self.change_percents_before_threshold if \
                    unit_price <= self.threshold_price_for_percentage_change else self.change_percents_after_threshold

                # Перевіряємо конкурента на присутність в списку ігнорування
                if user_name in ignore_competitors:
                    logger.CHANGE_PRICE(f" Ігноруємо {user_name} на позиції {position} по предмету {short_title}"
                                        f" з ціною {self.red}{unit_price}{self.reset}") if self.test_mode_logs else None
                    continue

                # Перевіряємо умови для зниження ціни на непопулярний товар
                elif user_name == self.owner and position == 1 and len(competitors) == 1:
                    return (f"{self.owner} єдиний продавець товару {short_title} з ціною"
                            f" {self.red}{unit_price}{self.reset} ")
                #
                #     if unit_price < limit:
                #         return (f"{owner} єдиний продавець непопулярного товару."
                #                 f" Ціна менша за встановленний ліміт {limit}"
                #                 f" знижена не буде і становить {RED}{unit_price}{RESET} ")
                #     # Розраховуємо знижену ціну
                #     new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                #     if new_price < limit:
                #         return (f"{owner} єдиний продавець непопулярного товару."
                #                 f" Знижена ціна менша за встановленний ліміт {limit}."
                #                 f"Змінена не буде і становить {RED}{unit_price}{RESET} ")
                #     logger.CHANGE_PRICE(f"{owner} єдиний продавець непопулярного товару."
                #                         f"Ціна знижена на {reduce_price_non_popular_item}%"
                #                         f" і становить {RED}{new_price}{RESET}")
                #     return new_price

                # Перевіряємо на можливість підтягування ціни
                elif pull_indicator is True:
                    if owner_position == max(competitors.keys()):
                        return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                                f" з ціною {self.red}{unit_price}{self.reset}"
                                f" так як має останню позицію {owner_position}")

                    if unit_price > self.max_limit_price_for_pull:
                        return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                                f" з ціною {self.red}{unit_price}{self.reset}"
                                f" вищою за встановленний ліміт {self.max_limit_price_for_pull} $ для підтягування")

                    pull_competitor_price = competitors[position_competitor_for_pull]['unit_price']
                    pull_competitor_username = competitors[position_competitor_for_pull]['username']
                    price_difference_percent = round(calculate_percent_difference(unit_price,
                                                                                  pull_competitor_price), 2)
                    potential_new_price = pull_competitor_price * (1 - (self.min_max_change_first_position / 100))
                    logger.info(f"unit_price: {unit_price}"
                                f"pull_competitor_username: {pull_competitor_username},"
                                f" pull_competitor_price: {pull_competitor_price},"
                                f"price_difference_percent: {price_difference_percent}"
                                f"potential_new_price: {potential_new_price}") if self.test_mode_logs else None

                    if potential_new_price > self.max_limit_price_for_pull:
                        return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                                f" з ціною {self.red}{unit_price}{self.reset}"
                                f" т.я потенційна ціна після підтягування вища"
                                f" за встановленний ліміт {self.max_limit_price_for_pull} $ для підтягування")

                    elif ((
                                  self.min_difference_percent_to_reduce_the_price_between_first_and_second <= price_difference_percent
                                  <= self.max_difference_percent_to_reduce_the_price_between_first_and_second)
                          and potential_new_price < self.max_limit_price_for_pull):

                        new_price = round(potential_new_price, 6)

                        logger.CHANGE_PRICE(f"Ціну підтягнуто з {position} позиції"
                                            f" з {self.red}{unit_price}{self.reset} до {self.red}{new_price}{self.reset}."
                                            f" Різниця з позицією {position_competitor_for_pull}"
                                            f" продавця {pull_competitor_username} становила {price_difference_percent} %"
                                            f" для ціни {self.red}{pull_competitor_price}{self.reset}") if self.test_mode_logs else None
                        return new_price

                    else:
                        return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                                f" з ціною {self.red}{unit_price}{self.reset}."
                                f" Ціну не підтягнуто, різниця з позицією {position_competitor_for_pull}"
                                f" становить {price_difference_percent} % для ціни {self.red}{pull_competitor_price}{self.reset}")

                # Якщо знаходимо власника після попередніх фільтрів, то виходимо.
                elif user_name == self.owner and position != 1:
                    return (f"{user_name} залишається на позиції {position} по предмету"
                            f" {short_title} з ціною {self.red}{unit_price}{self.reset}")

                elif unit_price < limit and pattern_name != 'asterisk':
                    continue

                logger.info(f"Знайдено конкурента {user_name}"
                            f" на позиції {position} з ціною {unit_price}") if self.test_mode_logs else None

                new_price = unit_price * (1 - (change_price_coefficient / 100))
                new_price = round(new_price, 6)
                logger.CHANGE_PRICE(f"Перебиваємо {user_name} позиції {position},  на товар {short_title}"
                                    f" з {self.red}{previous_price}{self.reset}"
                                    f" на {self.red}{new_price}{self.reset}") if self.test_mode_logs else None

                logger.info(
                    f"Ціна конкурента__{self.red}{unit_price}{self.reset}, відсоток перебиття__{change_price_coefficient} %"
                    f" Змінена ціна__{self.red}{new_price}{self.reset}") if self.test_mode_logs else None

                return new_price
            else:
                return f"Немає конкурентів на товар {short_title}"
        except Exception as e:
            return {'critical': f"Помилка у функції розрахунку ціни: {e} ціну не змінено"
                                f" на товар {short_title} з id {owner_offer_info['offer_id']} "}


    # def get_pull_indicator(self, owner_position, competitors, ignored_competitors):
    #     self.logger.info(f"competitors__{competitors}")
    #     self.logger.info(f"ignored_competitors__{ignored_competitors}")
    #     if  self.pull_if_ignore_after_me:
    #         self.logger.info(f"pull_if_ignore_after_me__{self.pull_if_ignore_after_me}")
    #
    #         if owner_position == 1:
    #             self.logger.info(f"owner_position__{owner_position},"
    #                              f" кількість конкурентів__{len(competitors)}")
    #             return True, 2 if len(competitors) >= 2 else False, None
    #         else:
    #             self.logger.info(f"pull_if_ignore_before_me__{self.pull_if_ignore_before_me}")
    #             if self.pull_if_ignore_before_me:
    #                 self.logger.info(f"owner_position__{owner_position},"
    #                                  f" кількість конкурентів__{len(competitors)}")
    #                 return True, owner_position + 1
    #
    #
    #     else:
    #         for pos, competitor in competitors.items():
    #             pos_competitor_after_owner_not_in_ignore = None
    #             if pos > owner_position and competitor['username'] not in ignored_competitors:
    #                 pos_competitor_after_owner_not_in_ignore = pos
    #                 break
    #         return (False, None if pos_competitor_after_owner_not_in_ignore is None else True,
    #                 pos_competitor_after_owner_not_in_ignore)
    #
    #
    #
    #
    #     self.logger.info(f"competitors_before_owner: {competitors_before_owner}") if self.test_mode_logs else None
    #
    #     for competitor in competitors_before_owner:
    #         if competitor not in ignored_competitors:
    #             return False
    #
    #     return True

    def get_pull_indicator(self, owner_position, competitors, ignored_competitors):
        self.logger.debug(f"Calling get_pull_indicator with owner_position: {owner_position}, "
                          f"competitors: {len(competitors)}, ignored: {len(ignored_competitors)}")

        if self.take_ignors_when_pulling_price:
            pos_competitor_after_owner_not_in_ignore = None

            for pos, competitor in competitors.items():
                if pos > owner_position and competitor['username'] not in ignored_competitors:
                    pos_competitor_after_owner_not_in_ignore = pos
                    break

            if pos_competitor_after_owner_not_in_ignore is None:
                self.logger.info(
                    f"Return from condition: owner_position == {owner_position}"
                    f" and take_ignors_when_pulling_price is on"
                    f"pos_competitor_after_owner_not_in_ignore is None")
                return False, None
            else:
                self.logger.info(
                    f"Return from condition: owner_position == 1"
                    " and take_ignors_when_pulling_price is on"
                    f"pos_competitor_after_owner_not_in_ignore__{pos_competitor_after_owner_not_in_ignore}")
                return True, pos_competitor_after_owner_not_in_ignore

        if owner_position == 1:
            self.logger.info(f"owner_position __ {owner_position},"
                             f"take_ignors_when_pulling_price is {self.take_ignors_when_pulling_price}")
            return (True, 2) if len(competitors) >= 2 else (False, None)
        not_ignored_competitors_before_owner = [competitor['username'] for pos, competitor in competitors.items()
                                                if (pos < owner_position and competitor[
                'username'] not in ignored_competitors)]
        self.logger(f"not_ignored_competitors_before_owner__{not_ignored_competitors_before_owner}")

        if not not_ignored_competitors_before_owner:
            self.logger.info(f"owner_position __ {owner_position},"
                             f"take_ignors_when_pulling_price is {self.take_ignors_when_pulling_price}")
            return False, None
        else:
            self.logger.info(f"owner_position __ {owner_position},"
                             f"take_ignors_when_pulling_price is {self.take_ignors_when_pulling_price}")
            return True, owner_position + 1

    def _process_single_offer(self, original_index: int, row_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Допоміжний метод для обробки одного товару в окремому потоці.
        Виконує API-запити, запис у БД та розрахунок ціни/назви.
        Повертає словник з даними для оновлення Excel.
        """
        offer_id = row_data['Offer ID']
        unit_price = row_data['Unit Price']
        title = row_data['Title']
        table_min_purchase_qty = row_data['Min. Purchase Qty']

        self.logger.info(f"[{offer_id}] Обробка пропозиції."
                         f" Оригінальна ціна: {unit_price},"
                         f" Назва: {title}") if self.test_mode_logs else None

        owner_offer_info = self.find_pattern(title)
        if not owner_offer_info:
            self.logger.warning(f"[{offer_id}] Помилка: Не знайдено патерн для товару {title}.")
            return None

        owner_offer_info.update({'offer_id': offer_id, 'unit_price': unit_price})

        params = self.get_params_from_db(owner_offer_info)

        if params:
            self.logger.info(f"[{offer_id}] Параметри отримано з БД.") if self.test_mode_logs else None
        else:
            params = self.get_params_from_api(owner_offer_info)
            if not params:
                self.logger.critical(f"[{offer_id}] Не вдалося отримати параметри з API.")
                return None
            self.logger.info(f"[{offer_id}] Параметри успішно отримано з API.") if self.test_mode_logs else None

            self.record_params_to_db(offer_id, params)

        competitors_list = self.get_list_competitors(params, offer_id)
        if competitors_list is None:
            self.logger.critical(
                f"[{offer_id}] Не можемо отримати список продавців для товару {owner_offer_info['short_title']}.")
            return None

            # 🔁 Перевірка: чи є поточний offer_id в списку results
        offer_ids = [item.get("offer_id") for item in competitors_list.get("payload", {}).get("results", [])]
        if offer_id not in offer_ids:
            self.logger.warning(
                f"[{offer_id}] Параметри у базі неактуальні. Отримуємо нові параметри з API.") if self.test_mode_logs else None
            params = self.get_params_from_api(owner_offer_info)
            logger.info(f"Api params: {params}") if self.test_mode_logs else None
            if not params:
                self.logger.critical(f"[{offer_id}] Повторно не вдалося отримати параметри з API.")
                return None
            self.record_params_to_db(offer_id, params)
            competitors_list = self.get_list_competitors(params, offer_id)
            if competitors_list is None:
                self.logger.critical(f"[{offer_id}] Повторно не вдалося отримати список конкурентів.")
                return None

        new_price, new_title = self.price_study(competitors_list, owner_offer_info)

        self.logger.info(f"[{offer_id}] Отримано з price_study:"
                         f" Нова ціна = {new_price},"
                         f" Нова назва = {new_title}") if self.test_mode_logs else None

        return {
            'original_index': original_index,
            'offer_id': offer_id,
            'original_unit_price': unit_price,
            'original_title': title,
            'table_min_purchase_qty': table_min_purchase_qty,
            'new_price': new_price,
            'new_title': new_title,
        }

    def fetch_from_api_with_retry(self,
                                  url,
                                  headers,
                                  payload=None,
                                  files=None,
                                  data=None,
                                  http_method="GET",
                                  request_name=None,
                                  api_retries=None,
                                  api_retry_delay=None):
        logger.info(f"headers: {headers}") if self.test_mode_logs else None
        """
        Синхронна функція для виконання HTTP-запитів з повторними спробами

        :param session: Об'єкт requests.Session
        :param url: URL для запиту
        :param headers: Заголовки запиту
        :param payload: Тіло запиту
        :param http_method: HTTP метод
        :param max_retries: Максимальна кількість спроб
        :param delay: Затримка між спробами (секунди)
        :return: Кортеж (результат, статус код) або (None, None) при помилці
        """
        api_retries = self.api_retries if api_retries is None else api_retries
        api_retry_delay = self.api_retry_delay if api_retry_delay is None else api_retry_delay
        request_headers = headers.copy()
        for attempt in range(api_retries):
            try:
                response = self._session.request(
                    method=http_method,
                    url=url,
                    params=payload if http_method in ["GET", "DELETE"] else None,
                    files=files,
                    data=data,
                    headers=request_headers,
                    json=payload if http_method in ["POST", "PUT", "PATCH"] else None,
                    timeout=30
                )
                logger.info(f"response.status_code: {response.status_code}") if self.test_mode_logs else None

                if response.status_code == 200:
                    time.sleep(0.1)
                    return response

                elif response.status_code == 204 and request_name == 's3_upload':  # Обробка 204 No Content окремо
                    self.logger.info(f"Успішна відповідь (204 No Content) від {url}") if self.test_mode_logs else None
                    return response

                elif request_name == 'bulk_export_init' and response.status_code == 400:
                    response_json = response.json()
                    for message in response_json.get('messages', []):
                        if message.get('code') == 11027:
                            self.logger.warning("Процес експорту вже ініційовано. Повертаємо відповідь.")
                            return response

                        elif message.get('code') == 11029:
                            self.logger.warning(f"Спроба {attempt + 1}/{api_retries}"
                                                            f" Неможливо замовити експорт, іде завантаження файлу на g2g."
                                                            f" Наступне опитування через  {api_retry_delay} секунд.")
                            time.sleep(api_retry_delay)
                    continue

                elif response.status_code == 400 and request_name == 'download_exel_files':
                    # Це очікувана поведінка, якщо файл ще не готовий. Просто чекаємо.
                    self.logger.warning(f"Спроба {attempt + 1}/{api_retries}"
                                                    f" Файл ще не готовий."
                                                    f" Наступне опитування через  {api_retry_delay} секунд.")
                    time.sleep(api_retry_delay)
                    continue

                elif response.status_code == 400 and request_name == 'delete_import':
                    # Це очікувана поведінка, якщо файл ще не готовий. Просто чекаємо.
                    self.logger.warning(f"Спроба {attempt + 1}/{api_retries}"
                                                    f" Не вдалося видалити імпорт. Файл ще завантажується."
                                                    f" Наступне опитування через {api_retry_delay} секунд.")
                    time.sleep(api_retry_delay)
                    continue

                elif response.status_code == 404 and request_name == 'delete_export':
                    self.logger.warning(f"Нема замовленого експорту для цього регіону")
                    return

                elif response.status_code == 404 and request_name == 'delete_import':
                    self.logger.warning(f"Нема активного імпорту для цього регіону")
                    return

                elif response.status_code == 401:
                    self.logger.warning(f"Отримано 401 Unauthorized. Робимо примусове оновлення токену..."
                                        f"Спроба {attempt + 1}/{api_retries}")
                    asyncio.run(self.token_manager.refresh_access_token())
                    request_headers = self.auth_headers()
                    continue

                self.logger.info(f"Спроба {attempt + 1}/{api_retries}: HTTP {response.status_code} - {response.text}")

            except RequestException as e:
                self.logger.info(f"Спроба {attempt + 1}/{api_retries}: Помилка з'єднання - {str(e)}")

            time.sleep(api_retry_delay)


    def upload_exel_file(self,file_path:Path, relation_id):
        """
                Завантажує оновлений Excel-файл на G2G.
                Виконує послідовність з 4 HTTP-запитів.
                """

        # Закриваємо активний імпорт, якщо він існує
        self.delete_import(relation_id)

        if not file_path.exists():
            self.logger.error(f"Помилка: Файл '{file_path}' не знайдено для завантаження.")
            return False

        file_name_encoded = quote_plus(file_path.name) # Кодування імені файлу для URL
        self.logger.info(f"Початок завантаження файлу '{file_name_encoded}' на G2G...")

        # 1. Запит на отримання URL для завантаження (GET /offer/upload_url)
        try:
            get_upload_url = "https://sls.g2g.com/offer/upload_url"
            get_upload_url_params = {
                "seller_id": self.seller_id,
                "file_name": file_name_encoded,
                "upload_type": "import_offer"
            }

            self.logger.info(f"Крок 1/4: Запит на URL завантаження: {get_upload_url}")
            response_get_url = self.fetch_from_api_with_retry(url=get_upload_url,
                                                              headers=self.auth_headers(),
                                                              payload=get_upload_url_params)
            self.logger.info(response_get_url.json()) if self.test_mode_logs else None
            response_get_url_json = response_get_url.json()  # Парсимо JSON відповідь
            payload = response_get_url_json.get('payload')

            if not payload:
                self.logger.error(f"Помилка Крок 1: Не отримано 'payload' з відповіді: {response_get_url_json}")
                return False

            upload_url = payload.get('url')
            upload_fields = payload.get('fields')
            uploaded_file_name = payload.get('uploaded_file_name')

            logger.info(f"upload_url: {upload_url}") if self.test_mode_logs else None
            logger.info(f"upload_fields: {upload_fields}") if self.test_mode_logs else None
            logger.info(f"uploaded_file_name: {uploaded_file_name}") if self.test_mode_logs else None
            logger.info(f"response_get_url_json: {response_get_url_json}") if self.test_mode_logs else None

            if not all([upload_url, upload_fields, uploaded_file_name]):
                self.logger.error(f"Помилка Крок 1: Відсутні необхідні дані в payload: {payload}")
                return False
            self.logger.info(f"response_get_url: {response_get_url}")
            self.logger.info(f"Крок 1 успішний. Отримано URL для завантаження та поля.") if self.test_mode_logs else None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Помилка Крок 1 (отримання URL завантаження): {e}")
            return False
        except json.JSONDecodeError:
            self.logger.error(f"Помилка Крок 1: Не вдалося декодувати JSON відповідь.")
            return False

            # 2. Завантаження файлу на S3 (POST до upload_url)
        try:
            # Читаємо файл у бінарному режимі
            with open(file_path, 'rb') as f:
                file_content = f.read()
        except IOError as e:
            self.logger.error(f"Помилка читання файлу '{file_path}': {e}")
            return False

        # Об'єднуємо всі поля з s3_fields та сам файл в один словник для параметра 'files'
        s3_post_data = {}
        for field_name, field_value in upload_fields.items():
            s3_post_data[field_name] = (None, field_value)  # Звичайні поля форми

        # Додаємо сам файл
        s3_post_data['file'] = (
        file_path.name, file_content, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        self.logger.info(f"Крок 2/4: Завантаження файлу '{file_path.name}' на S3 за URL: {upload_url}")
        self.logger.info(f"Поля для S3 завантаження: {upload_fields}") if self.test_mode_logs else None
        self.logger.info(f"Заголовки для S3 завантаження: {self.s3_headers}") if self.test_mode_logs else None

        response_s3_upload = self.fetch_from_api_with_retry(upload_url,
                                                   files=s3_post_data,
                                                   headers=self.s3_headers,
                                                   http_method="POST",
                                                   request_name='s3_upload')

        if not response_s3_upload:
            self.logger.error("Не вдалося завантажити файл на S3 (відповідь відсутня).")
            return False
        if response_s3_upload.status_code != 204:
            self.logger.error(f"Не вдалося завантажити файл на S3. Статус: {response_s3_upload.status_code}")
            return False

        self.logger.info(f"Файл успішно завантажено на S3."
                         f" Статус_код: {response_s3_upload.status_code}")

        # 3. Повідомлення G2G про масовий імпорт
        self.logger.info("Крок 3: Повідомлення G2G про масовий імпорт.")
        bulk_import_endpoint = f"https://sls.g2g.com/offer/seller/{self.seller_id}/bulk_import"
        payload_bulk_import = {
            "action": "update_offer",
            "display_file_name": file_path.name,
            "uploaded_file_name": uploaded_file_name,
        }

        response_bulk_import = self.fetch_from_api_with_retry(
            url=bulk_import_endpoint,
            headers=self.auth_headers(),
            payload=payload_bulk_import,
            http_method="POST"
        )

        if not response_bulk_import:
            self.logger.error("Не вдалося повідомити G2G про масовий імпорт (відповідь відсутня).")
            return False
        if response_bulk_import.status_code != 200:
            self.logger.error(
                f"Не вдалося повідомити G2G про масовий імпорт. Статус: {response_bulk_import.status_code}")
            return False

        if response_bulk_import.status_code == 200:
            self.logger.info(f"Повідомлення G2G про масовий імпорт успішно надіслано."
                             f"Response:{response_bulk_import.json()}"
                             f" Статус_код: {response_bulk_import.status_code}") if self.test_mode_logs else None


    def download_exel_files(self, game_alias,  relation_id):
        #Надсилаємо запит на отримання експорту
        logger.warning(f"Починаємо завантаження {game_alias}")

        # Видаляємо попередньо замовлені експорти
        self.delete_export(relation_id)

        url_bulk_export = "https://sls.g2g.com/offer/seller/5688923/bulk_export"
        payload_bulk_export = {
            "offer_status": "live",
            "out_of_stock": False,
            "relation_id": relation_id
        }

        response_bulk_export = self.fetch_from_api_with_retry(url=url_bulk_export,
                                                  headers=self.auth_headers(),
                                                  payload=payload_bulk_export,
                                                  request_name='bulk_export_init',
                                                  http_method="POST",
                                                 )
        # Перевіряємо успішний статус 200
        if response_bulk_export.status_code == 200:
            self.logger.info("Масовий експорт успішно ініційовано.")

        time.sleep(self.api_retry_delay) if response_bulk_export.status_code != 400 else time.sleep(1)

        # Формуємо правильний URL
        download_url = (f"https://sls.g2g.com/offer/seller/{self.seller_id}/"
                        f"exported_offers/{relation_id}")

        # Надсилаємо запит
        download_url_response = self.fetch_from_api_with_retry(url=download_url,
                                                  headers=self.auth_headers(),
                                                  request_name='download_exel_files')

        if download_url_response.status_code == 200:
            self.logger.info("Файл експорту успішно згенеровано! Завантажуємо...")

            try:
                download_data = download_url_response.json()
                s3_download_url = download_data['payload']['result']

                # Завантажуємо файл безпосередньо з S3 за отриманим посиланням
                final_file_response = requests.get(s3_download_url)

                if final_file_response.status_code == 200:
                    os.makedirs(self.offers_folder, exist_ok=True)
                    os.makedirs(self.offers_folder.joinpath("archives"), exist_ok=True)
                    os.makedirs(self.offers_folder.joinpath("unpacked exels"), exist_ok=True)
                    archive_path = self.offers_folder.joinpath("archives") / f"{game_alias}.zip"
                    # Зберігаємо файл
                    with open(archive_path, 'wb') as f:
                        f.write(final_file_response.content)
                    self.logger.info(f"Файл {archive_path} успішно завантажено.")
                    # --- Розпакування архіву ---
                    unpacked_dir = self.offers_folder.joinpath("unpacked exels", game_alias)
                    os.makedirs(unpacked_dir, exist_ok=True)

                    try:
                        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                            zip_ref.extractall(unpacked_dir)
                        self.logger.info(f"Архів {archive_path} успішно розпаковано до {unpacked_dir}.")
                    except zipfile.BadZipFile:
                        self.logger.error(f"Помилка: Файл {archive_path} не є дійсним ZIP-архівом.")
                    except Exception as e:
                        self.logger.error(f"Помилка при розпакуванні архіву {archive_path}: {e}")
                    # --- Кінець розпакування ---
                else:
                    self.logger.error(
                        f"Не вдалося завантажити файл з S3. Статус: {final_file_response.status_code}")
                    raise ConnectionError
                # Видалення експорту з G2G
                self.delete_export(relation_id)

            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Помилка при обробці відповіді API: {e}")
                return False

        return unpacked_dir

    def delete_export(self, relation_id):
        delete_export_url = "https://sls.g2g.com/offer/task_status?"
        params = {
            "relation_id": relation_id,
            "seller_id": self.seller_id,
            "task_name": "bulk_export_offer"
        }
        delete_export_response = self.fetch_from_api_with_retry(url=delete_export_url,
                                                  headers=self.auth_headers(),
                                                  payload=params,
                                                  request_name='delete_export',
                                                  http_method="DELETE")
        if delete_export_response is None:
            return
        if delete_export_response.status_code == 200:
            logger.info(f"delete_export_response: {delete_export_response.json()}") if self.test_mode_logs else None
            self.logger.warning("Експорт успішно видалено.")
        else:
            self.logger.error(f"Помилка при видаленні експорту. Статус: {delete_export_response.status_code}")

    def delete_import(self, relation_id):
        delete_import_url = "https://sls.g2g.com/offer/task_status?"
        params = {
            "relation_id": relation_id,
            "seller_id": self.seller_id,
            "task_name": "bulk_import_offer",
            "import_action": "update_offer"
        }
        delete_import_response = self.fetch_from_api_with_retry(url=delete_import_url,
                                                  headers=self.auth_headers(),
                                                  payload=params,
                                                  request_name='delete_import',
                                                  http_method="DELETE")
        if delete_import_response is None:
            return
        if delete_import_response.status_code == 200:
            logger.info(f"delete_import_response: {delete_import_response.json()}")
            self.logger.info("Імпорт успішно видалено.")
        else:
            self.logger.error(f"Помилка при видаленні імпорту. Статус: {delete_import_response.status_code}")

    def process_offers(self):
        self.token_manager.token_ready_event.wait()
        files_paths = {"panda_us": "/home/roll1ng/Documents/Python_projects/Last_item_bot/source_offers/unpacked exels/panda_us",
         "panda_eu": "/home/roll1ng/Documents/Python_projects/Last_item_bot/source_offers/unpacked exels/panda_eu",
         "era_us_test": "/home/roll1ng/Documents/Python_projects/Last_item_bot/source_offers/unpacked exels/era_us_test",
         "era_eu": r"C:\Users\admin\Desktop\Last_item_bot\source_offers\unpacked exels\era_eu"
         }
        while True:
            for game_alias, parameters in self.relations_ids.items():
                relation_id = parameters["relation_id"]
                self.logger.info(f"\033[92m\n_________________________________________________________________________"
                                         f" Починаємо роботу з {game_alias}"
                                         f"_________________________________________________________________________\033[0m")
                # exels_file_path = self.download_exel_files(game_alias, relation_id)
                exels_file_path = Path(files_paths[game_alias])
                self.logger.warning(f"exels_file_path  {exels_file_path}")
                if exels_file_path is None:
                    self.logger.error(f"Помилка: Папка для обробки '{game_alias}' не знайдена.")


                self.logger.info(f"Початок нового циклу обробки файлів у '{exels_file_path}'.")
                # Збираємо список файлів для обробки, щоб уникнути проблем з ітератором та мати можливість обробляти їх в порядку або повторно


                excel_files = sorted([f for f in exels_file_path.iterdir() if f.suffix == '.xlsx'])

                if not excel_files:
                    self.logger.warning(f"У папці '{excel_files}' не знайдено файлів Excel для обробки. Очікування...")
                    continue  # Починаємо наступну ітерацію зовнішнього циклу

                for file_path in excel_files:
                    self.logger.info(f"\nОбробка файлу: {file_path.name}")
                    try:
                        full_df = pd.read_excel(file_path, sheet_name='Offers', engine='openpyxl', header=None)
                        header_row_index = 4

                        if header_row_index >= len(full_df):
                            self.logger.error(f"Файл '{file_path.name}' має замало рядків. Пропускаємо.")
                            continue

                        columns = full_df.iloc[header_row_index].tolist()
                        data_df = full_df[header_row_index + 1:].copy()
                        data_df.columns = columns
                        data_df.columns = data_df.columns.str.strip()

                        # Отримуємо кількість рядків та розраховуємо тайм-аут очикування після завантаження
                        num_rows = data_df.shape[0]
                        time_aut_value_seconds = num_rows // 1000 * 60
                        if time_aut_value_seconds == 0:
                            time_aut_value_seconds = 1
                            time_aut_value_seconds = 1
                        self.logger.info(f"Тайм-aут для файла з {num_rows} рядками: {time_aut_value_seconds} секунд.") if self.test_mode_logs else None

                        required_columns = ['Offer ID', 'Unit Price', 'Title', 'Min. Purchase Qty']
                        if not all(col in data_df.columns for col in required_columns):
                            missing_cols = [col for col in required_columns if col not in data_df.columns]
                            self.logger.warning(
                                f"  Помилка: Не знайдено необхідні колонки {missing_cols} у файлі '{file_path.name}'. Пропускаємо.")
                            self.logger.info(f"  Доступні колонки у даних: {data_df.columns.tolist()}")
                            continue

                        try:
                            price_col_idx = data_df.columns.get_loc('Unit Price')
                            min_purchase_qty_idx = data_df.columns.get_loc('Min. Purchase Qty')
                            title_col_idx = data_df.columns.get_loc('Title')
                        except KeyError as e:
                            self.logger.error(f"  Не вдалося знайти колонку у файлі '{file_path.name}': {e}. Пропускаємо.")
                            continue

                        tasks = []
                        for original_index, row_data in data_df.iterrows():
                            tasks.append((original_index, row_data.to_dict()))

                        processed_results = []

                        if not tasks:
                            self.logger.info(f"  У файлі '{file_path.name}' немає рядків даних для обробки. Пропускаємо.")
                            continue

                        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads_quantity) as executor:
                            future_to_index = {
                                executor.submit(self._process_single_offer, index, data): index
                                for index, data in tasks
                            }

                            for future in concurrent.futures.as_completed(future_to_index):
                                try:
                                    result = future.result()
                                    if result:
                                        processed_results.append(result)
                                except Exception as inner_e:
                                    self.logger.error(
                                        f"  Помилка при обробці окремої пропозиції у файлі '{file_path.name}': {inner_e}",
                                        exc_info=True)

                        # --- Послідовне оновлення DataFrame ---
                        if processed_results:  # Перевіряємо, чи є результати для обробки
                            processed_results.sort(key=lambda x: x['original_index'])

                            for result_data in processed_results:
                                original_index = result_data['original_index']
                                offer_id = result_data['offer_id']
                                original_title = result_data['original_title']
                                table_min_purchase_qty = result_data['table_min_purchase_qty']
                                table_price = result_data['original_unit_price']
                                new_price = result_data['new_price']
                                new_title = result_data['new_title']
                                final_price=table_price

                                if new_price is not None:
                                    full_df.iloc[original_index, price_col_idx] = float(new_price)
                                    final_price= new_price


                                if final_price > 0 and (
                                     decimal.Decimal(final_price) * decimal.Decimal(table_min_purchase_qty)) < self.config_minimal_purchase_qty:
                                    new_min_purchase_qty = math.ceil(self.config_minimal_purchase_qty / decimal.Decimal(final_price))
                                    full_df.iloc[original_index, min_purchase_qty_idx] = int(new_min_purchase_qty)
                                    self.logger.info(
                                        f"Змінена мінімальна кількість покупки до"
                                        f" {new_min_purchase_qty:.0f} для Offer ID {offer_id}") if self.test_mode_logs else None


                                if new_title is not None:
                                    full_df.iloc[original_index, title_col_idx] = str(new_title)
                                    self.logger.info(
                                        f"{self.red}Оновлено Offer ID {offer_id}: Назва з '{original_title}' на '{new_title}'{self.reset}")
                        else:
                            self.logger.info(f"  Немає оновлених пропозицій для файлу '{file_path.name}'.")

                        self.output_folder.mkdir(parents=True, exist_ok=True)
                        output_file_path = self.output_folder / file_path.name  # Це перезапише файл
                        self.logger.info(f"Збереження оновленого файлу як: {output_file_path.name}")

                        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                            full_df.to_excel(writer, sheet_name='Offers', index=False, header=False)

                        # self.upload_exel_file(output_file_path, relation_id)
                        # self.logger.warning(f"  Файл '{output_file_path.name}' завантажується на G2G протягом {time_aut_value_seconds} секунд.")
                        # time.sleep(time_aut_value_seconds)

                    except Exception as e:
                        self.logger.error(f"  Загальна помилка при читанні/обробці файлу '{file_path.name}': {e}",
                                  exc_info=True)
                    finally:
                        pass

            # --- Пауза між повними проходами по всіх файлах ---
            self.logger.info(
                f"\033[97m_______________________________________"
                f"Завершено обробку всіх файлів."
                f" Очікування {self.delay_seconds_between_cycles} секунд перед новим циклом."
                f"_______________________________________\033[0m")
            time.sleep(self.delay_seconds_between_cycles)


async def main():
    """
    Основна асинхронна функція для запуску обох завдань.
    """
    loop = asyncio.get_running_loop()

    # Створюємо ThreadPoolExecutor для запуску синхронної offer_processor.process_offers()
    # Це дозволяє їй працювати у фоновому потоці, не блокуючи asyncio event loop.
    with ThreadPoolExecutor(max_workers=1) as executor:
        # Запускаємо token_refresh_scheduler_direct як асинхронне завдання
        token_task = loop.create_task(token_refresh_scheduler_direct(TokenManager()))


        # Запускаємо offer_processor.process_offers() в окремому потоці через executor
        # Ми можемо передавати параметри в конструктор OfferProcessor, якщо потрібно
        offer_processor_task = loop.run_in_executor(
            executor,
            run_offer_processor)


        try:
            # Чекаємо на завершення обох завдань (або на KeyboardInterrupt)
            await asyncio.gather(offer_processor_task, token_task)
        except asyncio.CancelledError:
            # Обробляємо скасування завдань, якщо основний цикл завершується
            pass


def run_offer_processor():
    """
    Обгортка для запуску OfferProcessor.process_offers().
    Це буде виконуватися в окремому потоці.
    """
    offer_processor_instance = None  # Ініціалізуємо змінну

    try:
        offer_processor_instance = OfferProcessor()
        offer_processor_instance.process_offers()
    except KeyboardInterrupt:
        # KeyboardInterrupt буде оброблено на рівні main() за допомогою asyncio.CancelledError
        # Тут ми просто повертаємо, щоб потік завершився.
        if offer_processor_instance is not None:
            offer_processor_instance.logger.info("OfferProcessor: Отримано сигнал завершення. Вихід.")
        else:
            print("OfferProcessor: Отримано сигнал завершення. Вихід.")
        raise  # Перевикидаємо, щоб основний цикл міг це обробити
    # except Exception as e:
    #     if offer_processor_instance is not None:
    #         offer_processor_instance.logger.error(f"Помилка при обробці OfferProcessor: {e}", exc_info=True)
    #     else:
    #        print(f"Неочікувана помилка в run_offer_processor до ініціалізації: {e}", file=sys.stderr)
    #     raise  # Перевикидаємо, щоб основний цикл міг це обробити



if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Цей блок перехоплює Ctrl+C, якщо він був натиснутий поза asyncio.gather
        # або під час ініціалізації asyncio.run.
        print("\nПрограму перервано користувачем (Ctrl+C). Завершення.", file=sys.stderr)
        sys.exit(0)
    # except Exception as e:
    #     print(f"\nКритична помилка програми: {e}", file=sys.stderr)
    #     sys.exit(1)

