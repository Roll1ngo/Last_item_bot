import json
import os
import re
import time
from pathlib import Path
from typing import Optional, Dict
from urllib.parse import urlencode

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from sqlalchemy.exc import IntegrityError

from data_base.models import Parameters
from functions.load_config import get_config
from functions.logger_config import logger  
from functions.util_functions import calculate_percent_difference
from data_base.connection import db



class OfferProcessor:
    def __init__(self, offers_folder_path: str = 'source_offers_xlsx', output_folder_name: str = 'updated_offers_xlsx'):
        load_dotenv()
        self.config = get_config()

        self.logger = logger  # Використовуємо logger з functions.logger_config
        if not self.config:
            self.logger.critical("Помилка завантаження конфігурації. Завершення роботи.")
            raise ValueError("Configuration could not be loaded.")
        
        self.offers_folder = Path(__file__).resolve().parent.joinpath(offers_folder_path)
        self.output_folder = Path(__file__).resolve().parent.joinpath(output_folder_name)
        # Визначення кольору для значення "unit_price" у логах
        self.red = "\033[31m"
        self.reset = "\033[0m"
        self.authorization_token = os.getenv("AUTH_TOKEN")
        self.db = db

        self._load_config_parameters()
        self._initialize_patterns()
        self._initialize_headers()

    def _load_config_parameters(self):
        """Завантажує всі параметри конфігурації в атрибути класу."""
        self.owner = self.config.get('owner')
        self.user_agent = self.config.get('user_agent')
        self.platform = self.config.get('platform')
        self.pause_between_runs = self.config.get('pause_between_runs')
        self.delete_temp_folders = self.config.get('delete_temp_folders')
        self.test_mode_logs = self.config.get('test_mode_logs')
        self.ignore_words = self.config.get('ignore_words')
        self.position_for_change_pattern_if_owner_price_over_limit = self.config.get(
            'position_for_change_pattern_if_owner_price_over_limit')
        self.position_for_change_pattern_if_owner_price_under_limit = self.config.get(
            'position_for_change_pattern_if_owner_price_under_limit')
        self.ignore_competitors_for_asterisk = self.config.get('ignore_competitors_for_asterisk')
        self.ignore_competitors_for_other_patterns = self.config.get('ignore_competitors_for_other_patterns')
        self.default_limit = self.config.get("default_limit", 0)
        
        self.api_retries = self.config.get("api_retries", 3)
        self.api_retry_delay = self.config.get("api_retry_delay", 1)

        self.threshold_price_for_percentage_change = self.config.get("threshold_price_for_percentage_change")
        self.change_percents_before_threshold = self.config.get("change_percents_before_threshold")
        self.change_percents_after_threshold = self.config.get("change_percents_after_threshold")

        self.min_max_change_first_position = self.config.get('min_max_change_first_position')
        self.reduce_price_non_popular_item = self.config.get('self.reduce_price_non_popular_item')
        self.max_limit_price_for_pull = self.config.get("max_limit_price_for_pull")
        self.config_minimal_purchase_qty = self.config.get("config_minimal_purchase_qty")

        self.max_difference_percent_to_reduce_the_price_between_first_and_second = (
            self.config.get("max_difference_percent_to_reduce_the_price_between_first_and_second"))
        self.min_difference_percent_to_reduce_the_price_between_first_and_second = (
            self.config.get("min_difference_percent_to_reduce_the_price_between_first_and_second"))

    def _initialize_headers(self):
        self.base_headers ={
            "accept": "application/json, text/plain, */*",
            "accept-language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            # "authorization": authorization,
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

    def _get_response_from_api_with_retry(self, url: str, offer_id: str,
                      headers: Dict[str, str],
                      params: Dict[str, str]) -> Optional[
        requests.Response]:
        """
        Відправляє HTTP GET запит, повертає відповідь.
        Додано повторні підключення при помилках мережі або тайм-аутах.
        """
        for attempt in range(self.api_retries):
            try:
                response = requests.get(url + offer_id, headers=headers, params=params, timeout=10)
                response.raise_for_status()  # Викликає HTTPError для поганих відповідей (4xx або 5xx)
                return response
            except (ConnectionError, Timeout) as e:
                self.logger.warning(
                    f"Спроба {attempt + 1}/{self.api_retries}: Помилка підключення або тайм-аут для offer ID {offer_id}: {e}. Затримка {self.api_retry_delay} сек..."
                )
                time.sleep(self.api_retry_delay)
            except HTTPError as e:
                # Для HTTP помилок (наприклад, 404, 500) ми можемо не робити повторні спроби,
                # оскільки це може бути постійна проблема. Але залишимо логіку повтору.
                self.logger.warning(
                    f"Спроба {attempt + 1}/{self.api_retries}: HTTP помилка для offer ID {offer_id}: {e.response.status_code} - {e.response.text}. Затримка {self.api_retry_delay} сек..."
                )
                time.sleep(self.api_retry_delay)
            except RequestException as e:
                self.logger.warning(
                    f"Спроба {attempt + 1}/{self.api_retries}: Загальна помилка запиту для offer ID {offer_id}: {e}. Затримка {self.api_retry_delay} сек..."
                )
                time.sleep(self.api_retry_delay)

        self.logger.error(f"Не вдалося отримати відповідь для offer ID {offer_id} після {self.api_retries} спроб.")
        return None

    def _receiving_data(self, concurrent_json_info: Dict, owner_offer_info: Optional[Dict]) -> Optional[Dict]:
        """Парсить інформацію про конкурентів з API відповіді."""
        try:
            json_data = concurrent_json_info["payload"]
            # offer_id = json_data["offer_id"] # offer_id вже є в owner_offer_info

            # if owner_offer_info is None: # Ця перевірка зайва, оскільки owner_offer_info передається
            #     return None

            # `q` is already present in `owner_offer_info['short_title']`
            # q = owner_offer_info.get('short_title') # This line seems out of place or self.redundant here if q is for params

            brand_id = json_data.get("brand_id")
            seo_term = None

            if brand_id == "lgc_game_29076":
                seo_term = "wow-classic-item"
            elif brand_id == "lgc_game_27816":
                seo_term = "wow-classic-era-item"

            region_id = json_data.get("region_id")  # Assuming region_id is directly in json_data
            filter_attr_row = json_data["offer_attributes"][1]
            collection_id = filter_attr_row["collection_id"]
            dataset_id = filter_attr_row["dataset_id"]
            filter_attr = f"{collection_id}:{dataset_id}"

            # Reconstruct params dict to be used by get_list_product
            params = {
                "seo_term": seo_term,
                "region_id": region_id,
                "q": owner_offer_info.get('short_title'),  # Using 'short_title' as 'q'
                "filter_attr": filter_attr,  # This was not defined in the original snippet, assuming it's dynamic
            }
            return params

        except KeyError as e:
            self.logger.error(f"Не вдалося розпарсити дані: відсутній ключ {e} у відповіді API.")
            return None
        except Exception as e:
            self.logger.error(f"Неочікувана помилка при парсингу даних: {e}")
            return None

    def get_params_from_api(self, owner_offer_info: Dict) -> dict or None:
        """Отримує інформацію про товар, включаючи нову ціну та назву."""
        offer_id = owner_offer_info.get('offer_id')
        self.logger.info(
            f'_________________________________________________________________________________________________'
            f'____')


        params = {
            "currency": "USD",
            "country": "UA",
            "include_out_of_stock": "1",
            "include_inactive": "1",
        }
        url = "https://sls.g2g.com/offer/"

        response = self._get_response_from_api_with_retry(url=url,
                                                          offer_id=offer_id,
                                                          headers=self.base_headers,
                                                          params=params)
        if response is None:
            self.logger.error(f"Не вдалося підключитись до сервера для offer ID {offer_id}. Спробуємо пізніше")
            return None

        concurrent_json_info = response.json()

        # `receiving_data` will now return the params needed for `get_list_product`
        api_params = self._receiving_data(concurrent_json_info, owner_offer_info)

        if api_params is None:
            self.logger.error(f"Не вдалося отримати параметри для запиту списку товарів для {offer_id}")
            return None

        return api_params

    def find_pattern(self, owner_title):
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
                logger.critical(f"ValueError при обробці {owner_title}. Паттерн: {pattern}")
                continue
        else:
            logger.warning(f" Пропускаємо: Паттерн не знайдено для title: {owner_title}")
            return None

    def define_offer_type(self,title):
        exist_ignore_in_owner_title = any(word in title for word in self.ignore_words)
        offer_type = 'schematic_type' if exist_ignore_in_owner_title else 'item_type'
        return offer_type

    def get_params_from_db(self, owner_offer_info) -> Optional[dict]:
        """
        Отримує параметри продукту з бази даних за offer_id, використовуючи ORM.
        Повертає словник з seo_term, region_id, filter_attribute, або None, якщо не знайдено.
        """
        offer_id = owner_offer_info.get('offer_id')
        try:
            with self.db.get_session() as session:
                # Використовуємо Query API з ORM
                # Запит для отримання одного об'єкта Parameter за offer_id
                parameter = session.query(Parameters).filter(Parameters.offer_id == offer_id).first()

                if parameter:
                    parameter_dict = {
                        "seo_term": parameter.seo_term,
                        "region_id": parameter.region_id,
                        "q": owner_offer_info.get('short_title'),
                        "filter_attribute": parameter.filter_attribute,
                    }
                    self.logger.info(f"Параметри для offer_id {offer_id} отримано з бази даних: {parameter_dict}")
                    return parameter_dict
                else:
                    logger.info(f"Параметри для offer_id {offer_id} не знайдено в базі даних.")
                    return None
        except Exception as e:
            self.logger.error(f"Помилка при отриманні параметрів з БД для offer_id {offer_id}: {e}", exc_info=True)
            return None

    def record_params_to_db(self, offer_id: str, params: dict):
        """
        Записує параметри продукту в базу даних, використовуючи ORM.
        Якщо запис для offer_id вже існує, він буде доданий як новий.
        """
        try:
            with self.db.get_session() as session:
                # Створюємо новий об'єкт моделі Parameter
                new_parameter = Parameters(
                    offer_id=offer_id,
                    seo_term=params.get('seo_term'),
                    region_id=params.get('region_id'),
                    filter_attribute=params.get('filter_attr'),
                )
                session.add(new_parameter)  # Додаємо об'єкт до сесії
                session.commit()  # Фіксуємо зміни
                self.logger.info(f"Параметри для offer_id {offer_id} успішно записано до БД.")
        except IntegrityError as e:
            # Це може статися, якщо offer_id зроблено UNIQUE і ви намагаєтеся додати дублікат
            self.logger.warning(f"Запис для offer_id {offer_id} вже існує або порушено унікальний ключ: {e}")
            session.rollback()  # Відкочуємо транзакцію у разі помилки
        except Exception as e:
            self.logger.error(f"Помилка при записі параметрів до БД для offer_id {offer_id}: {e}", exc_info=True)
            session.rollback()  # Завжди відкочуємо транзакцію у разі помилки

    def get_list_competitors(self, params, offer_id):

        # Додаємо параметри для формування url
        params["page_size"] = 48
        params["sort"] = "lowest_price"
        params["currency"] = 'USD'
        params["country"] = "UA"
        base_url = "https://sls.g2g.com/offer/search"

        # Формуємо остаточний url з параметрами сторінки
        encoded_params = urlencode(params)
        full_url = f"{base_url}?{encoded_params}"
        response = self._get_response_from_api_with_retry(url=full_url,
                                      headers=self.base_headers,
                                      offer_id=offer_id,
                                      params=params)
        if response is None:  # Перевіряємо, чи немає відповіді
            logger.critical("Немає відповіді сервера. Ідемо далі")
            return None
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Помилка при отриманні списку конкурентів: {response.status_code}")
            return None

    def price_study(self, data, owner_offer_info):

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
                    logger.warning(f" skip invalid id for: {username}, currently work with_offer_id: {owner_offer_id},"
                                   f"title: {owner_title} "
                                   f" but {self.owner} have another lot competitor_offer_id: {competitor_offer_id}"
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

                    logger.warning(f"У продавця {username} валюта {self.red}{competitor_currency}{self.reset} не відповідає USD. "
                                   f"Змінено параметр ціни з unit_price {self.red}{previous_competitor_unit_price}{self.reset}"
                                   f" на display_price {self.red}{competitor_unit_price}{self.reset}"
                                   f" у товарі {competitor_title}") if self.test_mode_logs else None

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
            logger.info(f"sellers_usernames_list: {sellers_usernames_list}") if self.test_mode_logs else None

            # Визначаємо чи необхідна зміна патерну та ліміту
            try:
                if (owner_pattern_name != 'asterisk' and owner_price > owner_limit
                        and owner_position >= self.position_for_change_pattern_if_owner_price_over_limit
                        and len(sellers_usernames_list) >= self.position_for_change_pattern_if_owner_price_over_limit):
                    logger.info(
                        f"{self.owner} на позиції {owner_position} з ціною {owner_price} вищою за ліміт {owner_limit}"
                        f" та продавці {[seller for seller in sellers_usernames_list if seller != self.owner]}"
                        f"мають ціну нижчу за ліміт {owner_limit}")

                    logger.info('Переходим до зміни патерну та ліміту')
                    logger.info(f"previous_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None
                    new_owner_info, new_title = self.define_new_title_and_owner_info(owner_info)

                    # Записуємо нову інформацію про лот власника
                    competitors['owner_offer_info'] = new_owner_info
                    logger.info(f"new_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None

                if (owner_pattern_name != 'asterisk' and owner_price < owner_limit
                        and owner_position >= self.position_for_change_pattern_if_owner_price_under_limit
                        and len(sellers_usernames_list) >= self.position_for_change_pattern_if_owner_price_under_limit):
                    logger.info(
                        f"{self.owner} на позиції {owner_position} з ціною {owner_price} нижчою за ліміт {owner_limit}"
                        f" та продавці {[seller for seller in sellers_usernames_list if seller != self.owner]}"
                        f"мають ціну нижчу за ліміт {owner_limit}")
                    logger.info('Переходим до зміни патерну та ліміту')
                    logger.info(f"previous_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None
                    new_owner_info, new_title = self.define_new_title_and_owner_info(owner_info)

                    # Записуємо нову інформацію про лот власника
                    competitors['owner_offer_info'] = new_owner_info
                    logger.info(f"new_owner_info: {competitors['owner_offer_info']}") if self.test_mode_logs else None

            except Exception as e:
                logger.critical(f"Помилка при зміні патерну {e}")
                return None, None
            new_price = self.general_patterns(competitors)  # Визначаємо поведінку ціни

            if isinstance(new_price, str):
                logger.info(new_price)
                if new_title:
                    return None, new_title
                return None, None

            elif isinstance(new_price, (int, float)):
                return new_price, new_title

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
        pull_indicator = self.get_pull_indicator(owner_position, competitors, ignore_competitors)

        logger.info(f"pull_indicator: {pull_indicator}") if self.test_mode_logs else None

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
                                        f" з ціною {self.red}{unit_price}{self.reset}")
                    continue

                # Перевіряємо умови для зниження ціни на непопулярний товар
                elif user_name == self.owner and position == 1 and len(competitors) == 1:
                    return (f"{self.owner} єдиний продавець товару {short_title} з ціною"
                            f" {self.red}{unit_price}{self.reset} ")
                #
                #     if unit_price < limit:
                #         return (f"{owner} єдиний продавець непопулярного товару."
                #                 f" Ціна менша за встановленний ліміт {limit}"
                #                 f" знижена не буде і становить {self.red}{unit_price}{self.reset} ")
                #     # Розраховуємо знижену ціну
                #     new_price = unit_price * (1 - (self.reduce_price_non_popular_item / 100))
                #     if new_price < limit:
                #         return (f"{owner} єдиний продавець непопулярного товару."
                #                 f" Знижена ціна менша за встановленний ліміт {limit}."
                #                 f"Змінена не буде і становить {self.red}{unit_price}{self.reset} ")
                #     logger.CHANGE_PRICE(f"{owner} єдиний продавець непопулярного товару."
                #                         f"Ціна знижена на {self.reduce_price_non_popular_item}%"
                #                         f" і становить {self.red}{new_price}{self.reset}")
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

                    position_competitor_for_pull = position + 1
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
                                            f" для ціни {self.red}{pull_competitor_price}{self.reset}")
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
                                    f" з {self.red}{previous_price}{self.reset} на {self.red}{new_price}{self.reset}")

                logger.info(
                    f"Ціна конкурента__{self.red}{unit_price}{self.reset}, відсоток перебиття__{change_price_coefficient} %"
                    f" Змінена ціна__{self.red}{new_price}{self.reset}") if self.test_mode_logs else None

                return new_price
            else:
                return f"Немає конкурентів на товар {short_title}"

        except Exception as e:
            return {'critical': f"Помилка у функції розрахунку ціни: {e} ціну не змінено"
                                f" на товар {short_title} з id {owner_offer_info['offer_id']} "}

    def get_pull_indicator(self, owner_position, competitors, ignored_competitors):
        if owner_position == 1:
            return True if len(competitors) >= 2 else False

        competitors_before_owner = [competitor['username'] for pos, competitor in competitors.items()
                                    if pos < owner_position]
        logger.info(f"competitors_before_owner: {competitors_before_owner}") if self.test_mode_logs else None

        for competitor in competitors_before_owner:
            if competitor not in ignored_competitors:
                return False

        return True

    def process_offers(self):
        """
        Перебирає файли .xlsx, оновлює "Unit Price" та "Title"
        зберігаючи всі початкові рядки заголовка.
        """
        self.logger.info(f"Offers folder: {self.offers_folder}")

        if not self.offers_folder.is_dir():
            self.logger.error(f"Помилка: Папка '{self.offers_folder}' не знайдена.")
            return

        if self.output_folder.exists() and any(self.output_folder.iterdir()):
            # Якщо папка існує і не порожня
            target_folder = self.output_folder
        else:
            # Якщо папка не існує або порожня
            target_folder = self.offers_folder

        self.logger.info(f"Target folder: {target_folder}")

        for file_path in target_folder.iterdir():
            if file_path.suffix == '.xlsx':
                self.logger.info(f"\nОбробка файлу: {file_path.name}")
                try:
                    full_df = pd.read_excel(file_path, sheet_name='Offers', engine='openpyxl', header=None)
                    header_row_index = 4  # Рядок 5 Excel, індекс 4 у pandas

                    if header_row_index >= len(full_df):
                        self.logger.error(
                            f"Файл '{file_path.name}' має замало рядків. Рядок заголовка {header_row_index + 1} не існує.")
                        continue

                    columns = full_df.iloc[header_row_index].tolist()

                    data_df = full_df[header_row_index + 1:].copy()
                    data_df.columns = columns
                    data_df.columns = data_df.columns.str.strip()

                    required_columns = ['Offer ID', 'Unit Price', 'Title']
                    if not all(col in data_df.columns for col in required_columns):
                        missing_cols = [col for col in required_columns if col not in data_df.columns]
                        self.logger.warning(
                            f"  Помилка: Не знайдено необхідні колонки {missing_cols} у файлі '{file_path.name}'.")
                        self.logger.info(f"  Доступні колонки у даних: {data_df.columns.tolist()}")
                        continue

                    for original_index, row_data in data_df.iterrows():
                        offer_id = row_data['Offer ID']
                        unit_price = row_data['Unit Price']
                        title = row_data['Title']
                        table_min_purchase_qty = row_data['Min. Purchase Qty']
                        self.logger.info(f"table_min_purchase_qty: {table_min_purchase_qty}")
                        owner_offer_info = self.find_pattern(title)
                        if not owner_offer_info:
                            self.logger.warning(f"  Помилка: Не знайдено патерн для товару {title} (Offer ID: {offer_id}).")
                            input("Press Enter to continue...")
                            continue

                        owner_offer_info.update({'offer_id': offer_id,
                                                 'unit_price': unit_price})
                       
                        self.logger.info(f"  Оригінальний рядок {original_index + 1}: Offer ID = {offer_id}"
                                         f", Unit Price = {unit_price}, Title = {title}")
                        
                        params = self.get_params_from_db(owner_offer_info)

                        logger.info(f"Database params: {params}") if params else None
                        if not params:
                            params = self.get_params_from_api(owner_offer_info)
                            logger.info(f"  Параметри {params} для offer_id {offer_id} успішно отримано з API.")
                            self.record_params_to_db(offer_id, params)

                        competitors_list = self.get_list_competitors(params, offer_id)
                        if competitors_list is None:
                            logger.critical(
                                f"Не можемо отримати список продавців для товару {owner_offer_info['short_title']}")
                            return None, None

                        new_price, new_title = self.price_study(competitors_list, owner_offer_info)


                        self.logger.info(
                            f"  Отримано з get_product_info: Нова ціна = {new_price}, Нова назва = {new_title}")
                        # Перезаписуємо ціну, якщо вона змінюється
                        if new_price is not None:
                            price_col_idx = full_df.columns.get_loc(columns.index('Unit Price'))
                            full_df.iloc[original_index, price_col_idx] = float(new_price)
                            self.logger.warning(
                                f"{self.red}  Оновлено Offer ID {offer_id}: Ціна з {unit_price} на {new_price}{self.reset}")

                            # Піднімаємо мінімальну кількість до конфігураційного значення якщо ціна змінюється
                            if (new_price * table_min_purchase_qty) < self.config_minimal_purchase_qty:
                                new_min_purchase_qty = self.config_minimal_purchase_qty / new_price
                                min_purchase_qty_idx = full_df.columns.get_loc(columns.index('Min. Purchase Qty'))
                                full_df.iloc[original_index, min_purchase_qty_idx] = float(new_min_purchase_qty)
                                self.logger.info(f"Змінена мінімальна кількість покупки(min purchase qty) до {new_min_purchase_qty} для Offer ID {offer_id}")

                        # Перезаписуємо назву, якщо вона змінюється
                        if new_title is not None:
                            title_col_idx = full_df.columns.get_loc(columns.index('Title'))
                            full_df.iloc[original_index, title_col_idx] = str(new_title)
                            self.logger.warning(
                                f"{self.red}  Оновлено Offer ID {offer_id}: Назва з '{title}' на '{new_title}'{self.reset}")

                    self.output_folder.mkdir(parents=True, exist_ok=True)

                    output_file_path = self.output_folder / f"updated_{file_path.name}"
                    self.logger.info(f"Збереження оновленого файлу як: {output_file_path.name}")

                    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                        full_df.to_excel(writer, sheet_name='Offers', index=False, header=False)


                except Exception as e:
                    self.logger.error(f"  Помилка при читанні/обробці файлу '{file_path.name}': {e}", exc_info=True)
            else:
                self.logger.info(f"Пропуск файлу: {file_path.name} (не .xlsx)")


if __name__ == '__main__':
    try:
        processor = OfferProcessor()
        processor.process_offers()

    except ValueError as e:
        logger.error(f"Критична помилка ініціалізації: {e}")
    except Exception as e:
        logger.error(f"Неочікувана помилка виконання: {e}", exc_info=True)
        
        