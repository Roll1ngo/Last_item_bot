import random

from functions.load_config import get_config
from functions.logger_config import logger

RED = "\033[31m"
RESET = "\033[0m"

config = get_config()

if config:

    test_mode_logs = config.get("test_mode_logs")

    price_range_values = config.get("price_range")

    max_difference_percent_to_reduce_the_price_between_first_and_second = (
        config.get("max_difference_percent_to_reduce_the_price_between_first_and_second"))
    min_difference_percent_to_reduce_the_price_between_first_and_second = (
        config.get("min_difference_percent_to_reduce_the_price_between_first_and_second"))

    one_percent_reduce_percent_difference = config.get("one_percent_reduce_percent_difference")

    owner = config.get('owner')
    ignore_competitors_for_asterisk = config.get('ignore_competitors_for_asterisk')
    ignore_competitors_for_other_patterns = config.get('ignore_competitors_for_other_patterns')

    min_max_change_first_position = config.get('min_max_change_first_position')
    reduce_price_non_popular_item = config.get('reduce_price_non_popular_item')
    asteriks_max_update = config.get("asteriks_max_update")

    if price_range_values:
        price_range_from = price_range_values.get("price_range_from")
        price_range_to = price_range_values.get("price_range_to")
        display_price_change_value = price_range_values.get("display_price_change_value")

    else:
        print("Ключ 'price_range' не знайдено в конфігураційному файлі.")
else:
    print("Помилка завантаження конфігурації.")


def general_patterns(competitors):
    """Обробка патерну "+" (плюс)"""

    owner_offer_info = competitors.pop('owner_offer_info')
    logger.info(f'owner_offer_info: {owner_offer_info}') if test_mode_logs else None
    logger.info(f'competitors: {competitors}') if test_mode_logs else None

    previous_price = owner_offer_info['previous_price']
    short_title = owner_offer_info['short_title']
    pattern_name = owner_offer_info['pattern_name']
    owner_position = owner_offer_info['position']
    ignore_competitors = ignore_competitors_for_asterisk if pattern_name == 'asterisk' \
        else ignore_competitors_for_other_patterns
    limit = owner_offer_info['limit']
    logger.warning(f" Для патерну {pattern_name} на знайдено значення ліміту."
                   f" Наразі встановлено стандартне значення {limit}"
                   f"Перевірте конфігураційний файл.") if limit == 0 else None
    logger.info(f"pattern_name: {pattern_name}") if test_mode_logs else None
    logger.info(f"ignore_competitors: {ignore_competitors}") if test_mode_logs else None

    try:
        for position, competitor in competitors.items():
            user_name = competitor['username']
            unit_price = competitor['unit_price']
            change_price_coefficient = competitor['change_price_coefficient']

            # Перевіряємо конкурента на присутність в списку ігнорування
            if user_name in ignore_competitors:
                logger.CHANGE_PRICE(f" Ігноруємо {user_name} на позиції {position} по предмету {short_title}"
                                    f" з ціною {RED}{unit_price}{RESET}")
                continue

            # Перевіряємо умови для зниження ціни на непопулярний товар
            elif user_name == owner and position == 1 and len(competitors) == 1:
                if pattern_name == 'asterisk':
                    return (f"{owner} єдиний продавець товару {short_title} з ціною"
                            f" {RED}{unit_price}{RESET} ")

                if unit_price < limit:
                    return (f"{owner} єдиний продавець непопулярного товару."
                            f" Ціна менша за встановленний ліміт {limit}"
                            f" знижена не буде і становить {RED}{unit_price}{RESET} ")
                # Розраховуємо знижену ціну
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                if new_price < limit:
                    return (f"{owner} єдиний продавець непопулярного товару."
                            f" Знижена ціна менша за встановленний ліміт {limit}."
                            f"Змінена не буде і становить {RED}{unit_price}{RESET} ")
                logger.CHANGE_PRICE(f"{owner} єдиний продавець непопулярного товару."
                                    f"Ціна знижена на {reduce_price_non_popular_item}%"
                                    f" і становить {RED}{new_price}{RESET}")
                return new_price

            # Перевіряємо на можливість підтягування ціни до другої позиції
            elif user_name == owner and position == 1 and len(competitors) >= 2:

                if pattern_name == 'asterisk' and unit_price > asteriks_max_update:
                    return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                            f" з ціною {RED}{unit_price}{RESET}."
                            f" Ціну не підтягнуто, по патерну {pattern_name} ціна перевищує {asteriks_max_update} $")

                second_position_competitor_price = competitors[2]['unit_price']

                logger.info(f"second_competitor__{competitors[2]['username']}, price: "
                            f"{second_position_competitor_price}") if test_mode_logs else None

                price_difference_percent = calculate_percent_difference(unit_price,
                                                                        second_position_competitor_price)

                if (min_difference_percent_to_reduce_the_price_between_first_and_second <= price_difference_percent
                        <= max_difference_percent_to_reduce_the_price_between_first_and_second):

                    new_price = second_position_competitor_price * (1 - (min_max_change_first_position / 100))
                    new_price = round(new_price, 6)

                    logger.CHANGE_PRICE(f"Ціну підтягнуто на першій позиції"
                                        f" з {RED}{unit_price}{RESET} до {RED}{new_price}{RESET}. Різниця з "
                                        f"другою позицією {RED}{second_position_competitor_price}{RESET}"
                                        f" становила {price_difference_percent} %")
                    return new_price
                else:
                    return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                            f" з ціною {RED}{unit_price}{RESET}."
                            f" Ціну не підтягнуто, різниця з другою позицією становить {price_difference_percent}%")

            # Якщо знаходимо власника після попередніх фільтрів, то виходимо.
            elif user_name == owner and position != 1:
                return (f"{user_name} залишається на позиції {position} по предмету"
                        f" {short_title} з ціною {RED}{unit_price}{RESET}")

            elif unit_price < limit and pattern_name != 'asterisk':
                continue

            logger.info(f"Знайдено конкурента {user_name}"
                        f" на позиції {position} з ціною {unit_price}") if test_mode_logs else None

            new_price = unit_price - change_price_coefficient
            new_price = round(new_price, 6)
            logger.CHANGE_PRICE(f"Перебиваємо {competitor['username']}"
                                f" позиції {position},  на товар {short_title}"
                                f" з {RED}{previous_price}{RESET} на {RED}{new_price}{RESET}")
            return new_price
        else:
            return f"Немає конкурентів на товар {short_title}"

    except Exception as e:
        return {'critical': f"Помилка у функції розрахунку ціни: {e} ціну не змінено"
                            f" на товар {short_title} з id {owner_offer_info['offer_id']} "}


def get_random_price_range():
    price_range = random.uniform(price_range_to, price_range_from)
    price_range = round(price_range, 6)
    return price_range


def calculate_percent_difference(owner_unit_price, second_position_competitor_price):
    price_difference_percent = abs(((second_position_competitor_price - owner_unit_price) /
                                    owner_unit_price) * 100)
    return round(price_difference_percent, 3)

# def asterisk(competitors):
#     owner_offer_info = competitors.pop('owner_offer_info')
#     logger.info(f'owner_offer_info: {owner_offer_info}') if test_mode_logs else None
#     logger.info(f'competitors: {competitors}') if test_mode_logs else None
#
#     previous_price = owner_offer_info['previous_price']
#     short_title = owner_offer_info['short_title']
#     pattern_name = owner_offer_info['pattern_name']
#
#     try:
#         for position, competitor in competitors.items():
#             user_name = competitor['username']
#             unit_price = competitor['unit_price']
#             change_price_coefficient = competitor['change_price_coefficient']
#
#             # Перевіряємо конкурента на присутність в списку ігнорування
#             if user_name in ignore_competitors_for_asterisk:
#                 logger.CHANGE_PRICE(f" Ігноруємо {user_name} на позиції {position} по предмету {short_title}"
#                                     f" з ціною {RED}{unit_price}{RESET}")
#                 continue
#
#             elif user_name == owner and position == 1 and len(competitors) < 2:
#                 return (f"{user_name} на позиції {position} не має конкурентів"
#                         f" по предмету {short_title} з ціною {RED}{unit_price}{RESET}")
#
#             # Перевіряємо на можливість підтягування ціни до другої позиції
#             elif user_name == owner and position == 1 and len(competitors) >= 2:
#
#                 second_position_competitor_price = competitors[2]['unit_price']
#
#                 logger.info(f"second_competitor__{competitors[2]['username']}, price: "
#                             f"{second_position_competitor_price}") if test_mode_logs else None
#
#                 price_difference_percent = calculate_percent_difference(unit_price,
#                                                                         second_position_competitor_price)
#
#                 if (min_difference_percent_to_reduce_the_price_between_first_and_second <= price_difference_percent
#                         <= max_difference_percent_to_reduce_the_price_between_first_and_second):
#
#                     new_price = second_position_competitor_price * (1 - (min_max_change_first_position / 100))
#                     new_price = round(new_price, 6)
#
#                     logger.CHANGE_PRICE(f"Ціну підтягнуто на першій позиції"
#                                         f" з {RED}{unit_price}{RESET} до {RED}{new_price}{RESET}. Різниця з "
#                                         f"другою позицією {RED}{second_position_competitor_price}{RESET}"
#                                         f" становила {price_difference_percent} %")
#                     return new_price
#                 else:
#                     return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
#                             f" з ціною {RED}{unit_price}{RESET}."
#                             f"Ціну не підтягнуто, різниця з другою позицією становить {price_difference_percent}")
#
#             # Якщо знаходимо власника після попередніх фільтрів, то виходимо.
#             elif user_name == owner and position != 1:
#                 return (f"{user_name} залишається на позиції {position} по предмету"
#                         f" {short_title} з ціною {RED}{unit_price}{RESET}")
#
#             elif unit_price < top_minimal_asterisk:
#                 return (f" {owner} має ціну {RED}{previous_price}{RESET} нижчу"
#                         f" за встановленний ліміт {top_minimal_asterisk} для {pattern_name}"
#                         f" Конкурент {user_name} має ціну {RED}{unit_price}{RESET}"
#                         f" яка нижча за ліміт {top_minimal_asterisk}")
#
#             logger.info(f"Знайдено конкурента {user_name}"
#                         f" на позиції {position} з ціною {unit_price}") if test_mode_logs else None
#
#             new_price = unit_price - change_price_coefficient
#             new_price = round(new_price, 6)
#             logger.CHANGE_PRICE(f"Перебиваємо {competitor['username']}"
#                                 f" позиції {position},  на товар {short_title}"
#                                 f" з {RED}{previous_price}{RESET} на {RED}{new_price}{RESET}")
#             return new_price
#         else:
#             return f"Немає конкурентів на товар {short_title}"
#
#     except Exception as e:
#         return {'critical': f"Помилка у функції asterisk: {e} ціну не змінено"
#                             f" на товар {short_title} з id {owner_offer_info['offer_id']} "}
