import random

from functions.load_config import get_config
from functions.logger_config import logger

'''
 ім'я першого конкурента = competitors[1]['username']
 ціна першого конкурента = competitors[1]['unit_price']
 видимі ціна першого конкурента = competitors[1]['display_price']
 коротка назва товару(для всіх) = competitors['title']
 другий конкурент = competitors[2]
 oстанній конкурент = competitors[-1]
'''
RED = "\033[31m"
RESET = "\033[0m"

config = get_config()

if config:
    top_minimal_values = config.get("top_minimal")
    price_range_values = config.get("price_range")
    difference_percent_to_reduce_the_price_between_first_and_second = (
        config.get("difference_percent_to_reduce_the_price_between_first_and_second"))
    owner = config.get('owner')
    ignore_competitors_for_asterisk = config.get('ignore_competitors_for_asterisk')
    ignore_words = config.get('ignore_words')
    error_top_limit = config.get('error_top_limit')
    min_max_change_first_position = config.get('min_max_change_first_position')
    reduce_price_non_popular_item = config.get('reduce_price_non_popular_item')

    if top_minimal_values:
        top_minimal_plus = top_minimal_values.get("plus")
        top_minimal_star = top_minimal_values.get("star")
        top_minimal_hash = top_minimal_values.get("hash")
        top_minimal_double_parentheses = top_minimal_values.get("double_parentheses")
        top_minimal_tilde = top_minimal_values.get("tilde")
        top_minimal_backtick = top_minimal_values.get("backtick")
        top_minimal_vertical_bar = top_minimal_values.get("vertical_bar")
        top_minimal_double_square_brackets = top_minimal_values.get("double_square_brackets")
        top_minimal_double_angle_brackets = top_minimal_values.get("double_angle_brackets")
        top_minimal_caret = top_minimal_values.get("caret")
        top_minimal_dollar = top_minimal_values.get("dollar")
        top_minimal_ampersand = top_minimal_values.get("ampersand")
        top_minimal_double_percent = top_minimal_values.get("double_percent")

    else:
        print("Ключ 'top_minimal' не знайдено в конфігураційному файлі.")

    if price_range_values:  # Перевірка на існування price_range
        price_range_from = price_range_values.get("price_range_from")
        price_range_to = price_range_values.get("price_range_to")

    else:
        print("Ключ 'price_range' не знайдено в конфігураційному файлі.")
else:
    print("Помилка завантаження конфігурації.")


def asterisk(competitors):
    logger.info(competitors)

    try:
        for position, competitor in competitors.items():
            found_competitor = None
            title = competitor["title"]
            user_name = competitor['username']
            short_title = competitor['short_title']
            display_price = competitor['display_price']
            unit_price = competitor['unit_price']
            previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner),
                                  None)

            # if any(word in title for word in ignore_words):
            #     logger.warning(f"Пропускаємо: Знайдено ігнор слово в {title}")
            #     continue
            # logger.info(f"title__{title}")

            if user_name == owner and position != 1:
                return (f"{user_name} на позиції {position} по предмету"
                        f" {short_title} з ціною {RED}{unit_price}{RESET}")

            elif competitor['username'] == owner and position == 1 and len(competitors) < 2:
                return (f"{user_name} на позиції {position} не має конкурентів"
                        f" по предмету {short_title} з ціною {RED}{unit_price}{RESET}")

            elif competitor['username'] == owner and position == 1 and len(competitors) >= 2:
                owner_unit_price = competitor["unit_price"]
                second_position_competitor_price = competitors[2]["unit_price"]

                price_difference_percent = abs(((second_position_competitor_price - owner_unit_price) /
                                                owner_unit_price) * 100)

                if 5 <= price_difference_percent <= difference_percent_to_reduce_the_price_between_first_and_second:
                    new_price = second_position_competitor_price * (1 - (min_max_change_first_position / 100))
                    new_price = round(new_price, 6)

                    logger.warning(f"Ціну піднято на першій позиції"
                                   f" з {RED}{owner_unit_price}{RESET} до {new_price}{RESET}. Різниця з "
                                   f"другою позицією {RED}{second_position_competitor_price}{RESET}"
                                   f" становила {price_difference_percent} %")
                    return new_price
                else:
                    return (f"{user_name} на позиції {position} по предмету {short_title}"
                            f" з ціною {RED}{unit_price}{RESET}")

            elif competitor['username'] not in ignore_competitors_for_asterisk:
                found_competitor = competitor
                competitor_position = position
                break
            else:
                logger.warning(f" Ігноруємо {user_name} на позиції {position} по предмету {short_title}"
                               f" з ціною {RED}{unit_price}{RESET}")

        if found_competitor is None:
            return None

        logger.info(f"Знайдено конкурента {user_name} на позиції {position}")
        short_title = found_competitor["short_title"]

        unit_price = found_competitor["unit_price"]
        if unit_price > error_top_limit:
            unit_price = display_price
            return (f" Пропускаємо:{user_name} позиції {position} на товар {short_title}"
                    f" з ціною {RED}{unit_price}{RESET} > за ліміт на помилку {error_top_limit}")

        price_rang = get_random_price_range()
        new_price = unit_price - price_rang
        new_price = round(new_price, 6)
        logger.warning(f"Перебиваємо {competitor['username']}"
                       f" позиції {competitor_position},  на товар {short_title}"
                       f" з {RED}{previous_price}{RESET} на {RED}{new_price}{RESET}")
    except Exception as e:
        return {'critical': f"Помилка у функції asterisk: {e} ціну не змінено"}

    return new_price


def plus(competitors):
    """Обробка патерну "+" (плюс)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_plus and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_plus:
                continue

            elif display_price >= top_minimal_plus:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_plus}")


def hash_(competitors):
    """Обробка патерну "#" (решітка)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_hash and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f" по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_hash:
                continue

            elif display_price >= top_minimal_hash:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_hash}")


def star(competitors):
    """Обробка патерну "★" (зірка)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_star and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_star:
                continue

            elif display_price >= top_minimal_star:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_star}")


def double_parentheses(competitors):
    """Обробка патерну "(())" (подвійні круглі дужки)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_double_parentheses and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_double_parentheses:
                continue

            elif display_price >= top_minimal_double_parentheses:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_double_parentheses}")


def tilde(competitors):
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_tilde and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_tilde:
                continue

            elif display_price >= top_minimal_tilde:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_tilde}")


def backtick(competitors):
    """Обробка патерну "`" (зворотний апостроф)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_backtick and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_tilde:
                continue

            elif display_price >= top_minimal_tilde:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_tilde}")


def vertical_bar(competitors):
    """Обробка паттерну "|" (вертикальна риска)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_tilde and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_tilde:
                continue

            elif display_price >= top_minimal_tilde:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_tilde}")


def double_square_brackets(competitors):
    """Обробка патерну "[[ ]]" (подвійні квадратні дужки)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_double_square_brackets and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_double_square_brackets:
                continue

            elif display_price >= top_minimal_double_square_brackets:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_double_square_brackets}")


def double_angle_brackets(competitors):
    """Обробка патерну "<< >>" (подвійні кутові дужки)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_double_angle_brackets and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_double_angle_brackets:
                continue

            elif display_price >= top_minimal_double_angle_brackets:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_double_angle_brackets}")


def caret(competitors):
    """Обробка патерну "^" (каретка)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_caret and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_caret:
                continue

            elif display_price >= top_minimal_caret:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_caret}")


def dollar(competitors):
    """Обробка патерну "$" (долар)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_dollar and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_dollar:
                continue

            elif display_price >= top_minimal_dollar:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_dollar}")


def ampersand(competitors):
    """Обробка патерну "&" (амперсанд)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_ampersand and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_ampersand:
                continue

            elif display_price >= top_minimal_ampersand:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_ampersand}")


def double_percent(competitors):
    """Обробка патерну "%%" (подвійний відсоток)"""
    logger.info(competitors)
    previous_price = next((item['unit_price'] for item in competitors.values() if item['username'] == owner), None)

    try:
        owner_info = {}
        for position, found_competitor in competitors.items():
            display_price = found_competitor['display_price']
            unit_price = found_competitor['unit_price']

            if found_competitor['username'] == owner and position == 1 and unit_price > top_minimal_double_percent and len(
                    competitors) == 1:
                new_price = unit_price * (1 - (reduce_price_non_popular_item / 100))
                return new_price

            if found_competitor['username'] == owner:
                owner_info['position'] = position
                owner_info['unit_price'] = found_competitor['unit_price']
                return (f"{owner} залишається на позиції {owner_info['position']}"
                        f"по товару {found_competitor['short_title']}"
                        f" з ціною {RED}{owner_info['unit_price']}{RESET}")

            elif display_price < top_minimal_double_percent:
                continue

            elif display_price >= top_minimal_double_percent:
                new_price = unit_price

                price_rang = get_random_price_range()
                new_price = new_price - price_rang
                new_price = round(new_price, 6)
                logger.warning(f"Перебиваємо {found_competitor['username']}"
                               f" позиції {position}, на товар {found_competitor['short_title']} "
                               f"з {RED}{previous_price}{RESET} до {RED}{new_price}{RESET}")
                return new_price

    except Exception as e:
        return {'critical': f"Помилка у функції plus: {e} ціну не змінено"}

    else:
        return (f"{owner} залишається на позиції {owner_info['position']}"
                f" з ціною {RED}{owner_info['unit_price']}{RESET}"
                f"на товар {found_competitor['short_title']}"
                f"так як не  знайдено конкурентів на товар {found_competitor['short_title']}"
                f" з ціною більше за {top_minimal_double_percent}")


def get_random_price_range():
    price_rang = random.uniform(price_range_to, price_range_from)
    price_rang = round(price_rang, 6)
    return price_rang


def start():
    pass


if __name__ == '__main__':
    start()
