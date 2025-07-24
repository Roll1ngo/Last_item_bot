import random

from functions.load_config import get_config
from functions.logger_config import logger

RED = "\033[31m"
RESET = "\033[0m"

config = get_config()

if config:

    test_mode_logs = config.get("test_mode_logs")

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
    max_limit_price_for_pull = config.get("max_limit_price_for_pull")

    threshold_price_for_percentage_change = config.get("threshold_price_for_percentage_change")
    change_percents_before_threshold = config.get("change_percents_before_threshold")
    change_percents_after_threshold = config.get("change_percents_after_threshold")

else:
    print("Помилка завантаження конфігурації.")


def general_patterns(competitors):
    owner_offer_info = competitors.pop('owner_offer_info')
    logger.info(f'owner_offer_info: {owner_offer_info}') if test_mode_logs else None
    logger.info(f'competitors: {competitors}') if test_mode_logs else None

    previous_price = owner_offer_info['previous_price']
    short_title = owner_offer_info['short_title']
    pattern_name = owner_offer_info['pattern_name']
    owner_position = owner_offer_info['position']

    # Визначаємо список ігнорованих конкурентів для поточного патерну
    ignore_competitors = ignore_competitors_for_asterisk if pattern_name == 'asterisk' \
        else ignore_competitors_for_other_patterns
    limit = owner_offer_info['limit']

    # Визначаємо індикатор підтягування отримуючи булеве значення
    pull_indicator = get_pull_indicator(owner_position, competitors, ignore_competitors)

    logger.info(f"pull_indicator: {pull_indicator}") if test_mode_logs else None

    logger.warning(f" Для патерну {pattern_name} на знайдено значення ліміту."
                   f" Наразі встановлено стандартне значення {limit}"
                   f"Перевірте конфігураційний файл.") if limit == 0 else None
    logger.info(f"pattern_name: {pattern_name}") if test_mode_logs else None
    logger.info(f"ignore_competitors: {ignore_competitors}") if test_mode_logs else None

    try:
        for position, competitor in competitors.items():
            user_name = competitor['username']
            unit_price = competitor['unit_price']

            # Підбираємо коефіцієнт для перебиття поточного конкурента
            change_price_coefficient = change_percents_before_threshold if \
                unit_price <= threshold_price_for_percentage_change else change_percents_after_threshold

            # Перевіряємо конкурента на присутність в списку ігнорування
            if user_name in ignore_competitors:
                logger.CHANGE_PRICE(f" Ігноруємо {user_name} на позиції {position} по предмету {short_title}"
                                    f" з ціною {RED}{unit_price}{RESET}")
                continue

            # Перевіряємо умови для зниження ціни на непопулярний товар
            elif user_name == owner and position == 1 and len(competitors) == 1:
                return (f"{owner} єдиний продавець товару {short_title} з ціною"
                        f" {RED}{unit_price}{RESET} ")
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
                            f" з ціною {RED}{unit_price}{RESET}"
                            f" так як має останню позицію {owner_position}")

                if unit_price > max_limit_price_for_pull:
                    return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                            f" з ціною {RED}{unit_price}{RESET}"
                            f" вищою за встановленний ліміт {max_limit_price_for_pull} $ для підтягування")

                position_competitor_for_pull = position + 1
                pull_competitor_price = competitors[position_competitor_for_pull]['unit_price']
                pull_competitor_username = competitors[position_competitor_for_pull]['username']
                price_difference_percent = round(calculate_percent_difference(unit_price,
                                                                              pull_competitor_price), 2)
                potential_new_price = pull_competitor_price * (1 - (min_max_change_first_position / 100))
                logger.info(f"unit_price: {unit_price}"
                            f"pull_competitor_username: {pull_competitor_username},"
                            f" pull_competitor_price: {pull_competitor_price},"
                            f"price_difference_percent: {price_difference_percent}"
                            f"potential_new_price: {potential_new_price}") if test_mode_logs else None

                if potential_new_price > max_limit_price_for_pull:
                    return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                            f" з ціною {RED}{unit_price}{RESET}"
                            f" т.я потенційна ціна після підтягування вища"
                            f" за встановленний ліміт {max_limit_price_for_pull} $ для підтягування")

                elif ((min_difference_percent_to_reduce_the_price_between_first_and_second <= price_difference_percent
                        <= max_difference_percent_to_reduce_the_price_between_first_and_second)
                        and potential_new_price < max_limit_price_for_pull):

                    new_price = round(potential_new_price, 6)

                    logger.CHANGE_PRICE(f"Ціну підтягнуто з {position} позиції"
                                        f" з {RED}{unit_price}{RESET} до {RED}{new_price}{RESET}."
                                        f" Різниця з позицією {position_competitor_for_pull}"
                                        f" продавця {pull_competitor_username} становила {price_difference_percent} %"
                                        f" для ціни {RED}{pull_competitor_price}{RESET}")
                    return new_price

                else:
                    return (f"{user_name} залишається на позиції {position} по предмету {short_title}"
                            f" з ціною {RED}{unit_price}{RESET}."
                            f" Ціну не підтягнуто, різниця з позицією {position_competitor_for_pull}"
                            f" становить {price_difference_percent} % для ціни {RED}{pull_competitor_price}{RESET}")

            # Якщо знаходимо власника після попередніх фільтрів, то виходимо.
            elif user_name == owner and position != 1:
                return (f"{user_name} залишається на позиції {position} по предмету"
                        f" {short_title} з ціною {RED}{unit_price}{RESET}")

            elif unit_price < limit and pattern_name != 'asterisk':
                continue

            logger.info(f"Знайдено конкурента {user_name}"
                        f" на позиції {position} з ціною {unit_price}") if test_mode_logs else None

            new_price = unit_price * (1 - (change_price_coefficient/100))
            new_price = round(new_price, 6)
            logger.CHANGE_PRICE(f"Перебиваємо {user_name} позиції {position},  на товар {short_title}"
                                f" з {RED}{previous_price}{RESET} на {RED}{new_price}{RESET}")

            logger.info(f"Ціна конкурента__{RED}{unit_price}{RESET}, відсоток перебиття__{change_price_coefficient} %"
                        f" Змінена ціна__{RED}{new_price}{RESET}") if test_mode_logs else None

            return new_price
        else:
            return f"Немає конкурентів на товар {short_title}"

    # except Exception as e:
    #     return {'critical': f"Помилка у функції розрахунку ціни: {e} ціну не змінено"
    #                         f" на товар {short_title} з id {owner_offer_info['offer_id']} "}
    finally:
        pass


def calculate_percent_difference(owner_unit_price, second_position_competitor_price):
    price_difference_percent = abs(((second_position_competitor_price - owner_unit_price) /
                                    owner_unit_price) * 100)
    return round(price_difference_percent, 3)


def get_pull_indicator(owner_position, competitors, ignored_competitors):
    if owner_position == 1:
        return True if len(competitors) >= 2 else False


    competitors_before_owner = [competitor['username'] for pos, competitor in competitors.items()
                                if pos < owner_position]
    logger.info(f"competitors_before_owner: {competitors_before_owner}") if test_mode_logs else None

    for competitor in competitors_before_owner:
        if competitor not in ignored_competitors:
            return False

    return True
