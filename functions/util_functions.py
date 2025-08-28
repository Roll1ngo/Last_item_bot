import requests


def calculate_percent_difference(owner_unit_price, second_position_competitor_price):
    price_difference_percent = abs(((second_position_competitor_price - owner_unit_price) /
                                    owner_unit_price) * 100)
    return round(price_difference_percent, 3)



if __name__ == "__main__":
    pass

    # update_chrome_user_agent()