def calculate_percent_difference(owner_unit_price, second_position_competitor_price):
    price_difference_percent = abs(((second_position_competitor_price - owner_unit_price) /
                                    owner_unit_price) * 100)
    return round(price_difference_percent, 3)

def get_pull_indicator(owner_position, competitors, ignored_competitors):
    if owner_position == 1:
        return True if len(competitors) >= 2 else False
