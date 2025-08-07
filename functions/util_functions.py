import requests
from bs4 import BeautifulSoup


def calculate_percent_difference(owner_unit_price, second_position_competitor_price):
    price_difference_percent = abs(((second_position_competitor_price - owner_unit_price) /
                                    owner_unit_price) * 100)
    return round(price_difference_percent, 3)

def get_pull_indicator(owner_position, competitors, ignored_competitors):
    if owner_position == 1:
        return True if len(competitors) >= 2 else False

# def update_chrome_user_agent():
#     try:
#         url = "https://useragentapi.com/api/v4/json/YOUR_API_KEY/latest_user_agents"
#
#         headers = {
#             "User-Agent": "Mozilla/5.0"
#         }
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#         print(response.json())
#
#         soup = BeautifulSoup(response.text, 'html.parser')
#
#         # Знаходимо перший <code> з валідним user-agent
#         code_blocks = soup.find_all("code")
#         latest_user_agent = None
#
#         for code in code_blocks:
#             if "Mozilla/5.0" in code.text and "Chrome/" in code.text:
#                 latest_user_agent = code.text.strip()
#                 break
#
#         if not latest_user_agent:
#             raise ValueError("Не вдалося знайти user-agent у відповіді.")
#
#         print(f"Останній user-agent Chrome: {latest_user_agent}")
#         return latest_user_agent
#
#     except requests.exceptions.RequestException as e:
#         print(f"Помилка при отриманні останнього user-agent Chrome: {e}")
#     except ValueError as e:
#         print(f"Помилка при парсингу сторінки: {e}")

if __name__ == "__main__":
    pass

    # update_chrome_user_agent()