import io
import logging.config
import os
import re
import zipfile
from environs import Env
import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id: str, client_id: str, seller_token: str) -> dict:
    """Returns product list in JSON format as a dictionary.

    Args:
        last_id (str): ID of the last value on the page.
        client_id (str): String with client_id from Ozon.ru.
        seller_token (str): String with seller_token from Ozon.ru.

    Returns:
        dict: Product list in JSON format as a dictionary.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If client_id or seller_token have wrong key or wrong type.

    Example:
        >>> get_product_list('bnVсbA==', '39481726',
        ... 'w2DDI76ydkuFFq-oX-FC6WXgg0T=wegHg')
        {"result": {"items": [{"product_id": 223681945, "offer_id":
        "136748"}], "total": 1, "last_id": "bnVсbA=="}}
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id: str, seller_token: str) -> list:
    """Returns offer id's for items as a list.

    Args:
        client_id (str): String with client_id from Ozon.ru.
        seller_token (str): String with seller_token from Ozon.ru.

    Returns:
        list: Offer ids for items from Ozon as a list.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If client_id or seller_token have wrong key or wrong type.

    Example:
        >>> get_offer_ids('39481726', 'w2DDI76ydkuFFq-oX-FC6WXgg0T=wegHg')
        [71405, 64151, 71698]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id: str, seller_token: str) -> dict:
    """Returns update price result in JSON format as a dictionary.

    Args:
        prices (list): List of dictionaries containing id, price, etc.
        client_id (str): String with client_id from Ozon.ru.
        seller_token (str): String with seller_token from Ozon.ru.

    Returns:
        dict: Update price result in JSON format as a dictionary.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If input data is not a list and if list and if
        dictionaries in list does not contain the correct structure.

    Example:
        >>> prices = [{'auto_action_enabled': 'UNKNOWN', 'currency_code':
        ... 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}]
        >>> client_id = '1234523434'
        >>> seller_token = '32e23d23dasd23(#@N-213e'
        >>> update_price(prices, client_id, seller_token)
        {"result": [{"offer_id": "123", "updated": true}]}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id: str, seller_token: str):
    """Returns upload result in JSON format as a dictionary.

    Args:
        stocks (list): Contains dictionares with offer_id and stock data.
        client_id (str): String with client_id from Ozon.ru".
        seller_token (str): String with seller_token from Ozon.ru".

    Returns:
        dict:Dictionary containing upload result.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If input data is not a list and if list and if
        dictionaries in list does not contain the correct structure.

    Example:
        >>> stock_list = [{'offer_id': 123, 'stock': 5}]
        >>> client_id = '1234523434'
        >>> seller_token = '32e23d23dasd23(#@N-213e'
        >>> update_stocks(stock_list, client_id, seller_token)
        {"result": [{"offer_id": 123,"updated": true }]}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock() -> list:
    """Returns a list of products as a list of dictionaries.

    Returns:
        dict:Dictionary containing stock information.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If you have any problem with archive

    Example:
        >>> print(download_stock())
        [{'код': 68122, 'Наименование товара': 'BA-110X-1A',
        'Цена': '17'990.00 руб.', 'Количество': '8'}]
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids) -> list:
    """Returns offer_id and count of products as a list of dictionaries.

    Args:
        watch_remnants (list): List with products info.
        offer_ids (list): list with offer id's.

    Returns:
        list:A list of dictionaries containing stock information.

    Raises:
        Exception: If watch_remnants or offer_ids is empty.

    Example:
        >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Количество": '5'}, {"Код": '987', "Наименование": "Product B",
        ... "Количество": '10'}]
        >>> offer_ids = ['123', '789']
        >>> print(create_stocks(watch_remnants, offer_ids))
        [{'offer_id': '123', 'stock': 5},
        {'offer_id': '789', 'stock': 0}]
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants: list, offer_ids):
    """Returns a dictionaries of products as a list.

    Args:
        watch_remnants (list): List with products info.
        offer_ids (list): list with offer id's.

    Returns:
        list:A list of dictionaries containing product information.

    Raises:
        Exception: If watch_remnants or offer_ids is empty.

    Example:
        >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Цена": "5'990.00 руб."}, {"Код": '987', "Наименование":
        ... "Product B", "Цена": "10'990.00 руб."}]
        >>> items_id = ['123', '789']
        >>> print(create_prices(watch_remnants, items_id))
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
        'offer_id': '123', 'old_price': '0', 'price': '5990'}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Returns the converted price as a string value.

    Args:
        price (str): String with price in format "5'990.00 руб.".

    Returns:
        str: Converted price as a string without separators and symbols.

    Raises:
        Exception: If the input string does not contain digits.

    Example:
        >>> price_conversion("5'990.00 руб.")
        "5990"
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Splits a list into sublists of n elements.

    Args:
        lst (list): Original list.
        n (int): Maximum number of element in each sublist.

    Yields:
        list: Sublists, contains not more than n elements.

    Raises:
        Exception: If input data is not a list

    Eample:
        >>> numbers = [1, 2, 3, 4, 5, 6]
        >>> print(divide(numbers, 3))
        [1, 2, 3]
        [4, 5, 6]
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants: list, client_id, seller_token):
    """Update prices and return a dictionaries of products as a list.

    Args:
        watch_remnants (list): List with products info.
        client_id (str): String with client_id from Ozon.ru.
        seller_token (str): String with seller_token from Ozon.ru.

    Returns:
        str: Products as a list of dictionaries.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If watch_remnants is empty.

    Example:
         >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Цена": "5'990.00 руб."}, {"Код": '987', "Наименование":
        ... "Product B", "Цена": "10'990.00 руб."}]
        >>> client_id = '1234523434'
        >>> seller_token = '32e23d23dasd23(#@N-213e'
        >>> print(upload_prices(watch_remnants, client_id, seller_token))
         [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
        'offer_id': '123', 'old_price': '0', 'price': '5990'}]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants: list, client_id, seller_token):
    """Update prices and returns information about stock as a tuple with 2 lists.

    Args:
        watch_remnants (list): List with products info.
        client_id (str): String with client_id from Ozon.ru.
        seller_token (str): String with seller_token from Ozon.ru.

    Returns:
        Tuple containing 2 lists.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If watch_remnants is empty.

     Example:
         >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Цена": "5'990.00 руб."}, {"Код": '987', "Наименование":
        ... "Product B", "Цена": "10'990.00 руб."}]
        >>> client_id = '1234523434'
        >>> seller_token = '32e23d23dasd23(#@N-213e'
        >>> print(upload_stocks(watch_remnants, client_id, seller_token))
        ([{'offer_id': '123', 'stock': 5}],
        [{'offer_id': '123', 'stock': 5},
        {'offer_id': '789', 'stock': 0}])
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
