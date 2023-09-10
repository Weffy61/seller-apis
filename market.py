import datetime
import logging.config
from environs import Env
from seller import download_stock
import requests
from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Returns dict with item details.

    Args:
        page (str): String with page_token or empty string.
        campaign_id (str): String with campaign id.
        access_token (str): String with market token from Yandex Market.

    Returns:
        dict: Item details.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If campaign_id or access_token have wrong key or wrong type.

    Example:
        >>> page = ''
        >>> campaign_id = 'campaign_id'
        >>> access_token = 'access_token'
        >>> print(get_product_list(page, campaign_id, access_token)
        ... .get('offerMappingEntries'))
        [
        {
        "offer": {
            "name": "Ударная дрель Makita HP1630, 710 Вт",
            "shopSku": "string",
            "category": "string",
            "vendor": "LEVENHUK",
            "vendorCode": "VNDR-0005A",
            "description": "string",
            "id": "string",
            "feedId": 0,
            "price": 0,
            "barcodes": [46012300000000],
            "urls": ["string"],
            "pictures": ["string"],
            "manufacturer": "string",
            "manufacturerCountries": ["string"],
            "minShipment": 0,
            "transportUnitSize": 0,
            "quantumOfSupply": 0,
            "deliveryDurationDays": 0,
            "boxCount": 0,
            "customsCommodityCodes": ["string"],
            "weightDimensions": {
                "length": 65.55,
                "width": 50.7,
                "height": 20,
                "weight": 1.001,
            },
            "supplyScheduleDays": ["MONDAY"],
            "shelfLifeDays": 0,
            "lifeTimeDays": 0,
            "guaranteePeriodDays": 0,
            "processingState": {
                "status": "UNKNOWN",
                "notes": [{"type": "ASSORTMENT", "payload": "string"}],
            },
            "availability": "ACTIVE",
            "shelfLife": {"timePeriod": 0, "timeUnit": "HOUR",
            "comment": "string"},
            "lifeTime": {"timePeriod": 0, "timeUnit": "HOUR",
            "comment": "string"},
            "guaranteePeriod": {
                "timePeriod": 0,
                "timeUnit": "HOUR",
                "comment": "string",
            },
            "certificate": "string",
        },
        "mapping": {"marketSku": 0, "modelId": 0, "categoryId": 0},
        "awaitingModerationMapping":
        {"marketSku": 0, "modelId": 0, "categoryId": 0},
        "rejectedMapping": {"marketSku": 0, "modelId": 0, "categoryId": 0},
        }
        ]
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Updates stocks on the site.

    Args:
        stocks (list): Contains dictionares with offer_id and stock data.
        campaign_id (str): String with campaign id.
        access_token (str): String with market token from Yandex Market.

    Returns:
        dict: Upload result in JSON format as a dictionary.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If stocks is not a list, and if list and
        dictionaries in list does not contain the correct structure.

    Example:
        >>> stock_list = [{'offer_id': 123, 'stock': 5}]
        >>> campaign_id = 'campaign_id'
        >>> access_token = 'access_token'
        >>> update_stocks(stock_list, campaign_id, access_token)
        {"status": "OK"}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Updates price for your items in Yandex Market.

    Args:
        prices (list): List of dictionaries containing id, price, etc.
        campaign_id (str): String with campaign id.
        access_token (str): String with market token from Yandex Market.

    Returns:
        dict: Update price result in JSON format as a dictionary.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If input data is not a list and if list and if
        dictionaries in list does not contain the correct structure.

    Example:
         >>> price = [{'id': '123', 'price':
         ... {'value': 5990, 'currencyId': 'RUR'}}]
        >>> campaign_id = 'campaign_id'
        >>> access_token = 'access_token'
        >>> update_price(price, campaign_id, access_token)
        {"status": "OK"}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Returns product SKUs as a list.

    Args:
        campaign_id (str): String with campaign id.
        market_token (str) : String with market token from Yandex Market.

    Returns:
        list: Prodict SKUs from Yandex Market.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If campaign_id or market_token have wrong key or wrong type.

    Example:
        >>> get_offer_ids('campaign_id', 'market_token')
        ['71405', '64151', '71698']
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Returns dictionaries with info of products as a list.

    Args:
        watch_remnants (list): List with products info.
        offer_ids (list): List with product SKUs.
        warehouse_id (str): String with warehouse id.

    Returns:
        list: Dictionaries with info of products.

    Raises:
        Exception: If watch_remnants or offer_ids is empty.

    Example:
        >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Количество": '5'}, {"Код": '987', "Наименование": "Product B",
        ... "Количество": '10'}]
        >>> offer_ids = ['123', '789']
        >>> print(create_stocks(watch_remnants, offer_ids, 'warehouse_id'))
        [{'sku': '123', 'warehouseId': 'warehouse_id', 'items':
        [{'count': 5, 'type': 'FIT', 'updatedAt': '2023-09-09T21:07:47Z'}]},
        {'sku': '789', 'warehouseId': 'warehouse_id', 'items':
        [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-09-09T21:07:47Z'}]}]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Returns a dictionaries of products as a list.

    Args:
        watch_remnants (list): List with products info.
        offer_ids (list): list with offer id's.

    Returns:
        list: Dictionaries containing product information.

    Raises:
        Exception: If watch_remnants or offer_ids is empty.

    Example:
        >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Цена": "5'990.00 руб."}, {"Код": '987', "Наименование":
        ... "Product B", "Цена": "10'990.00 руб."}]
        >>> offer_ids = ['123', '789']
        >>> print(create_prices(watch_remnants, offer_ids))
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Update prices on the site.

    Args:
        watch_remnants (list): List with products info.
        campaign_id (str): String with campaign id.
        market_token (str) : String with market token from Yandex Market.

    Returns:
        list: Dictionaries with product data.

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If watch_remnants is empty.

    Example:
        >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Цена": "5'990.00 руб."}, {"Код": '987', "Наименование":
        ... "Product B", "Цена": "10'990.00 руб."}]
        >>> campaign_id = 'campaign_id'
        >>> market_token = 'market_token'
        >>> print(upload_prices(watch_remnants, campaign_id, market_token))
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Update prices and returns information about stock as a tuple with 2 lists.

    Args:
        watch_remnants (list): List with products info.
        campaign_id (str): String with campaign id.
        market_token (str): String with market token from Yandex Market.
        warehouse_id (str): String with warehouse id.

    Returns:
        tuple: 1st list with SKU where stock > 0, 2nd all SKUs

    Raises:
        requests.exceptions.ReadTimeout: If the request timed out.
        requests.exceptions.ConnectionError: If there is an error with
        the connection.
        Exception: If watch_remnants is empty or wrong access api tokens.

    Example:
         >>> watch_remnants = [{"Код": '123', "Наименование": "Product A",
        ... "Цена": "5'990.00 руб."}, {"Код": '987', "Наименование":
        ... "Product B", "Цена": "10'990.00 руб."}]
        >>> campaign_id = 'campaign_id'
        >>> market_token = 'market_token'
        >>> warehouse_id = 'warehouse_id'
        >>> print(upload_stocks(watch_remnants, campaign_id, market_token,
        ... warehouse_id))
        (
    [
        {
            "sku": "123",
            "warehouseId": "warehouse_id",
            "items": [{"count": 5, "type": "FIT", "updatedAt":
            "2023-09-09T21:07:47Z"}],
        }
    ],
    [
        {
            "sku": "123",
            "warehouseId": "warehouse_id",
            "items": [{"count": 5, "type": "FIT",
            "updatedAt": "2023-09-09T21:07:47Z"}],
        },
        {
            "sku": "789",
            "warehouseId": "warehouse_id",
            "items": [{"count": 0, "type": "FIT",
            "updatedAt": "2023-09-09T21:07:47Z"}],
        },
    ])
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
