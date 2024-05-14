import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров, участвующих в кампании на Яндекс.Маркете.

    Args:
        page (str): Токен страницы для постраничной навигации.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркете.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Словарь с результатами запроса, включая список товаров и пагинацию.

    Examples:
        >>> get_product_list("", "123", "access_token123")
        {'result': {'offerMappingEntries': [{'shopSku': '001', 'marketSku': '002'}], 'paging': {'nextPageToken': 'token'}}}

    Raises:
        requests.exceptions.HTTPError: Если запрос не удался или вернул ошибку.
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
    """
    Обновляет информацию о наличии товаров в кампании на Яндекс.Маркете.

    Args:
        stocks (list): Список словарей с данными о наличии товаров.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркете.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Словарь с результатами обновления.

    Examples:
        >>> update_stocks([{'sku': '001', 'warehouseId': 'wh1', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2020-10-10T10:00:00Z'}]}, "123", "access_token123")
        {'status': 'OK'}

    Raises:
        requests.exceptions.HTTPError: Если запрос не удался или вернул ошибку.
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
    """
    Обновляет цены товаров в кампании на Яндекс.Маркете.

    Args:
        prices (list): Список словарей с информацией о ценах товаров.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркете.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Словарь с результатами обновления цен.

    Examples:
        >>> update_price([{'id': '001', 'price': {'value': 500, 'currencyId': 'RUR'}}, "123", "access_token123")
        {'status': 'OK'}

    Raises:
        requests.exceptions.HTTPError: Если запрос не удался или вернул ошибку.
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
    """
    Получает список артикулов товаров из кампании на Яндекс.Маркете.

    Args:
        campaign_id (str): Идентификатор кампании на Яндекс.Маркете.
        market_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        list: Список строковых идентификаторов товаров (shopSku).

    Examples:
        >>> get_offer_ids("123", "access_token123")
        ['001', '002', '003']

    Notes:
        Для получения полного списка артикулов функция может выполнять множество последовательных запросов,
        если товаров больше, чем может быть получено за один запрос.
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
    """
    Формирует данные об остатках товаров для загрузки на Яндекс.Маркет.

    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        offer_ids (list): Список артикулов товаров, которые присутствуют на Яндекс.Маркете.
        warehouse_id (str): Идентификатор склада на Яндекс.Маркете.

    Returns:
        list: Список словарей, готовых для отправки в API Яндекс.Маркета.

    Examples:
        >>> create_stocks([{'Код': '001', 'Количество': '10'}], ['001'], 'wh1')
        [{'sku': '001', 'warehouseId': 'wh1', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2020-10-10T10:00:00Z'}]}]

    Notes:
        Скрипт преобразует количество товаров в соответствии с доступными данными и убирает товары, которых нет в списке offer_ids.
    """
    # Уберем то, что не загружено в market
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
    # Добавим недостающее из загруженного:
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
    """
    Формирует данные о ценах товаров для загрузки на Яндекс.Маркет.

    Args:
        watch_remnants (list): Список словарей с данными остатков и ценах товаров.
        offer_ids (list): Список артикулов товаров, которые присутствуют на Яндекс.Маркете.

    Returns:
        list: Список словарей, готовых для отправки в API Яндекс.Маркета, с обновленными ценами.

    Examples:
        >>> create_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], ['001'])
        [{'id': '001', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

    Notes:
        Скрипт преобразует формат цены в число, удаляя нечисловые символы и оставляя только целую часть цены.
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
    """
    Асинхронно загружает обновленные цены на товары в кампанию на Яндекс.Маркет.

    Args:
        watch_remnants (list): Список словарей с данными остатков и ценах товаров.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркете.
        market_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        list: Список словарей с обновленными ценами товаров.

    Examples:
        >>> await upload_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], '123', 'access_token123')
        [{'id': '001', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

    Notes:
        Функция разделяет список на пакеты и выполняет асинхронные запросы для ускорения загрузки данных.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Асинхронно загружает обновленные данные об остатках товаров в кампанию на Яндекс.Маркет.

    Args:
        watch_remnants (list): Список словарей с данными об остатках товаров.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркете.
        market_token (str): Токен доступа к API Яндекс.Маркета.
        warehouse_id (str): Идентификатор склада на Яндекс.Маркете.

    Returns:
        tuple: Пара, содержащая список загруженных остатков с ненулевым количеством и полный список загруженных остатков.

    Examples:
        >>> await upload_stocks([{'Код': '001', 'Количество': '10'}], '123', 'access_token123', 'wh1')
        ([{'sku': '001', 'warehouseId': 'wh1', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2020-10-10T10:00:00Z'}]}, [{'sku': '001', 'warehouseId': 'wh1', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2020-10-10T10:00:00Z'}]}])

    Notes:
        Функция разделяет список на пакеты и выполняет асинхронные запросы для ускорения загрузки данных.
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
    """
    Главная функция, запускающая процесс управления товарами на Яндекс.Маркете.

    Производит загрузку остатков и цен, используя данные окружения для доступа к API.

    Examples:
        >>> main()
        Запускает процедуру обновления остатков и цен.

    Notes:
        Функция использует переменные окружения для чтения токенов и идентификаторов.
        Обрабатывает возможные ошибки в процессе выполнения, включая таймауты и ошибки соединения.
    """
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
