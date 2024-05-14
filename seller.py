import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров магазина озон, начиная с указанного идентификатора.

    Args:
        last_id (str): Идентификатор последнего полученного товара для постраничной загрузки.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен для доступа к API Ozon.

    Returns:
        dict: Словарь с результатами запроса, содержащий список товаров и информацию о пагинации.

    Examples:
        >>> get_product_list("", "123", "token123")
        {'items': [{'offer_id': '001', 'name': 'Product 1'}], 'last_id': '002', 'total': 50}

    Notes:
        Если `last_id` пуст, начинается загрузка с первого товара в списке. Функция возвращает до 1000 товаров за один вызов.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API не успешен.
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


def get_offer_ids(client_id, seller_token):
    """
    Получает список идентификаторов товаров для магазина на платформе Ozon.

    Args:
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен для доступа к API Ozon.

    Returns:
        list: Список строковых идентификаторов товаров.

    Examples:
        >>> get_offer_ids("123", "token123")
        ['id1', 'id2', 'id3']

    Notes:
        Функция обращается к API Ozon для получения полного списка товаров постранично.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API не успешен.
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


def update_price(prices: list, client_id, seller_token):
    """
    Отправляет запрос на обновление цен товаров в магазине на платформе Ozon.

    Args:
        prices (list): Список словарей с информацией о ценах товаров.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен для доступа к API Ozon.

    Returns:
        dict: Словарь с результатами выполнения запроса, включая статус и ошибки.

    Examples:
        >>> update_price([{"offer_id": "123", "price": 500}], "123", "token123")
        {'status': 'OK'}

    Notes:
        Обратите внимание, что цены должны быть представлены в формате, требуемом API Ozon.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API не успешен.
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


def update_stocks(stocks: list, client_id, seller_token):
    """
    Отправляет запрос на обновление остатков товаров в магазине на платформе Ozon.

    Args:
        stocks (list): Список словарей с информацией об остатках товаров.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен для доступа к API Ozon.

    Returns:
        dict: Словарь с результатами выполнения запроса, включая статус и ошибки.

    Examples:
        >>> update_stocks([{"offer_id": "123", "stock": 10}], "123", "token123")
        {'status': 'OK'}

    Notes:
        Обратите внимание, что данные остатков должны быть представлены в формате, требуемом API Ozon.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API не успешен.
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


def download_stock():
    """
    Загружает и анализирует данные об остатках товаров с внешнего источника.

    Returns:
        list: Список словарей с данными об остатках.

    Examples:
        >>> download_stock()
        [{'Код': '001', 'Количество': 10, 'Цена': "5'990.00 руб."}]

    Notes:
        Функция скачивает архив с данными, извлекает файл Excel и анализирует его содержимое.

    Raises:
        requests.exceptions.HTTPError: Если не удается скачать данные.
        zipfile.BadZipFile: Если скачанный файл не является допустимым архивом ZIP.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Подготавливает данные остатков для загрузки на платформу Ozon.

    Args:
        watch_remnants (list): Список словарей с данными остатков, полученных из внешнего источника.
        offer_ids (list): Список строковых идентификаторов товаров, доступных в магазине.

    Returns:
        list: Список словарей, готовых для отправки в API Ozon для обновления остатков.

    Examples:
        >>> create_stocks([{'Код': '001', 'Количество': '10'}], ['001'])
        [{'offer_id': '001', 'stock': 10}]

    Notes:
        Функция проверяет наличие каждого товара из внешних данных в списке идентификаторов товаров и формирует правильный формат данных.
    """
    # Уберем то, что не загружено в seller
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
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Подготавливает данные цен для загрузки на платформу Ozon.

    Args:
        watch_remnants (list): Список словарей с данными остатков, включая цены, полученных из внешнего источника.
        offer_ids (list): Список строковых идентификаторов товаров, доступных в магазине.

    Returns:
        list: Список словарей, готовых для отправки в API Ozon для обновления цен.

    Examples:
        >>> create_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], ['001'])
        [{'offer_id': '001', 'price': 5990, 'currency_code': 'RUB'}]

    Notes:
        Функция проверяет наличие каждого товара из внешних данных в списке идентификаторов товаров и формирует правильный формат данных для цен.
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
    """
    Преобразует строку с ценой, удаляя все недесятичные символы, кроме цифр.

    Args:
        price (str): Строка с ценой, содержащая числа и возможно другие символы, например, "5'990.00 руб."

    Returns:
        str: Строка, содержащая только числовое представление цены, например "5990".

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("12,890.50")
        '12890'

    Notes:
        Функция не обрабатывает случаи, когда цена представлена в форматах, отличных от принятых для российского рынка
        (например, с использованием точки для разделения тысяч или запятой для копеек).

    Raises:
        ValueError: Если входная строка не содержит числовых символов, функция может генерировать ошибку преобразования.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на части по заданному количеству элементов.

    Args:
        lst (list): Список, который необходимо разделить.
        n (int): Количество элементов в каждой подсписке.

    Returns:
        iter: Итератор, который поочередно возвращает подсписки заданной длины.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

    Notes:
        Последняя возвращаемая подсписка может содержать меньше элементов, если общее количество не делится нацело на `n`.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Асинхронно загружает данные о ценах на товары на платформу Ozon.

    Args:
        watch_remnants (list): Список словарей с данными остатков, включая цены.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен для доступа к API Ozon.

    Returns:
        list: Список словарей с данными о ценах, которые были загружены на платформу.

    Examples:
        >>> await upload_prices([{'Код': '001', 'Цена': "5'990.00 руб."}], '123', 'token123')
        [{'offer_id': '001', 'price': 5990}]

    Notes:
        Данные о ценах делятся на части по 1000 элементов для оптимизации загрузки.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Асинхронно загружает данные об остатках товаров на платформу Ozon.

    Args:
        watch_remnants (list): Список словарей с данными остатках.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен для доступа к API Ozon.

    Returns:
        tuple: Пара, содержащая список загруженных остатков с ненулевым количеством и полный список загруженных остатков.

    Examples:
        >>> await upload_stocks([{'Код': '001', 'Количество': '10'}], '123', 'token123')
        ([{'offer_id': '001', 'stock': 10}], [{'offer_id': '001', 'stock': 10}])

    Notes:
        Данные об остатках делятся на части по 100 элементов для оптимизации загрузки.
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
