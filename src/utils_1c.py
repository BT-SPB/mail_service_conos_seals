from typing import Callable
from functools import wraps

import base64
import requests
from requests.auth import HTTPBasicAuth

from src.logger import logger
from config import CONFIG


def cache_http_requests(func):
    """ Декоратор для кэширования запросов на основе URL """

    cache = {}
    max_cache_size = 40

    @wraps(func)
    def wrapper(function, *args, **kwargs):
        # Формируем ключ кэша из функции + "_" + аргументы
        function_args = r'_'.join(args)
        url_cache_key = function + r'_' + function_args

        # Проверяем, есть ли результат в кэше для данного URL
        if url_cache_key in cache:
            logger.print("Получение результата из кэша...")
            return cache[url_cache_key]

        # Выполняем запрос и сохраняем результат в кэше
        result = func(function, *args, **kwargs)
        cache[url_cache_key] = result

        if len(cache) > max_cache_size:
            cache.pop(next(iter(cache)))

        return result

    return wrapper


@cache_http_requests
def cup_http_request(
        function,
        *args,
        kappa=False,
        encode_off=False,
        user_1c: str = CONFIG.USER_1C,
        password_1c: str = CONFIG.PASSWORD_1C,
) -> list | dict | None:
    # Определение серверов
    if kappa:
        primary_base = r'http://kappa5.group.ru:81/ca/hs/interaction/'
        secondary_base = r'http://10.10.0.10:81/ca/hs/interaction/'
    else:
        primary_base = r'http://10.10.0.10:81/ca/hs/interaction/'
        secondary_base = r'http://kappa5.group.ru:81/ca/hs/interaction/'

    if encode_off:
        encode_func: Callable = lambda x: x
    else:
        encode_func: Callable = lambda x: base64.urlsafe_b64encode(x.encode()).decode()

    function_args = r'/'.join(map(encode_func, args))

    try:
        # Формируем URL для первого сервера
        primary_url = primary_base + function + r'/' + function_args
        logger.print(f"Попытка запроса: {primary_url}")

        # Попытка отправить запрос на первый сервер
        response = requests.get(primary_url, auth=HTTPBasicAuth(user_1c, password_1c))

        # Если первый запрос успешен, возвращаем результат
        if response.status_code == 200:
            return response.json()
        else:
            logger.print(f"Ошибка при запросе к первому серверу: {response.status_code} - {response.reason}")
    except Exception as error:
        logger.print(error)

    try:
        # Формируем URL для второго сервера
        secondary_url = secondary_base + function + r'/' + function_args
        logger.print(f"Попытка запроса ко второму серверу: {secondary_url}")

        # Попытка отправить запрос на второй сервер
        response = requests.get(secondary_url, auth=HTTPBasicAuth(user_1c, password_1c))

        # Возвращаем результат, если успешен
        if response.status_code == 200:
            return response.json()
        else:
            logger.print(f"Ошибка при запросе ко второму серверу: {response.status_code} - {response.reason}")
            return None
    except Exception as error:
        logger.print(error)
        return None

if __name__ == "__main__":
    CBL = r'TransactionNumberFromBillOfLading'
    BL = r'CustomsTransactionFromBillOfLading'
    arg = r'MEDUFE573169'
    print(cup_http_request(CBL, arg))
    print(cup_http_request(BL, arg))
    # print(cup_http_request(BL, arg, kappa=True))