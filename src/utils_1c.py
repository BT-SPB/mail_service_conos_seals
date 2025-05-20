import copy
import base64
from typing import Callable
from functools import wraps

import requests
from requests.auth import HTTPBasicAuth

from config import CONFIG
from src.logger import logger

KAPPA_URL = "http://kappa5.group.ru:81/ca/hs/interaction/"
LOCAL_URL = "http://10.10.0.10:81/ca/hs/interaction/"


def cache_http_requests(func: Callable) -> Callable:
    """
    Декоратор для кэширования HTTP-запросов на основе аргументов запроса.

    Args:
        func (Callable): Функция, результат которой необходимо кэшировать.

    Returns:
        Callable: Обёрнутая функция с кэшированием.
    """

    cache: dict[str, list | dict | None] = {}
    max_cache_size = 40

    @wraps(func)
    def wrapper(function: str, *args: str, **kwargs) -> list | dict | None:
        # Формируем ключ кэша из названия функции и аргументов
        function_args = "_".join(args)
        url_cache_key = f"{function}_{function_args}"

        # Проверка, есть ли результат в кэше
        if url_cache_key in cache:
            logger.debug("💾 Получение результата из кэша.")
            return cache[url_cache_key]

        # Выполнение оригинальной функции
        result = func(function, *args, **kwargs)
        cache[url_cache_key] = result

        # Ограничение размера кэша
        if len(cache) > max_cache_size:
            cache.pop(next(iter(cache)))

        return result

    return wrapper


@cache_http_requests
def cup_http_request(
        function: str,
        *args: str,
        kappa: bool = False,
        encode: bool = True,
        user_1c: str = CONFIG.USER_1C,
        password_1c: str = CONFIG.PASSWORD_1C,
) -> list | dict | None:
    """
    Выполняет GET-запрос к серверу 1С и возвращает результат в формате JSON.

    Функция сначала пытается выполнить запрос к основному серверу, и при ошибке
    делает повторную попытку на резервный сервер. Поддерживает кодирование
    параметров запроса в base64 для URL-безопасности.

    Args:
        function: Название вызываемой функции/метода API на сервере 1С
        *args: Позиционные аргументы, передаваемые в URL
        kappa: Флаг, определяющий приоритет серверов (основной/резервный)
        encode: Если True, аргументы кодируются в base64
        user_1c: Логин пользователя для базовой авторизации
        password_1c: Пароль пользователя для базовой авторизации

    Returns:
        list | dict | None: Ответ сервера в формате JSON, если успешен. Иначе — None.
    """
    # Определяем функцию кодирования аргументов: base64 или без изменений
    encode_func: Callable[[str], str] = (
        (lambda x: base64.urlsafe_b64encode(x.encode()).decode()) if encode else
        (lambda x: x)
    )

    # Формируем строку аргументов, кодируя их при необходимости и соединяя через слеш
    function_args: str = "/".join(encode_func(arg) for arg in args)

    # Список URL-адресов в порядке приоритета
    urls = [
        f"{KAPPA_URL if kappa else LOCAL_URL}{function}/{function_args}",
        f"{LOCAL_URL if kappa else KAPPA_URL}{function}/{function_args}"
    ]

    # Пытаемся последовательно выполнить запросы
    for url in urls:
        try:
            logger.debug(f"🌐 Отправка GET-запроса на {url}")

            # Выполняем GET-запрос с таймаутом 10 секунд
            response = requests.get(
                url,
                auth=HTTPBasicAuth(user_1c, password_1c),
                timeout=30
            )

            if response.status_code == 200:
                # Парсим JSON-ответ и возвращаем его
                result = response.json()
                logger.debug(f"✔️ Успешный ответ от сервера: {result}")
                return result
            else:
                logger.warning(
                    f"⚠️ Ошибка при запросе к {url}. "
                    f"Код: {response.status_code}, Причина: {response.reason}"
                )
        except Exception as e:
            # Логируем сетевое исключение и продолжаем с резервным сервером
            logger.exception(f"⛔ Сетевая ошибка при запросе к {url}: {e}")
            continue


def remap_production_data(data: dict[str, any]) -> None:
    """
    Подготавливает словарь с данными для отправки в 1С, переименовывая поля и удаляя ненужные.

    Функция изменяет входной словарь, подготавливая его для отправки на сервер 1С.
    Удаляет ненужные поля и переименовывает ключи в соответствии с требованиями системы.
    """
    # Переименование ключей верхнего уровня с использованием значений по умолчанию
    data["ИмпМорскаяПеревозкаДатаПолученияДУ"] = data.pop("document_created_datetime", "")
    data["ИмпМорскаяПеревозкаНомерРейсаФидер"] = data.pop("voyage_number", "")
    # Удаление поля document_type
    data.pop("document_type", None)

    # Обработка списка контейнеров
    for container in data.get("containers", []):
        # Переименование ключей в словаре контейнера
        container["ИмпМорскаяПеревозкаНомерПломбы"] = container.pop("seals", [])
        container["ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера"] = container.pop("upload_datetime", "")
        # Удаление поля note
        container.pop("note", None)


def send_production_data(
        data_source: dict[str, any],
        kappa: bool = False,
        user_1c: str = CONFIG.USER_1C,
        password_1c: str = CONFIG.PASSWORD_1C,
) -> bool:
    """
    Отправляет производственные данные (в формате JSON) на сервер 1С с авторизацией.

    В зависимости от флага `kappa`, сначала пытается отправить данные на один сервер,
    при неудаче — повторяет попытку на резервный.

    Args:
        data_source (dict): Словарь с производственными данными, которые необходимо отправить
        Ожидается следующая структура:
        {
            "bill_of_lading": str,            # Номер коносамента
            "document_created_datetime": str, # Дата ДО
            "voyage_number": str,             # Номер рейса
            "transaction_numbers": list[str], # Список номеров и дат транзакций, полученных с помощью TransactionNumberFromBillOfLading
            "source_file_name": str,          # Название исходного файла
            "source_file_base64": str,        # Исходный файл, закодированный в base64
            "containers": [                   # Список контейнеров
                {
                    "container": str,         # Номер контейнера
                    "seals": list[str],       # Список пломб (одна или несколько строк)
                    "upload_datetime": str    # Дата выгрузки
                },
                ...
            ]
        }

        Пример корректного значения:
            {
                "bill_of_lading": "VX75EA25000897",
                "transaction_numbers": ["АА-0095444 от 14.04.2025", "АА-0095445 от 15.04.2025"],
                "source_file_name": "КС_VX75EA25000897.pdf",
                "source_file_base64": "JVBERi0xLjcKJeLjz9MK...",
                "containers": [
                    {
                        "container": "DFTU1001462",
                        "seals": ["22528791", "2252880"]
                    },
                    {
                        "container": "DFTU1001502",
                        "seals": ["2117691"]
                    }
                ]
            }

        kappa: Если True — основным сервером будет `KAPPA_URL`, иначе `LOCAL_URL`
        user_1c: Имя пользователя для базовой авторизации
        password_1c: Пароль пользователя для базовой авторизации

    Returns:
        True - при успешной отправке всех данных на сервер. False - при неудаче хотя бы одной отправки.
    """
    # Создаем глубокую копию входных данных, чтобы избежать изменения оригинала
    data = copy.deepcopy(data_source)

    # Подготовка данных: переименование и удаление полей для соответствия формату 1С
    remap_production_data(data)

    # Имя функции на сервере 1С
    function_name: str = "SendProductionDataToTransaction"

    # Определяем порядок серверов в зависимости от флага kappa
    urls = [
        f"{KAPPA_URL if kappa else LOCAL_URL}{function_name}",
        f"{LOCAL_URL if kappa else KAPPA_URL}{function_name}"
    ]

    # Заголовки для передачи JSON с указанием кодировки
    headers = {"Content-Type": "application/json; charset=utf-8"}

    # Извлекаем список номеров транзакций, удаляя их из словаря данных
    transaction_numbers: list[str] = data.pop("transaction_numbers", [])

    # Флаг успешной отправки хотя бы одной транзакции
    success_flag = False

    # Для каждого номера сделки отправляем отдельный запрос
    for transaction_number in transaction_numbers:
        # Добавляем текущий номер транзакции в данные для отправки
        data["transaction_number"] = transaction_number

        for url in urls:
            try:
                logger.debug(f"🌐 Попытка отправки данных на {url} для транзакции {transaction_number}")
                # Выполняем POST-запрос с максимальным таймаутом 60 секунд
                response = requests.post(
                    url,
                    auth=HTTPBasicAuth(user_1c, password_1c),
                    headers=headers,
                    json=data,
                    timeout=60
                )

                if response.status_code == 200:
                    logger.debug(
                        f"✔️ Данные успешно отправлены в сделку: {transaction_number}. "
                        f"Ответ: {response.text or 'пустой'}"
                    )
                    success_flag = True
                    break  # Прерываем цикл по серверам при успехе
                else:
                    # Логируем неуспешный ответ сервера
                    logger.warning(
                        f"⚠️ Ошибка при отправке данных для транзакции {transaction_number}. "
                        f"Код: {response.status_code}, Ответ: {response.text}"
                    )

            except requests.exceptions.RequestException as e:
                # Логируем исключение при сетевой ошибке
                logger.exception(f"⛔ Сетевая ошибка при отправке на {url}: {e}")
                continue  # Пробуем резервный сервер

    return success_flag

# if __name__ == "__main__":
#     from src.utils import read_json, write_json
#
#     # data_json = read_json(r"C:\Users\Cherdantsev\Documents\develop\OCR_CONOS_FILES\large.json")
#     # send_production_data(data_json, kappa=True)
#     # print(data_json)
#
#     data_json = read_json(
#         r"C:\Users\Cherdantsev\Documents\develop\OCR_CONOS_FILES\WORKFLOW\SUCCESS\test_out_1\ДУ_EGML001367.pdf.json")
#     remap_production_data(data_json)
#     write_json(r"C:\Users\Cherdantsev\Documents\develop\OCR_CONOS_FILES\WORKFLOW\SUCCESS\test_out_1\new.json",
#                data_json)
#
#     # func = r'TransactionNumberFromBillOfLading'
#     # arg = r'MEDUAS937386'
#     # tn = cup_http_request(func, arg)
#     # print(tn)
#     #
#     # func = "GetTransportPositionNumberByTransactionNumber"
#     # print(cup_http_request(func, tn[-1].split()[0], encode=False))
