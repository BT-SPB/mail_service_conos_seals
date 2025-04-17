from typing import Callable
from functools import wraps
import base64

import requests
from requests.auth import HTTPBasicAuth

from src.logger import logger
from config import CONFIG

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
        encode: Кодирования аргументов base64
        user_1c: Логин пользователя для базовой авторизации
        password_1c: Пароль пользователя для базовой авторизации

    Returns:
        list | dict | None: Ответ сервера в формате JSON, если успешен. Иначе — None.
    """

    # Определение порядка серверов: основной и резервный
    primary_base = KAPPA_URL if kappa else LOCAL_URL
    secondary_base = LOCAL_URL if kappa else KAPPA_URL

    # Выбор функции кодирования: base64 или passthrough
    encode_func: Callable[[str], str] = (
        (lambda x: base64.urlsafe_b64encode(x.encode()).decode()) if encode else
        (lambda x: x)
    )

    # Кодируем аргументы и формируем путь
    function_args = "/".join(map(encode_func, args))

    # Список URL-адресов в порядке приоритета
    urls = [
        f"{primary_base}{function}/{function_args}",
        f"{secondary_base}{function}/{function_args}"
    ]

    # Пытаемся последовательно выполнить запросы
    for url in urls:
        try:
            logger.debug(f"🌐 Отправка GET-запроса: {url}")
            response = requests.get(
                url,
                auth=HTTPBasicAuth(user_1c, password_1c),
                timeout=10
            )

            if response.status_code == 200:
                logger.info(f"✔️ Успешный ответ от сервера: {response.json()}")
                return response.json()
            else:
                logger.warning(f"⚠️ Ошибка {response.status_code} при запросе: {url} - {response.reason}")
        except Exception as e:
            logger.error(f"⛔ Исключение при запросе к {url}: {e}")


def send_production_data(
        data: dict,
        kappa: bool = False,
        user_1c: str = CONFIG.USER_1C,
        password_1c: str = CONFIG.PASSWORD_1C,
) -> bool:
    """
    Отправляет производственные данные (в формате JSON) на сервер 1С с авторизацией.

    В зависимости от флага `kappa`, сначала пытается отправить данные на один сервер,
    при неудаче — повторяет попытку на резервный.

    Args:
        data (dict): Словарь с производственными данными, которые необходимо отправить.
        Ожидается следующая структура:
        {
            "bill_of_lading": str,           # Номер коносамента
            "transaction_numbers": list[str], # Список номеров и дат транзакций, полученных с помощью TransactionNumberFromBillOfLading
            "source_file_name": str,         # Название исходного файла
            "source_file_base64": str,       # Исходный файл, закодированный в base64
            "containers": [                  # Список контейнеров
                {
                    "container": str,        # Номер контейнера
                    "seals": list[str]       # Список пломб (одна или несколько строк)
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
    # Название функции
    function = "SendProductionDataToTransaction"
    # Определяем порядок серверов в зависимости от флага kappa
    urls = [
        (KAPPA_URL if kappa else LOCAL_URL) + function,
        (LOCAL_URL if kappa else KAPPA_URL) + function
    ]

    # Заголовки для передачи JSON с указанием кодировки
    headers = {"Content-Type": "application/json; charset=utf-8"}

    # Успешность отправки всех транзакций
    all_success = True

    # Извлекаем список номеров сделок
    transaction_numbers: list[str] = data.pop("transaction_numbers")

    # Для каждого номера сделки отправляем отдельный запрос
    for transaction_number in transaction_numbers:
        # Записываем в данные текущий номер сделки
        data["transaction_number"] = transaction_number

        # Проходим по писку URL-адресов в порядке приоритета
        success = False
        for url in urls:
            try:
                logger.debug(f"🌐 Попытка отправки данных на {url}")
                response = requests.post(
                    url,
                    auth=HTTPBasicAuth(user_1c, password_1c),
                    headers=headers,
                    json=data,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(f"✔️ Данные успешно отправлены. Ответ: {response.text}")
                    success = True
                    break
                else:
                    logger.warning(f"⚠️ Ошибка {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                logger.warning(f"⛔ Исключение при отправке на {url}: {e}")

        if not success:
            all_success = False
            logger.error(f"❌ Не удалось отправить данные для transaction_number: {transaction_number}")

    return all_success


if __name__ == "__main__":
    # from src.utils import read_json
    #
    # data = read_json(r"C:\Users\Cherdantsev\Documents\develop\OCR_CONOS\test_1c.json")
    # send_production_data(data)

    func = r'TransactionNumberFromBillOfLading'
    arg = r'KCO000006945'
    tn = cup_http_request(func, arg)

    func = "GetTransportPositionNumberByTransactionNumber"
    cup_http_request(func, tn[-1].split()[0], encode=False)
