import base64
import logging
from copy import deepcopy
from functools import wraps
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Callable, Literal, get_args

import requests
from requests.auth import HTTPBasicAuth
from dateutil.relativedelta import relativedelta

from config import config
from src.utils import parse_datetime

logger = logging.getLogger(__name__)

KAPPA_URL = "http://kappa5.group.ru:81/ca/hs/interaction/"
LOCAL_URL = "http://10.10.0.10:81/ca/hs/interaction/"

SendMethodName = Literal["SendProductionDataToTransaction", "SendDataToMonitoringImport2"]


def enrich_containers_with_provision_date(
        function_name: SendMethodName,
        source_data: dict[str, Any]
) -> dict[str, Any]:
    """
    Добавляет (если необходимо) значение даты предоставления ФС (ДатаПредоставленияФСПоГП)
    для контейнеров в переданных данных на основе логики и данных, полученных из ЦУП.

    Args:
        function_name: Имя метода/функции отправки. Обрабатывается только
            "SendProductionDataToTransaction".
        source_data: Входной payload, ожидается, что содержит ключи:
            - "transaction_numbers": list[str] (опционально)
            - "containers": list[dict] (опционально). Каждый элемент контейнера может содержать:
                - "container" (идентификатор контейнера)
                - "ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера" (дата в строке, которую нужно распарсить)

    Returns:
        dict[str, Any]: Новый словарь с теми же ключами, но с возможными добавлениями
        поля "ДатаПредоставленияФСПоГП" в элементах списка containers (только для
        тех контейнеров, у которых ранее в ЦУП были пустые даты получения/предоставления).
    """
    # Обрабатываем только целевой метод
    if function_name != "SendProductionDataToTransaction":
        return source_data

    # Копируем входные данные полностью, чтобы не изменять оригинал
    data = deepcopy(source_data)

    # Быстрые локальные ссылки — уменьшают количество обращений к словарю
    transaction_numbers = data.get("transaction_numbers")
    containers = data.get("containers")

    # Валидация структуры: нам нужны списки транзакций и контейнеров
    if not (transaction_numbers and isinstance(transaction_numbers, list) and
            containers and
            isinstance(containers, list)
    ):
        return source_data

    # Название поля с датой выгрузки в данных контейнера
    date_issue_field = "ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера"

    # Собираем множество контейнеров, для которых в payload есть дата выгрузки.
    # Это позволяет игнорировать контейнеры без исходной даты выгрузки.
    containers_with_issue_date: set[str] = {
        cont["container"] for cont in containers
        if isinstance(cont, dict) and cont.get("container") and cont.get(date_issue_field)
    }

    if not containers_with_issue_date:
        # Нет контейнеров с датой выгрузки — ничего не делаем
        return source_data

    # Берём первый transaction number
    transaction_number = transaction_numbers[0].split()[0]

    # Название функции и полей, запрашиваемых у ЦУП
    func_main = "ContainersWithProductionRequisitesByTransactionNumber"
    receiving_fc_field = "ВнутрипортовоеЭкспедированиеДатаПолученияФС"
    provision_fc_field = "ДатаПредоставленияФСПоГП"

    # Выполняем запрос к TSUP. Ожидаем список словарей вида [{container_id: {field: value, ...}}, ...]
    response: list[dict[str, dict[str, str]]] = tsup_http_request(
        func_main,
        transaction_number,
        (receiving_fc_field, provision_fc_field)
    )
    if not response:
        return source_data

    # Выбираем контейнеры, у которых в TSUP оба поля пусты — только их стоит обновлять.
    containers_need_update: set[str] = {
        cont_id
        for data_tsup_item in response
        for cont_id, fields in data_tsup_item.items()
        if cont_id in containers_with_issue_date
           and isinstance(fields, dict)
           and fields.get(receiving_fc_field) == ""
           and fields.get(provision_fc_field) == ""
    }

    if not containers_with_issue_date:
        # Нечего обновлять
        return source_data

    # Проходим по контейнерам в копии данных и при необходимости добавляем дату предоставления ФС.
    for container in containers:
        # Защищаемся от некорректного элемента
        if not isinstance(container, dict):
            continue

        container_id = container.get("container")

        if not container_id or container_id not in containers_need_update:
            continue

        issue_date_str = container.get(date_issue_field)
        if not issue_date_str:
            continue

        # Преобразуем дату и прибавляем один месяц. Ошибки логируем, но продолжаем обработку.
        try:
            new_date: datetime = parse_datetime(container[date_issue_field]) + relativedelta(months=1)
            container[provision_fc_field] = new_date.strftime(config.tsup_datetime_format)
        except Exception as e:
            logger.exception(
                "⛔ Ошибка преобразования/присвоения даты для контейнера %s (исходная строка: %s): %s",
                container_id, issue_date_str, e
            )

    return data


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
        function_args = "_".join(
            "_".join(arg) if isinstance(arg, (tuple, list, set)) else str(arg)
            for arg in args
        )

        cache_key = f"{function}_{function_args}"

        # Проверка, есть ли результат в кэше
        if cache_key in cache:
            cache_value = cache[cache_key]
            logger.debug(f"🌐 Повторный вызов функции: {cache_key}")
            logger.debug(f"💾 Результат возвращён из кэша: {cache_value}")
            return cache_value

        # Выполнение оригинальной функции
        result = func(function, *args, **kwargs)
        cache[cache_key] = result

        # Ограничение размера кэша
        if len(cache) > max_cache_size:
            cache.pop(next(iter(cache)))

        return result

    return wrapper


@cache_http_requests
def tsup_http_request(
        function_name: str,
        *args: str | tuple[str],
        kappa: bool = False,
        encode: bool = True,
        login: str = config.user_1c,
        password: str = config.password_1c,
) -> list | dict | None:
    """
    Выполняет GET-запрос к серверу 1С и возвращает результат в формате JSON.

    Функция сначала пытается выполнить запрос к основному серверу, и при ошибке
    делает повторную попытку на резервный сервер. Поддерживает кодирование
    параметров запроса в base64 для URL-безопасности.

    Args:
        function_name: Название вызываемой функции/метода API на сервере 1С
        *args: Позиционные аргументы, передаваемые в URL
        kappa: Флаг, определяющий приоритет серверов (основной/резервный)
        encode: Если True, аргументы кодируются в base64
        login: Логин пользователя для базовой авторизации
        password: Пароль пользователя для базовой авторизации

    Returns:
        list | dict | None: Ответ сервера в формате JSON, если успешен. Иначе — None.
    """
    # Определяем функцию кодирования аргументов: base64 или без изменений
    encode_func: Callable[[str], str] = (
        (lambda x: base64.urlsafe_b64encode(x.encode()).decode()) if encode else
        (lambda x: x)
    )

    # Формируем строку аргументов, кодируя их при необходимости и соединяя через слеш
    # function_args: str = "/".join(encode_func(arg) for arg in args if arg)
    function_args: str = "/".join(
        encode_func(",".join(arg) if isinstance(arg, (tuple, list, set)) else str(arg))
        for arg in args if arg
    )

    # Список URL-адресов в порядке приоритета
    urls = [
        f"{KAPPA_URL if kappa else LOCAL_URL}{function_name}/{function_args}",
        f"{LOCAL_URL if kappa else KAPPA_URL}{function_name}/{function_args}"
    ]

    # Пытаемся последовательно выполнить запросы
    for url in urls:
        try:
            logger.debug(f"🌐 Отправка GET-запроса на {url}")

            # Выполняем GET-запрос с таймаутом 10 секунд
            response = requests.get(
                url,
                auth=HTTPBasicAuth(login, password),
                timeout=30
            )

            if response.status_code == 200:
                # Парсим JSON-ответ и возвращаем его
                try:
                    result = response.json()
                    logger.debug(f"✔️ Успешный ответ от сервера: {result}")
                    return result
                except JSONDecodeError:
                    logger.warning(
                        f"⚠️ Не удалось распарсить ответ через .json(). Текст ответа сервера: "
                        f"{response.text or 'Пустой ответ'}"
                    )
            else:
                logger.warning(
                    f"⚠️ Ошибка при запросе к {url}. "
                    f"Код: {response.status_code}, Причина: {response.reason}"
                )
        except Exception as e:
            # Логируем сетевое исключение и продолжаем с резервным сервером
            logger.exception(f"⛔ Сетевая ошибка при запросе к {url}: {e}")
            continue


def send_data_to_tsup(
        function_name: SendMethodName,
        data: dict[str, Any],
        kappa: bool = False,
        login: str = config.user_1c,
        password: str = config.password_1c,
        apply_provision_enrichment: bool = True
) -> bool:
    """
    Отправляет данные (в формате JSON) на сервер ЦУП (1С) с авторизацией.

    Перед отправкой, при активированном флаге `apply_provision_enrichment`,
    выполняет дополнительную трансформацию данных — добавление дат предоставления ФС
    (см. `enrich_containers_with_provision_date`).

    В зависимости от флага `kappa`, приоритетным сервером для отправки будет
    либо `KAPPA_URL`, либо `LOCAL_URL`. Если отправка на основной сервер не удалась,
    выполняется повторная попытка на резервный.

    Args:
        function_name: Имя вызываемой функции/метода API на сервере 1С.
        data: Словарь с производственными данными, которые необходимо отправить.
        kappa: Определяет порядок приоритета серверов:
            - True  → сначала отправка на `KAPPA_URL`, затем резерв на `LOCAL_URL`;
            - False → наоборот.
        login: Имя пользователя для базовой авторизации.
        password: Пароль пользователя для базовой авторизации.
        apply_provision_enrichment: Если True — перед отправкой выполняется
            обогащение контейнерных данных через `enrich_containers_with_provision_date`.

    Returns:
        bool: True — если данные успешно отправлены хотя бы на один сервер;
        False — если обе попытки отправки завершились неудачей.


    Ожидается следующая структура data для SendProductionDataToTransaction:
        {
            "bill_of_lading": str,                      # Номер коносамента
            "ИмпМорскаяПеревозкаДатаПолученияДУ": str,  # Дата ДО
            "ИмпМорскаяПеревозкаНомерРейсаФидер": str,  # Номер рейса
            "ЭтоКоносамент": str[bool],                 # Флаг, является ли документ коносаментом или нет
            "transaction_numbers": list[str],           # Список номеров и дат транзакций, полученных с помощью TransactionNumberFromBillOfLading
            "source_file_name": str,                    # Название исходного файла
            "source_file_base64": str,                  # Исходный файл, закодированный в base64
            "containers": [                             # Список контейнеров
                {
                    "container": str,                                 # Номер контейнера
                    "ИмпМорскаяПеревозкаНомерПломбы": list[str],      # Список пломб (одна или несколько строк)
                    "ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера": str  # Дата выгрузки
                },
                ...
            ]
        }

        Пример корректного значения для SendProductionDataToTransaction:
        {
            "bill_of_lading": "VX75EA25000897",
            "ИмпМорскаяПеревозкаДатаПолученияДУ": "28.05.2025 00:00:00",
            "ИмпМорскаяПеревозкаНомерРейсаФидер": "2503",
            "ЭтоКоносамент": "true",
            "transaction_numbers": ["АА-0095444 от 14.04.2025", "АА-0095445 от 15.04.2025"],
            "source_file_name": "КС_VX75EA25000897.pdf",
            "source_file_base64": "JVBERi0xLjcKJeLjz9MK...",
            "containers": [
                {
                    "container": "DFTU1001462",
                    "ИмпМорскаяПеревозкаНомерПломбы": ["22528791", "2252880"],
                    "ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера": "28.05.2025 11:34:00"
                },
                {
                    "container": "DFTU1001502",
                    "ИмпМорскаяПеревозкаНомерПломбы": ["2117691"],
                    "ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера": "28.05.2025 11:41:00"
                }
            ]
        }

        Пример корректного значения для SendDataToMonitoringImport2:
        {
            "bill_of_lading": "NNLRZH241632185",
            "containers": [
                {
                    "container": "FTAU1721399",
                    "size": "20",
                    "type": "DC",
                    "cargo_gross_weight": "23680.0",
                    "tare_weight": ""
                }
            ],
            "shipping_line": "Junan",
            "bt_user": "Cherdantsev",
            "bt_id": "a95785c2b7244e438da0b091a86c833e",
            "bt_partner": "AA-024296"
        }
    """
    if function_name not in get_args(SendMethodName):
        logger.error(
            "❌ Некорректное значение аргумента 'function_name': %s. Доступные значения: %s.",
            function_name, ", ".join(get_args(SendMethodName))
        )
        return False

    # Формируем список URL в порядке приоритета — основной и резервный сервер
    # в зависимости от флага kappa
    urls = [
        f"{KAPPA_URL if kappa else LOCAL_URL}{function_name}",
        f"{LOCAL_URL if kappa else KAPPA_URL}{function_name}"
    ]

    # При необходимости выполняем предобработку данных — добавляем даты предоставления ФС
    if apply_provision_enrichment:
        data = enrich_containers_with_provision_date(function_name, data)

    for url in urls:
        try:
            logger.debug(f"🌐 Попытка отправки данных на {url}.")
            # Выполняем POST-запрос с максимальным таймаутом 60 секунд
            response = requests.post(
                url,
                auth=HTTPBasicAuth(login, password),
                headers={"Content-Type": "application/json; charset=utf-8"},
                json=data,
                timeout=60
            )

            if response.status_code == 200:
                logger.debug(
                    f"✔️ Данные успешно отправлены. "
                    f"Ответ: {response.text or 'пустой'}"
                )
                return True
            else:
                # Логируем неуспешный ответ сервера
                logger.warning(
                    f"⚠️ Ошибка при отправке данных. "
                    f"Код: {response.status_code}, Ответ: {response.text}"
                )

        except requests.exceptions.RequestException as e:
            # Логируем исключение при сетевой ошибке
            logger.exception(f"⛔ Сетевая ошибка при отправке на {url}: {e}")
            continue  # Пробуем резервный сервер

    return False


if __name__ == "__main__":
    from src.utils import read_json
    import json

    data_json = read_json(r"C:\Users\Cherdantsev\Desktop\Новая папка (2)\ДУ_SILJEA25586000.pdf_1c.json")
    print(json.dumps(data_json, indent=4, ensure_ascii=False))
    new_dict = enrich_containers_with_provision_date("SendProductionDataToTransaction", data_json)
    print(json.dumps(new_dict, indent=4, ensure_ascii=False))
    # print(json.dumps(data_json, indent=4, ensure_ascii=False))

    # from src.utils import read_json, write_json
    # from src.utils_data_process import remap_production_data_for_1c
    #
    # data_json = read_json(r"C:\Users\Cherdantsev\Documents\data\OCR_CONOS_FILES\ДУ_EGML001367.pdf_one_cont.json")
    # # data_json = read_json(r"C:\Users\Cherdantsev\Documents\develop\OCR_CONOS_FILES\ДУ_EGML001367.pdf_one_cont.json")
    # data_json = remap_production_data_for_1c(data_json)
    # # write_json(r"C:\Users\Cherdantsev\Desktop\test\test.json", data_json)
    # print("Статус отправки:", send_production_data(data_json))
    # print(data_json)

    # data_json = read_json(
    #     r"C:\Users\Cherdantsev\Desktop\250528_173535_aby@sdrzbt.ru\_КС_AKKSUS25060413SRV.pdf.json"
    # )
    # remap_production_data(data_json)
    # write_json(r"C:\Users\Cherdantsev\Desktop\250528_173535_aby@sdrzbt.ru\КС_AKKSUS25060413SRV.pdf.json",
    #            data_json)

    # func = r'TransactionNumberFromBillOfLading'
    # arg = r'MDTRLS2506086'
    # print(cup_http_request(func, arg))
    # for i in range(2):
    #     tn = cup_http_request(func, arg)
    # print(tn)

    # func = "GetTransportPositionNumberByTransactionNumber"
    # print(cup_http_request(func, tn[-1].split()[0], encode=False))

    # container_numbers_cup: list[list[str]] = [
    #     # Очищаем полученные номера от лишних пробелов
    #     [number.strip() for number in tsup_http_request(
    #         "GetTransportPositionNumberByTransactionNumber",
    #         # Извлекаем только номер, отсекая дату (например, "АА-0095444 от 14.04.2025" → "АА-0095444"
    #         transaction_number.split()[0],
    #         encode=False
    #     )]
    #     for transaction_number in ["АА-0095444", "АG-0095563"]
    # ]
    #
    # print(container_numbers_cup)
