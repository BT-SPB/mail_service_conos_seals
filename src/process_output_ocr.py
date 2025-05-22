import re
import shutil
from pathlib import Path
from collections import defaultdict

from config import CONFIG
from src.logger import logger
from src.utils import (
    read_json,
    write_json,
    file_to_base64,
    transfer_files,
    sanitize_pathname,
    is_directory_empty,
)
from src.utils_1c import cup_http_request, send_production_data
from src.utils_email import send_email, convert_email_date_to_moscow


def update_json(
        data: dict[str, any],
        source_file: Path,
        transaction_numbers: list[str]
) -> None:
    """Обновляет словарь с JSON-данными, добавляя или изменяя определенные поля.

    Функция модифицирует входной словарь `data`, добавляя или обновляя поля, связанные
    с типом документа, датой создания, номерами транзакций, именем исходного файла
    и его содержимым в формате base64. Также обновляет информацию о контейнерах,
    устанавливая дату загрузки и обрабатывая заметки с учетом запрета ОПК.

    Args:
        data: Словарь с JSON-данными, который будет обновлен
        source_file: Объект Path, представляющий путь к исходному файлу
        transaction_numbers: Список строк с номерами транзакций

    Returns:
        None: Функция изменяет словарь `data` на месте и ничего не возвращает.
    """
    # Устанавливаем значения по умолчанию для типа документа, даты и рейса, если они отсутствуют
    data.setdefault("document_type", "КС")
    data.setdefault("document_created_datetime", "")
    data.setdefault("voyage_number", "")

    # Обрабатываем список контейнеров, если он присутствует в данных
    for container in data.get("containers", []):
        # Устанавливаем дату загрузки по умолчанию, если она отсутствует.
        container.setdefault("upload_datetime", "")

        # Проверяем наличие фразы "запрет опк" в заметках контейнера.
        note = container.get("note", "")
        container["note"] = "Запрет ОПК" if re.search(r"запрет\s+опк", note, re.IGNORECASE) else ""

    # Обновляем JSON-данные дополнительной информацией
    data.update({
        "transaction_numbers": transaction_numbers,
        "source_file_name": f"{data['document_type']}_{data['bill_of_lading']}_AUTO{source_file.suffix}",
        "source_file_base64": file_to_base64(source_file),
    })


def format_json_data_to_mail(
        json_data: dict[str, any],
        title: str | None = None
) -> str:
    """Форматирует данные из словаря JSON в читаемую строку с информацией о коносаменте, транзакциях и контейнерах.

    Функция проверяет наличие ключей 'document_type', 'document_created_datetime', 'bill_of_lading',
    'transaction_numbers' и 'containers' в словаре `json_data`. Для поля 'containers' дополнительно
    проверяется, что оно является списком словарей с обязательными ключами 'container' и 'seals'.
    Если указан заголовок (`title`), он добавляется в начало результата. Форматирование результата
    выполняется в следующем виде:
        <title>
        document_type: <значение>
        bill_of_lading: <значение>
        date_do: <значение>
        voyage_number: <значение>
        transaction_numbers: <значение>
        containers:
            - <container>: <seals> [- <upload_datetime>] [- <note>]
    Если после обработки нет данных (только заголовок или ничего), возвращается пустая строка.

    Args:
        json_data: Словарь с данными, содержащий:
            - 'document_type': строка с типом документа (опционально)
            - 'bill_of_lading': строка с номером коносамента (опционально)
            - 'document_created_datetime': строка с датой создания документа (опционально)
            - 'voyage_number': строка с номером рейса
            - 'transaction_numbers': список строк с номерами транзакций (опционально)
            - 'containers': список словарей, где каждый словарь содержит ключи:
                - 'container': строка с номером контейнера
                - 'seals': строка или список строк с номерами пломб
                - 'upload_datetime': строка с датой загрузки (опционально)
                - 'note': строка с примечаниями (опционально)
        title: Заголовок, добавляемый в начало строки (опционально, по умолчанию None)

    Returns:
        str: Отформатированная строка с данными. Если нет валидных полей или добавлен только заголовок,
            возвращается пустая строка.
    """
    # Инициализируем список для накопления строк результата
    output_lines: list[str] = []

    # Добавляем заголовок, если он задан и не пустой
    if title and title.strip():
        output_lines.append(title)

    # Считаем, сколько строк было до добавления данных (для проверки только заголовка)
    initial_length = len(output_lines)

    # Добавляем тип документа
    if document_type := json_data.get("document_type"):
        output_lines.append(f"Тип документа: {document_type}")

    # Добавляем bill_of_lading, если ключ существует и значение не пустое
    if bill_of_lading := json_data.get("bill_of_lading"):
        output_lines.append(f"Номер коносамента: {bill_of_lading}")

    # Добавляем дату создания документа
    if date_do := json_data.get("document_created_datetime"):
        output_lines.append(f"Дата ДО: {date_do}")

    # Добавляем номер рейса
    if voyage_number := json_data.get("voyage_number"):
        output_lines.append(f"Номер рейса: {voyage_number}")

    # Добавляем transaction_numbers, если ключ существует и значение не пустое
    if transaction_numbers := json_data.get("transaction_numbers"):
        if isinstance(transaction_numbers, list):
            # Форматируем список транзакций в строку, разделяя элементы запятыми
            formatted_transactions = ", ".join(str(t) for t in transaction_numbers)
            output_lines.append(f"Номера сделок: {formatted_transactions}")

    # Обрабатываем containers: проверяем, что это список, и форматируем каждый контейнер
    if containers := json_data.get("containers"):
        if isinstance(containers, list):
            # Фильтруем контейнеры, у которых есть оба ключа: container и seals
            valid_containers = [
                cont for cont in containers
                if isinstance(cont, dict) and cont.get("container") and "seals" in cont
            ]
            # Добавляем секцию containers, если есть валидные контейнеры
            if valid_containers:
                # Добавление заголовка секции контейнеров
                output_lines.append("Контейнеры:")
                for container in valid_containers:
                    # Извлечение номера контейнера и пломб
                    container_number = container["container"]
                    seals = container["seals"]
                    # Форматирование пломб: если это список, объединяем элементы через запятую
                    seals_formatted = ", ".join(seals) if isinstance(seals, list) else seals
                    # Формирование базовой строки для контейнера
                    container_line = f"{' ' * 4}- {container_number}: [{seals_formatted}]"

                    # Добавление даты загрузки, если она указана
                    if upload_datetime := container.get("upload_datetime"):
                        container_line += f" - {upload_datetime}"

                    # Добавление примечаний, если они указаны
                    if note := container.get("note"):
                        container_line += f" - {note}"

                    output_lines.append(container_line)

    # Возвращаем пустую строку, если добавлен только заголовок или ничего
    return "\n".join(output_lines) if len(output_lines) > initial_length else ""


def formatted_text_from_data(
        data: dict[str, list[str]],
        bullet: str = "•",
        indent: int = 4,
        entry_separator: str = "\n\n"
) -> str:
    """Форматирует словарь с данными в читаемый текст для отправки по email.

    Создаёт отформатированный текст, где каждый файл нумеруется, за именем файла следует двоеточие,
    а сообщения отображаются как маркированный список. Многострочные сообщения разбиваются на строки,
    причём первая строка начинается с маркера (`bullet`), а последующие выравниваются с учётом длины
    маркера. Записи для файлов разделяются указанным разделителем.

    Args:
        data: Словарь, где ключи — имена файлов (строки), а значения — списки сообщений (списки строк)
        bullet: Символ, используемый для маркировки сообщений (по умолчанию "•")
        indent: Начальный отступ для всех строк, включая номер файла (по умолчанию 4)
        entry_separator: Строка, разделяющая записи для разных файлов (по умолчанию "\n\n")

    Returns:
        str: Отформатированная строка с пронумерованными файлами и сообщениями. Если словарь пуст или
            все списки сообщений пусты, возвращается пустая строка.
    """
    if not data:
        return ""

    # Формируем строки отступов
    base_indent_spaces = " " * indent  # Отступ для номера файла и имени
    bullet_indent_spaces = " " * len(bullet)

    # Список для хранения отформатированных записей для каждого файла
    formatted_entries: list[str] = []

    # Перебираем файлы и их сообщения, нумеруя с 1
    for idx, (filename, messages) in enumerate(data.items(), 1):
        # Вычисляем ширину номера (длина номера + точка + пробел) для выравнивания сообщений
        number_width = len(str(idx)) + 2  # +2 для ". "
        # Отступ для сообщений: базовый отступ + ширина номера
        message_indent_spaces = " " * (indent + number_width)

        # Формируем сообщения, обрабатывая многострочные строки
        formatted_messages: list[str] = []

        for msg in messages:
            # Приводим сообщение к строке и разбиваем на строки для обработки многострочных сообщений
            lines = str(msg).split("\n")
            # Первая строка сообщения начинается с маркера (bullet)
            formatted_messages.append(f"{message_indent_spaces}{bullet} {lines[0]}")
            # Последующие строки выравниваются с учётом длины маркера, пустые строки игнорируются
            formatted_messages.extend(
                f"{message_indent_spaces}{bullet_indent_spaces} {line}"
                for line in lines[1:]
                if line.strip()  # Игнорируем пустые строки
            )

        # Формируем полную запись для файла: номер, имя файла и сообщения
        file_entry = (
                f"{base_indent_spaces}{idx}. {filename}:\n"
                + "\n".join(formatted_messages)
        )
        formatted_entries.append(file_entry)

    # Объединяем записи с указанным разделителем
    return entry_separator.join(formatted_entries)


def format_email_message(
        metadata: dict[str, any],
        error_folder: Path,
) -> str:
    """Форматирует email-сообщение с отчётом об обработке файлов.

    Создаёт понятный текст письма для пользователей, включая приветствие, результаты обработки файлов
    (успешные, с ошибками или частично успешные) и инструкции по доступу к файлам. Использует функцию
    formatted_text_from_data для форматирования списков сообщений.

    Args:
        metadata: Словарь с метаданными, содержащий ключи 'sender' (строка, email отправителя),
            'date' (строка, дата получения), 'errors' (словарь ошибок), 'partial_successes'
            (словарь частичных успехов), 'successes' (словарь успешных обработок)
        error_folder: Путь к папке, где хранятся файлы с ошибками (объект Path)

    Returns:
        str: Отформатированный текст письма. Если нет сообщений об обработке, возвращается пустая строка.
    """
    # Инициализируем список секций письма с приветствием и информацией об отправителе
    email_sections: list[str] = [
        f"Здравствуйте!\n"
        f"Это автоматическое уведомление об обработке файлов, полученных от {metadata['sender']}.\n"
        f"Дата получения: {convert_email_date_to_moscow(metadata['date'])}."
    ]

    # Проверяем, есть ли сообщения для включения в письмо
    has_content = False

    # Добавляем информацию об ошибках, если они есть
    if metadata.get("errors"):
        formatted_errors = formatted_text_from_data(metadata["errors"])
        email_sections.append(
            f"❌ Файлы, при обработке которых возникли проблемы:\n"
            f"{formatted_errors}\n\n"
            f"Копии файлов доступны по пути: {error_folder}"
        )
        has_content = True

    # Добавляем информацию о частично успешных файлах, если они есть
    if metadata.get("partial_successes"):
        formatted_partial = formatted_text_from_data(metadata["partial_successes"])
        email_sections.append(
            f"⚠️ Частично обработанные файлы (только часть данных загружена в ЦУП):\n"
            f"{formatted_partial}\n\n"
            f"Копии файлов доступны по пути: {error_folder}"
        )
        has_content = True

    # Добавляем информацию об успешных файлах, если включены уведомления и есть успехи
    if CONFIG.enable_success_notifications and metadata.get("successes"):
        formatted_successes = formatted_text_from_data(metadata["successes"])
        email_sections.append(
            f"✅ Успешно обработанные файлы (данные загружены в ЦУП):\n"
            f"{formatted_successes}"
        )
        has_content = True

    # Объединяем секции, если есть хотя бы одно сообщение, иначе возвращаем пустую строку
    return "\n\n\n".join(email_sections) if has_content else ""


def process_output_ocr(
        email_user: str,
        email_pass: str,
        smtp_server: str,
        smtp_port: int
) -> None:
    """
    Обрабатывает результаты OCR, извлекая номера сделок из ЦУП по коносаменту и отправляя номера пломб ЦУП.

    Функция сканирует директории с результатами OCR, проверяет метаданные, обрабатывает файлы,
    взаимодействует с ЦУП для получения номеров транзакций, перемещает файлы в папки успешной обработки
    или ошибок, отправляет email-уведомления и очищает директории.

    Args:
        email_user: Адрес электронной почты для отправки уведомлений
        email_pass: Пароль от почтового ящика
        smtp_server: Адрес SMTP-сервера для отправки email
        smtp_port: Порт SMTP-сервера

    Returns:
        None: Функция изменяет файловую систему, отправляет email, но не возвращает значений.
    """
    # Получаем список директорий, содержащих файл metadata.json
    folders_for_processing: list[Path] = [
        folder for folder in CONFIG.OUT_OCR_FOLDER.iterdir()
        if folder.is_dir() and (folder / "metadata.json").is_file()
    ]

    # Если директорий нет, логируем и завершаем выполнение
    if not folders_for_processing:
        logger.debug("➖ Новых директорий для обработки нет")
        return

    logger.info(f"📁 Обнаружено директорий для обработки: {len(folders_for_processing)}")

    # Последовательно обрабатываем каждую директорию
    for folder in folders_for_processing:
        try:
            # Читаем метаданные из файла metadata.json
            metadata_file: Path = folder / "metadata.json"
            metadata: dict[str, any] = read_json(metadata_file)

            # Формируем пути для папок ошибок и успешной обработки с безопасными именами
            error_folder = sanitize_pathname(CONFIG.ERROR_FOLDER, folder.name, is_file=False)
            success_folder = sanitize_pathname(CONFIG.SUCCESS_FOLDER, folder.name, is_file=False)

            container_notes: list[str] = []

            # Проверяем целостность метаданных: наличие и типы всех обязательных полей
            required_fields = {
                "subject": str,
                "sender": str,
                "date": str,
                "text_content": str,
                "files": list,
                "errors": dict,
                "partial_successes": dict,
                "successes": dict
            }
            if not metadata or not all(
                    isinstance(metadata.get(field), expected_type)
                    for field, expected_type in required_fields.items()
            ):
                error_message = (f"Файл metadata.json имеет неверный формат "
                                 f"или тип данных: {metadata_file}")
                logger.warning(f"❌ {error_message}")
                metadata["GLOBAL_ERROR"] = error_message
                write_json(metadata_file, metadata)
                # Перемещаем директорию в папку ошибок
                shutil.move(folder, error_folder)
                continue

            # Преобразуем словари в defaultdict для удобной работы с сообщениями
            metadata["errors"] = defaultdict(list, metadata["errors"])
            metadata["partial_successes"] = defaultdict(list, metadata["partial_successes"])
            metadata["successes"] = defaultdict(list, metadata["successes"])

            # Проверяем, есть ли файлы для обработки
            if not metadata["files"]:
                error_message = f"В metadata.json нет файлов для обработки: {metadata_file}"
                logger.warning(f"❌ {error_message}")
                metadata["GLOBAL_ERROR"] = error_message
                write_json(metadata_file, metadata)
                shutil.move(folder, error_folder)
                continue

            # Обрабатываем каждый файл из метаданных
            for source_file_name in metadata["files"]:
                source_file: Path = folder / source_file_name
                json_file: Path = source_file.with_name(source_file.name + ".json")
                files_to_transfer = [source_file, json_file]

                # Проверяем существование исходного файла
                if not source_file.is_file():
                    error_message = "Исходный файл отсутствует."
                    logger.warning(f"❌ {error_message} ({source_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Проверяем существование JSON файла
                if not json_file.is_file():
                    error_message = "JSON-файл с данными OCR отсутствует."
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Читаем и валидируем данные из JSON
                json_data: dict[str, any] = read_json(json_file)

                # Проверяем наличие номера коносамента
                if not json_data.get("bill_of_lading"):
                    error_message = "Номер коносамента отсутствует или не распознан."
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Фильтруем контейнеры, оставляя только те, у которых есть номер
                json_data["containers"] = [
                    cont for cont in json_data.get("containers", [])
                    if isinstance(cont, dict) and cont.get("container")
                ] if json_data.get("containers") else None

                # Проверяем наличие контейнеров
                if not json_data["containers"]:
                    error_message = "Информация о контейнерах отсутствует или не распознана."
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Определяем контейнеры с пустыми номерами пломб
                containers_with_empty_seals = {
                    cont["container"] for cont in json_data["containers"]
                    if not cont.get("seals")
                }

                # Если все контейнеры имеют пустые пломбы
                if len(containers_with_empty_seals) == len(json_data["containers"]):
                    error_message = (f"Номера пломб отсутствуют для всех контейнеров: "
                                     f"{', '.join(containers_with_empty_seals)}.")
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Если есть контейнеры с пустыми пломбами, логируем частичную ошибку
                if containers_with_empty_seals:
                    error_message = (f"Номера пломб отсутствуют для части контейнеров: "
                                     f"{', '.join(containers_with_empty_seals)}.")
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "copy2")
                    # Удаляем контейнеры с пустым полем "seals"
                    json_data["containers"] = [
                        cont for cont in json_data["containers"]
                        if cont["container"] not in containers_with_empty_seals
                    ]

                # Запрашиваем номер транзакции из ЦУП по коносаменту
                # Пример получаемого значения: ["АА-0095444 от 14.04.2025"]
                transaction_numbers: list[str] = cup_http_request(
                    "TransactionNumberFromBillOfLading", json_data["bill_of_lading"]
                )

                # Если транзакции не найдены и коносамент заканчивается на `SRV`, пробуем без суффикса
                if not transaction_numbers and json_data["bill_of_lading"].endswith("SRV"):
                    bill_of_lading = json_data["bill_of_lading"].removesuffix("SRV")
                    transaction_numbers: list[str] = cup_http_request(
                        "TransactionNumberFromBillOfLading", bill_of_lading
                    )
                    json_data["bill_of_lading"] = bill_of_lading

                # Проверяем, получены ли номера транзакций
                if not (transaction_numbers and isinstance(transaction_numbers, list)):
                    error_message = (
                        f"Номер транзакции из ЦУП отсутствует. "
                        f"Возможно, номер коносамента ({json_data['bill_of_lading']}) "
                        f"распознан неверно."
                    )
                    formatted_json_data = format_json_data_to_mail(
                        json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):"
                    )
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(f"{error_message}{formatted_json_data}")
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Обновляем JSON-данные дополнительной информацией
                update_json(json_data, source_file, transaction_numbers)
                # Сохраняем обновленный JSON-файл
                write_json(json_file, json_data)

                # Запрашиваем номера контейнеров по каждому номеру транзакции
                container_numbers_cup: list[list[str]] = [
                    # Очищаем полученные номера от лишних пробелов
                    [number.strip() for number in cup_http_request(
                        "GetTransportPositionNumberByTransactionNumber",
                        # Извлекаем только номер, отсекая дату (например, "АА-0095444 от 14.04.2025" → "АА-0095444"
                        transaction_number.split()[0],
                        encode=False
                    )]
                    for transaction_number in transaction_numbers
                ]

                # Проверяем, получены ли номера контейнеров
                if not any(container_numbers_cup):
                    error_message = (
                        f"Номера контейнеров по номеру сделки ({transaction_numbers}) "
                        f"из ЦУП отсутствуют."
                    )
                    formatted_json_data = format_json_data_to_mail(
                        json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):"
                    )
                    logger.warning(f"⚠️ {error_message} ({source_file})")
                    metadata["errors"][source_file_name].append(f"{error_message}{formatted_json_data}")
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Сравниваем номера контейнеров из OCR и ЦУП
                container_numbers_cup_set: set[str] = {x for sublist in container_numbers_cup for x in sublist}
                container_numbers_ocr_set: set[str] = {cont.get("container") for cont in json_data.get("containers")}

                # Проверяем, есть ли совпадения между наборами номеров
                if not container_numbers_cup_set & container_numbers_ocr_set:
                    error_message = (
                        f"Номера контейнеров из OCR ({', '.join(container_numbers_ocr_set)}) "
                        f"не совпадают с номерами из ЦУП ({', '.join(container_numbers_cup_set)}) "
                        f"по номеру сделки {transaction_numbers}."
                    )
                    formatted_json_data = format_json_data_to_mail(
                        json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):"
                    )
                    logger.warning(f"⚠️ {error_message} ({source_file})")
                    metadata["errors"][source_file_name].append(f"{error_message}{formatted_json_data}")
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Проверяем наличие контейнеров, которые были распознаны, но отсутствуют в ЦУП
                missing_containers: set[str] = container_numbers_ocr_set - container_numbers_cup_set
                if missing_containers:
                    # Отправляем сообщение, но не прерываем цикл, так как
                    # некоторые контейнеры были успешно распознаны
                    error_message = (
                        f"Некоторые из распознанных номеров контейнеров ({', '.join(missing_containers)}) "
                        f"отсутствуют в данных ЦУП по номеру сделки {transaction_numbers}."
                    )
                    logger.warning(f"⚠️ {error_message} ({source_file})")
                    metadata["errors"][source_file_name].append(error_message)
                    transfer_files(files_to_transfer, error_folder, "copy2")
                    json_data["containers"] = [
                        cont for cont in json_data["containers"]
                        if cont["container"] not in missing_containers
                    ]

                # Отправляем данные в ЦУП, если включена настройка
                if CONFIG.enable_send_production_data:
                    if not send_production_data(json_data):
                        error_message = (
                            f"Не удалось загрузить данные в ЦУП "
                            f"по номеру сделки {transaction_numbers}.\n"
                        )
                        formatted_json_data = format_json_data_to_mail(
                            json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):"
                        )
                        logger.warning(f"❌ {error_message} ({json_file})")
                        metadata["errors"][source_file_name].append(f"{error_message}{formatted_json_data}")
                        transfer_files(files_to_transfer, error_folder, "move")
                        continue
                else:
                    logger.debug(
                        "🔔 Отправка данных в ЦУП отключена настройкой "
                        "'enable_send_production_data'"
                    )

                # Формируем сообщение об успехе и перемещаем файлы в директорию успешной обработки
                success_message = format_json_data_to_mail(json_data, "Загруженные данные:")
                logger.info(f"✔️ Файл обработан успешно: {source_file}")
                metadata["successes"][source_file_name].append(success_message)
                transfer_files(files_to_transfer, success_folder, "move")

                # Добавляем в список все примечания для контейнеров
                container_notes.extend(cont["note"] for cont in json_data.get("containers", []) if cont["note"])

            # Формируем список частично успешных файлов (которые есть одновременно
            # в errors и successes) с сохранением порядка
            partial_successes_files = [
                filename for filename in metadata["errors"]
                if filename in metadata["successes"]
            ]
            # Обрабатываем частично распознанные файлы
            for partial_filename in partial_successes_files:
                # Объединяем сообщения из errors и successes, удаляя информацию из исходных списков
                metadata["partial_successes"][partial_filename] = (
                        metadata["errors"].pop(partial_filename, []) +
                        metadata["successes"].pop(partial_filename, [])
                )

            # Сохраняем обновленные метаданные после обработки всех файлов в директории
            write_json(metadata_file, metadata)

            # Удаляем дубликаты из списка примечаний для контейнеров
            container_notes = list(dict.fromkeys(container_notes))

            # Формируем и отправляем email, если есть сообщения
            email_text = format_email_message(metadata, error_folder)
            if email_text:
                subject = f"Автоответ: {metadata['subject']}"
                if container_notes:
                    subject += f" + {', '.join(container_notes)}"

                send_email(
                    email_text=email_text,
                    # recipient_emails=metadata["sender"],
                    recipient_emails=CONFIG.notification_emails,
                    subject=subject,
                    email_user=email_user,
                    email_pass=email_pass,
                    smtp_server=smtp_server,
                    smtp_port=smtp_port,
                    email_format="plain"
                )

            # Копируем metadata.json в error_folder, если есть ошибки или частичные успехи
            if metadata["errors"] or metadata["partial_successes"]:
                transfer_files(metadata_file, error_folder, "copy2")

            # Перемещаем metadata.json в success_folder, если есть успехи
            if metadata["successes"]:
                transfer_files(metadata_file, success_folder, "move")

            # Удаляем metadata.json из исходной директории (при наличии).
            # Условие сработает, если не было успехов.
            if metadata_file.exists():
                try:
                    metadata_file.unlink()
                except OSError as e:
                    logger.error(f"⚠️ Не удалось удалить {metadata_file}: {e}")

            # Очищаем директорию: удаляем, если пуста, или перемещаем остатки
            if is_directory_empty(folder):
                folder.rmdir()
                logger.info(f"✔️ Удалена пустая директория: {folder}")
            else:
                residual_destination = error_folder / f"residual_files"
                shutil.move(folder, residual_destination)
                logger.error(
                    f"❗❗❗ В директории {folder.name} остались необработанные файлы. "
                    f"Они перемещены в {residual_destination} для ручной проверки"
                )

        except Exception as e:
            logger.exception(f"⛔ Ошибка при обработке директории {folder}: {e}")
            continue
