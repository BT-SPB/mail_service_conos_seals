import re
import copy
from pathlib import Path

from config import CONFIG
from src.utils import file_to_base64
from src.utils_email import convert_email_date_to_moscow


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


def remap_production_data_for_1c(data_source: dict[str, any]) -> dict[str, any]:
    """
    Подготавливает словарь с данными для отправки в 1С, переименовывая поля и удаляя ненужные.

    Функция изменяет входной словарь, подготавливая его для отправки на сервер 1С.
    Удаляет ненужные поля и переименовывает ключи в соответствии с требованиями системы.

    Args:
        data_source (dict): Словарь с исходными данными

    Returns:
        Обновленные словарь
    """
    # Создаем глубокую копию входных данных, чтобы избежать изменения оригинала
    data = copy.deepcopy(data_source)

    # Переименование ключей верхнего уровня с использованием значений по умолчанию
    data["ИмпМорскаяПеревозкаДатаПолученияДУ"] = data.pop("document_created_datetime", "")
    # data["ИмпМорскаяПеревозкаНомерРейсаФидер"] = data.pop("voyage_number", "")
    data.pop("voyage_number", None)  # Временно
    # Инициализация бинарного признака "ЭтоКоносамент"
    data["ЭтоКоносамент"] = "true" if data.pop("document_type", "КС") == "КС" else "false"

    # Обработка списка контейнеров
    for container in data.get("containers", []):
        # Переименование ключей в словаре контейнера
        container["ИмпМорскаяПеревозкаНомерПломбы"] = container.pop("seals", [])
        container["ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера"] = container.pop("upload_datetime", "")
        # Удаление поля note
        container.pop("note", None)

    return data
