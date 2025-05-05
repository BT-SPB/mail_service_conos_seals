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


def format_json_data(
        json_data: dict[str, any],
        title: str | None = None
) -> str:
    """Форматирует данные из словаря JSON в строку с информацией о коносаменте, транзакциях и контейнерах.

    Проверяет наличие ключей 'bill_of_lading', 'transaction_numbers' и 'containers' в словаре.
    Для 'containers' проверяет, что это список словарей, содержащих ключи 'container' и 'seals'.
    Если задан заголовок (title), добавляет его в начало строки. Формирует строку в формате:
        <title>
        bill_of_lading: <значение>
        transaction_numbers: <значение>
        containers:
            • <container>: <seals>
    Добавляет только строки для существующих и валидных полей. Если в результате только заголовок,
    возвращается пустая строка.

    Args:
        json_data: Словарь с данными, содержащий ключи 'bill_of_lading' (строка),
            'transaction_numbers' (список строк), 'containers' (список словарей
            с ключами 'container' и 'seals')
        title: Заголовок для добавления в начало строки (опционально, по умолчанию None)

    Returns:
        str: Отформатированная строка с данными. Если нет валидных полей или только заголовок,
            возвращается пустая строка.
    """
    # Инициализируем список строк для формирования результата
    result_lines: list[str] = []

    # Добавляем заголовок, если он задан и не пустой
    if title and title.strip():
        result_lines.append(title)

    # Считаем, сколько строк было до добавления данных (для проверки только заголовка)
    initial_length = len(result_lines)

    # Добавляем bill_of_lading, если ключ существует и значение не пустое
    if bill_of_lading := json_data.get("bill_of_lading"):
        result_lines.append(f"bill_of_lading: {bill_of_lading}")

    # Добавляем transaction_numbers, если ключ существует и значение не пустое
    if transaction_numbers := json_data.get("transaction_numbers"):
        result_lines.append(f"transaction_numbers: {transaction_numbers}")

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
                result_lines.append("containers:")
                result_lines.extend(
                    f"{' ' * 4}- {cont['container']}: {cont['seals']}"
                    for cont in valid_containers
                )

    # Возвращаем пустую строку, если добавлен только заголовок или ничего
    return "\n".join(result_lines) if len(result_lines) > initial_length else ""


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
        logger.info("➖ Новых директорий для обработки нет")
        return

    logger.info(f"📁 Обнаружено директорий для обработки: {len(folders_for_processing)}")

    # Последовательно обрабатываем каждую директорию
    for folder in folders_for_processing:
        try:
            # Читаем метаданные из файла metadata.json
            metadata_file: Path = folder / "metadata.json"
            metadata: dict[str, any] = read_json(metadata_file)

            # Формируем пути для папок ошибок и успешной обработки с безопасными именами
            error_folder = sanitize_pathname(CONFIG.ERROR_FOLDER / folder.name, is_file=False)
            success_folder = sanitize_pathname(CONFIG.SUCCESS_FOLDER / folder.name, is_file=False)

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
                    formatted_json_data = format_json_data(json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):")
                    logger.warning(f"⚠️ {error_message} ({json_file})")
                    metadata["errors"][source_file_name].append(f"{error_message}{formatted_json_data}")
                    transfer_files(files_to_transfer, error_folder, "move")
                    continue

                # Обновляем JSON-данные дополнительной информацией
                json_data.update({
                    "transaction_numbers": transaction_numbers,
                    "source_file_name": f"КС_{json_data['bill_of_lading']}{source_file.suffix}",
                    "source_file_base64": file_to_base64(source_file),
                })
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
                    formatted_json_data = format_json_data(json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):")
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
                    formatted_json_data = format_json_data(json_data, "\nРаспознанные данные (НЕ загружены в ЦУП):")
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
                        formatted_json_data = format_json_data(json_data, "\nРаспознанные данные:")
                        logger.warning(f"❌ {error_message} ({json_file})")
                        metadata["errors"][source_file_name].append(f"{error_message}{formatted_json_data}")
                        transfer_files(files_to_transfer, error_folder, "move")
                        continue
                else:
                    logger.info(
                        "🔔 Отправка данных в ЦУП отключена настройкой "
                        "enable_send_production_data"
                    )

                # Формируем сообщение об успехе и перемещаем файлы в директорию успешной обработки
                success_message = format_json_data(json_data, "Загруженные данные:")
                logger.info(f"✔️ Файл обработан успешно: {source_file}")
                metadata["successes"][source_file_name].append(success_message)
                transfer_files(files_to_transfer, success_folder, "move")

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

            # Формируем и отправляем email, если есть сообщения
            email_text = format_email_message(metadata, error_folder)
            if email_text:
                send_email(
                    email_text=email_text,
                    # recipient_emails=metadata["sender"],
                    recipient_emails=CONFIG.notification_emails,
                    subject=f"Автоответ от {email_user}",
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
