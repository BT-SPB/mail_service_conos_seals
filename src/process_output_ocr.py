import shutil
import logging
from pathlib import Path
from collections import defaultdict

from config import config
from src.utils import (
    read_json,
    write_json,
    transfer_files,
    sanitize_pathname,
    is_directory_empty,
)
from src.utils_tsup import tsup_http_request, send_production_data
from src.utils_email import send_email
from src.utils_data_process import (
    update_json,
    format_json_data_to_mail,
    format_email_message,
    remap_production_data_for_1c,
)
from src.models.enums import DocType

logger = logging.getLogger(__name__)


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
        folder for folder in config.OUT_OCR_FOLDER.iterdir()
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
            error_folder = sanitize_pathname(config.ERROR_FOLDER, folder.name, is_file=False)
            success_folder = sanitize_pathname(config.SUCCESS_FOLDER, folder.name, is_file=False)

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
                json_file: Path = folder / f"{source_file_name}.json"
                json_file_1c: Path = folder / f"{source_file_name}_1c.json"
                files_to_transfer = [source_file, json_file, json_file_1c]

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

                document_type: DocType = DocType(json_data.get("document_type", None))

                # Проверяем наличие пломб, кроме ДУ от теринала НМТП, в котором пломб не предусмотрено
                if document_type != DocType.DU_NMTP:
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
                transaction_numbers: list[str] = tsup_http_request(
                    "TransactionNumberFromBillOfLading", json_data["bill_of_lading"]
                )

                # Если транзакции не найдены и коносамент заканчивается на `SRV`, пробуем без суффикса
                if not transaction_numbers and json_data["bill_of_lading"].endswith("SRV"):
                    bill_of_lading = json_data["bill_of_lading"].removesuffix("SRV")
                    transaction_numbers: list[str] = tsup_http_request(
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
                    [number.strip() for number in tsup_http_request(
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

                # Подготовка данных для подачи в ЦУП: создание глубокой копии, чтобы избежать изменения оригинала,
                # переименование и удаление полей для соответствия формату 1С
                json_data_1c = remap_production_data_for_1c(json_data)
                write_json(json_file_1c, json_data_1c)

                # Отправляем данные в ЦУП, если включена настройка
                if config.enable_send_production_data:
                    # Отправляем данные в ЦУП. Функция возвращает флаг успешности отправки
                    is_send_production_data = send_production_data(json_data_1c)
                    # Если не удалось отправить данные
                    if not is_send_production_data:
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
                    logger.info(
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
                    recipient_emails=config.notification_emails,
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
