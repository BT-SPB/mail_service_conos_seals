import shutil
import logging
import time
from pathlib import Path

from config import config
from src.utils import (
    write_json,
    write_text,
    transfer_files,
    sanitize_pathname,
    is_directory_empty,
)
from src.utils_tsup import tsup_http_request, send_data_to_tsup
from src.utils_email import send_email
from src.utils_data_process import fetch_transaction_numbers, correct_container_numbers
from src.models.enums import DocType
from src.models.metadata_model import StructuredMetadata
from src.models.document_model import StructuredDocument

logger = logging.getLogger(__name__)


def process_output_ocr() -> None:
    """
    Обрабатывает результаты OCR, извлекая номера сделок из ЦУП по коносаменту и отправляя номера пломб ЦУП.

    Функция сканирует директории с результатами OCR, проверяет метаданные, обрабатывает файлы,
    взаимодействует с ЦУП для получения номеров транзакций, перемещает файлы в папки успешной обработки
    или ошибок, отправляет email-уведомления и очищает директории.

    Args:
        None

    Returns:
        None: Функция изменяет файловую систему, отправляет email, но не возвращает значений.
    """
    # Получаем список директорий, содержащих файл metadata.json
    folders_to_process: list[Path] = [
        folder for folder in config.OUTPUT_DIR.iterdir()
        if folder.is_dir() and (folder / "metadata.json").is_file()
    ]

    # Если директорий нет, логируем и завершаем выполнение
    if not folders_to_process:
        logger.debug("➖ Новых директорий для обработки нет")
        return

    logger.info(f"📁 Обнаружено директорий для обработки: {len(folders_to_process)}")

    # Последовательно обрабатываем каждую директорию
    for folder in folders_to_process:
        try:
            # Читаем метаданные из файла metadata.json
            metadata_path: Path = folder / "metadata.json"
            metadata: StructuredMetadata = StructuredMetadata.load(metadata_path)

            # Формируем пути для папок ошибок и успешной обработки с безопасными именами
            error_subdir = sanitize_pathname(config.ERROR_DIR, folder.name, is_file=False)
            success_subdir = sanitize_pathname(config.SUCCESS_DIR, folder.name, is_file=False)

            metadata.error_dir = error_subdir
            metadata.success_dir = success_subdir

            container_notes: list[str] = []

            # # Проверяем целостность метаданных: наличие и типы всех обязательных полей
            # required_fields = {
            #     "subject": str,
            #     "sender": str,
            #     "date": str,
            #     "text_content": str,
            #     "files": list,
            #     "errors": dict,
            #     "partial_successes": dict,
            #     "successes": dict
            # }
            # if not metadata or not all(
            #         isinstance(metadata.get(field), expected_type)
            #         for field, expected_type in required_fields.items()
            # ):
            #     error_message = (f"Файл metadata.json имеет неверный формат "
            #                      f"или тип данных: {metadata_path}")
            #     logger.warning(f"❌ {error_message}")
            #     metadata["GLOBAL_ERROR"] = error_message
            #     write_json(metadata_path, metadata)
            #     # Перемещаем директорию в папку ошибок
            #     shutil.move(folder, error_subdir)
            #     continue

            # Проверяем, есть ли файлы для обработки
            if not metadata.files:
                error_message = f"В metadata.json нет файлов для обработки: {metadata_path}"
                logger.warning(f"❌ {error_message}")
                metadata.global_errors.add(error_message)
                metadata.save(metadata_path)
                shutil.move(folder, error_subdir)
                continue

            # Обрабатываем каждый файл из метаданных
            for source_file_name in metadata.files:
                source_file_path: Path = folder / source_file_name
                json_path: Path = folder / f"{source_file_name}.json"
                json_path_tsup: Path = folder / f"{source_file_name}_tsup.json"
                files_to_transfer = [source_file_path, json_path, json_path_tsup]

                # Проверяем существование исходного файла
                if not source_file_path.is_file():
                    error_message = "Исходный файл отсутствует."
                    logger.warning(f"❌ {error_message} ({source_file_path})")
                    metadata.errors[source_file_name].add(error_message)
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Проверяем существование JSON файла
                if not json_path.is_file():
                    error_message = "JSON-файл с данными OCR отсутствует."
                    logger.warning(f"⚠️ {error_message} ({json_path})")
                    metadata.errors[source_file_name].add(error_message)
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Читаем данные из JSON, загружая в pydantic модель
                document: StructuredDocument = StructuredDocument.load(json_path)
                document.file_path = source_file_path

                # Проверяем наличие номера коносамента
                if not document.bill_of_lading:
                    error_message = "Номер коносамента отсутствует или не распознан."
                    logger.warning(f"⚠️ {error_message} ({json_path})")
                    document.errors.add(error_message)
                    document.save(json_path)
                    metadata.errors[source_file_name].update(document.format_report_with_errors())
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Проверяем наличие контейнеров
                if not document.containers:
                    error_message = "Информация о контейнерах отсутствует или не распознана."
                    logger.warning(f"⚠️ {error_message} ({json_path})")
                    document.errors.add(error_message)
                    document.save(json_path)
                    metadata.errors[source_file_name].update(document.format_report_with_errors())
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Проверяем наличие пломб,
                # кроме ДУ от теринала НМТП, в котором пломб не предусмотрено
                if document.document_type != DocType.DU_NMTP:
                    # Определяем контейнеры с пустыми номерами пломб
                    containers_with_empty_seals: set[str] = {
                        cont.container for cont in document.containers
                        if not cont.seals
                    }

                    # Если все контейнеры имеют пустые пломбы
                    if len(containers_with_empty_seals) == len(document.containers):
                        error_message = (f"Номера пломб отсутствуют для всех контейнеров: "
                                         f"{', '.join(containers_with_empty_seals)}.")
                        logger.warning(f"⚠️ {error_message} ({json_path})")
                        document.errors.add(error_message)
                        document.save(json_path)
                        metadata.errors[source_file_name].update(document.format_report_with_errors())
                        transfer_files(files_to_transfer, error_subdir, "move")
                        continue

                    # Если есть контейнеры с пустыми пломбами, логируем частичную ошибку
                    if containers_with_empty_seals:
                        error_message = (f"Номера пломб отсутствуют для части контейнеров: "
                                         f"{', '.join(containers_with_empty_seals)}.")
                        logger.warning(f"⚠️ {error_message} ({json_path})")
                        document.errors.add(error_message)
                        # Удаляем контейнеры с пустым полем "seals"
                        document.containers = [
                            cont for cont in document.containers
                            if cont.container not in containers_with_empty_seals
                        ]

                # Запрашиваем номер транзакции из ЦУП по коносаменту
                fetch_transaction_numbers(document)

                # Проверяем, получены ли номера транзакций
                if not document.transaction_numbers:
                    error_message = (
                        f"Номер транзакции из ЦУП отсутствует. "
                        f"Возможно, номер коносамента ({document.bill_of_lading}) "
                        f"распознан неверно."
                    )
                    logger.warning(f"⚠️ {error_message} ({json_path})")
                    document.errors.add(error_message)
                    document.save(json_path)
                    metadata.errors[source_file_name].update(document.format_report_with_errors())
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Запрашиваем номера контейнеров по каждому номеру транзакции
                container_numbers_cup: list[list[str]] = [
                    # Очищаем полученные номера от лишних пробелов
                    [number.strip() for number in tsup_http_request(
                        "GetTransportPositionNumberByTransactionNumber",
                        # Извлекаем только номер, отсекая дату (например, "АА-0095444 от 14.04.2025" → "АА-0095444"
                        transaction_number.split()[0],
                        encode=False
                    )]
                    for transaction_number in document.transaction_numbers
                ]

                # Проверяем, получены ли номера контейнеров
                if not any(container_numbers_cup):
                    error_message = (
                        f"Номера контейнеров по номеру сделки ({document.transaction_numbers}) "
                        f"из ЦУП отсутствуют."
                    )
                    logger.warning(f"⚠️ {error_message} ({source_file_path})")
                    document.errors.add(error_message)
                    document.save(json_path)
                    metadata.errors[source_file_name].update(document.format_report_with_errors())
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Сравниваем номера контейнеров из OCR и ЦУП
                container_numbers_cup_set: set[str] = {
                    x
                    for sublist in container_numbers_cup
                    for x in sublist
                }

                correct_container_numbers(document, container_numbers_cup_set)

                container_numbers_ocr_set: set[str] = {cont.container for cont in document.containers}

                # Проверяем, есть ли совпадения между наборами номеров
                if not container_numbers_cup_set & container_numbers_ocr_set:
                    error_message = (
                        f"Номера контейнеров из OCR ({', '.join(container_numbers_ocr_set)}) "
                        f"не совпадают с номерами из ЦУП ({', '.join(container_numbers_cup_set)}) "
                        f"по номеру сделки {document.transaction_numbers}."
                    )
                    logger.warning(f"⚠️ {error_message} ({source_file_path})")
                    document.errors.add(error_message)
                    document.save(json_path)
                    metadata.errors[source_file_name].update(document.format_report_with_errors())
                    transfer_files(files_to_transfer, error_subdir, "move")
                    continue

                # Проверяем наличие контейнеров, которые были распознаны, но отсутствуют в ЦУП
                missing_containers: set[str] = container_numbers_ocr_set - container_numbers_cup_set
                if missing_containers:
                    # Отправляем сообщение, но не прерываем цикл, так как
                    # некоторые контейнеры были успешно распознаны
                    error_message = (
                        f"Некоторые из распознанных номеров контейнеров ({', '.join(missing_containers)}) "
                        f"отсутствуют в данных ЦУП по номеру сделки {document.transaction_numbers}."
                    )
                    logger.warning(f"⚠️ {error_message} ({source_file_path})")
                    document.errors.add(error_message)
                    document.containers = [
                        cont for cont in document.containers
                        if cont.container not in missing_containers
                    ]

                # Формируем имя файла для ЦУП и кодируем сам файл в base64 для передачи.
                document.encode_file()

                # Подготовка данных для подачи в ЦУП
                data_for_tsup = document.to_tsup_dict()

                # Сохраняем копию данных
                write_json(json_path_tsup, data_for_tsup)

                # Отправляем данные в ЦУП, если включена настройка
                if config.enable_send_data_to_tsup:
                    # Отправляем данные в ЦУП. Функция возвращает флаг успешности отправки
                    is_send_production_data = send_data_to_tsup(
                        "SendProductionDataToTransaction", data_for_tsup
                    )
                    # Если не удалось отправить данные
                    if is_send_production_data:
                        document.is_data_sent_to_tsup = True
                    else:
                        error_message = (
                            f"Не удалось загрузить данные в ЦУП "
                            f"по номеру сделки {document.transaction_numbers}"
                        )
                        logger.warning(f"❌ {error_message} ({json_path})")
                        document.errors.add(error_message)
                        document.save(json_path)
                        metadata.errors[source_file_name].update(document.format_report_with_errors())
                        transfer_files(files_to_transfer, error_subdir, "move")
                        continue
                else:
                    logger.info(
                        "🔔 Отправка данных в ЦУП отключена настройкой 'enable_send_data_to_tsup'"
                    )

                # Формируем сообщение об успехе и перемещаем файлы в директорию успешной обработки
                logger.info(f"✔️ Файл обработан успешно: {source_file_path}")
                document.save(json_path)
                if document.errors:
                    metadata.partial_successes[source_file_name].update(document.format_report_with_errors())
                else:
                    metadata.successes[source_file_name].update(document.format_report_with_errors())

                transfer_files(files_to_transfer, success_subdir, "move")

                # Добавляем в список все примечания для контейнеров
                container_notes.extend(cont.note for cont in document.containers if cont.note)

            # Сохраняем обновленные метаданные после обработки всех файлов в директории
            metadata.save(metadata_path)

            # Удаляем дубликаты из списка примечаний для контейнеров с сохранением порядка.
            # Уникальные примечания выносятся в тему email письма
            container_notes = list(dict.fromkeys(container_notes))

            # Формируем и отправляем email, если есть сообщения
            email_text = metadata.email_report()
            if email_text:
                subject = (
                        f"Автоответ: {metadata.subject}" +
                        (f" + {', '.join(container_notes)}" if container_notes else "")
                )

                send_email(
                    email_text=email_text,
                    recipient_emails=config.notification_emails,
                    subject=subject,
                    email_format="html",
                )

            if config.block_processed_files_to_output:
                write_text(folder / "email_data.html", email_text)
                time.sleep(5)
            else:
                # Копируем metadata.json в error_subdir, если есть ошибки или частичные успехи
                if metadata.errors or metadata.partial_successes:
                    transfer_files(metadata_path, error_subdir, "copy2")
                    write_text(error_subdir / "email_data.html", email_text)

                # Перемещаем metadata.json в success_subdir, если есть успехи
                if metadata.successes:
                    transfer_files(metadata_path, success_subdir, "move")
                    write_text(success_subdir / "email_data.html", email_text)

                # Удаляем metadata.json из исходной директории (при наличии).
                # Условие сработает, если не было успехов.
                if metadata_path.exists():
                    try:
                        metadata_path.unlink()
                    except OSError as e:
                        logger.error(f"⚠️ Не удалось удалить {metadata_path}: {e}")

                # Очищаем директорию: удаляем, если пуста, или перемещаем остатки
                if is_directory_empty(folder):
                    folder.rmdir()
                    logger.info(f"✔️ Удалена пустая директория: {folder}")
                else:
                    residual_destination = error_subdir / f"residual_files"
                    shutil.move(folder, residual_destination)
                    logger.error(
                        f"❗❗❗ В директории {folder.name} остались необработанные файлы. "
                        f"Они перемещены в {residual_destination} для ручной проверки"
                    )

        except Exception as e:
            logger.exception(f"⛔ Ошибка при обработке директории {folder}: {e}")
            time.sleep(2)
            continue
