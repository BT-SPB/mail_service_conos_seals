import shutil
import traceback
from pathlib import Path

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


def process_output_ocr(
        email_user: str,
        email_pass: str,
        smtp_server: str,
        smtp_port: int
) -> None:
    """
    Обрабатывает результаты OCR, извлекая номера сделок из ЦУП по коносаменту и отправляя номера пломб ЦУП.

    Функция сканирует директории с результатами OCR, проверяет наличие метаданных, обрабатывает файлы,
    взаимодействует с ЦУП для получения номеров транзакций, перемещает файлы в папки успешной обработки
    или ошибок, отправляет уведомления по email при наличии ошибок и очищает пустые директории.

    Args:
        email_user: Адрес электронной почты для отправки уведомлений
        email_pass: Пароль от почтового ящика
        smtp_server: Адрес SMTP-сервера для отправки email
        smtp_port: Порт SMTP-сервера

    Returns:
        None: Функция не возвращает значений, но изменяет файловую систему и отправляет email.
    """
    # Получаем список директорий с файлом metadata.json для обработки
    folders_for_processing: list[Path] = [
        folder for folder in CONFIG.OUT_OCR_FOLDER.iterdir()
        if folder.is_dir() and (folder / "metadata.json").is_file()
    ]

    # Логируем информацию о найденных директориях
    if not folders_for_processing:
        logger.info("➖ Новых директорий для обработки нет")
        return

    logger.info(f"📁 Обнаружено директорий для обработки: {len(folders_for_processing)}")

    # Последовательно обрабатываем каждую директорию
    for folder in folders_for_processing:
        try:
            # Читаем метаданные из JSON-файла, содержащего информацию о файлах и ошибках
            metadata_file = folder / "metadata.json"
            metadata: dict = read_json(metadata_file)
            success_flag: bool = False  # Флаг успешной обработки хотя бы одного файла

            # Формирование путей для папок ошибок и успешной обработки.
            # Используем sanitize_pathname для создания безопасных имен директорий
            error_folder = sanitize_pathname(CONFIG.ERROR_FOLDER / folder.name, is_file=False)
            success_folder = sanitize_pathname(CONFIG.SUCCESS_FOLDER / folder.name, is_file=False)

            # Проверяем целостность метаданных: файл не должен быть пустым,
            # а так же должны присутствовать все ключевые поля
            required_fields = {
                "subject": str,
                "sender": str,
                "date": str,
                "text_content": str,
                "files": list,
                "errors": list,
                "successes": list
            }
            if not metadata or not all(
                    isinstance(metadata.get(field), expected_type)
                    for field, expected_type in required_fields.items()
            ):
                warning_message = f"❌ Файл metadata.json имеет неверный формат или тип данных: {metadata_file}"
                logger.warning(warning_message)
                # Добавляем сообщение об ошибке в метаданные
                metadata.setdefault("errors", []).append(warning_message)
                write_json(metadata_file, metadata)
                # Перемещаем директорию в папку ошибок
                shutil.move(folder, error_folder)
                continue

            # Обрабатываем файлы (исходный и JSON), указанные в метаданных
            for source_file_name in metadata["files"]:
                source_file: Path = folder / source_file_name
                json_file: Path = source_file.with_name(source_file.name + ".json")

                # Проверяем существование исходного файла
                if not source_file.is_file():
                    logger.warning(f"❌ Отсутствует исходный файл {source_file} из metadata.json")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания.")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Проверяем существование JSON файла
                if not json_file.is_file():
                    logger.info(f"⚠️ Отсутствует JSON-файл {json_file}")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания.")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Читаем и валидируем данные из JSON
                json_data: dict = read_json(json_file)

                if not json_data.get("bill_of_lading"):
                    warning_message = "Не удалось получить номер коносамента."
                    logger.info(f"⚠️ {warning_message}: {json_file}")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                if not json_data.get("containers"):
                    warning_message = "Не удалось получить ни одного контейнера."
                    logger.info(f"⚠️ {warning_message}: {json_file}")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                if not all(
                        isinstance(cont, dict) and cont.get("container") and cont.get("seals")
                        for cont in json_data["containers"]
                ):
                    warning_message = "Не удалось получить номер пломбы для одного или нескольких контейнеров."
                    logger.info(f"⚠️ {warning_message}: {json_file}")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Запрашиваем номер транзакции из ЦУП по коносаменту
                # Пример получаемого значения: ["АА-0095444 от 14.04.2025"]
                transaction_numbers: list[str] = cup_http_request(
                    "TransactionNumberFromBillOfLading", json_data["bill_of_lading"]
                )
                if not (transaction_numbers and isinstance(transaction_numbers, list)):
                    warning_message = (f"Не удалось получить номер транзакции из ЦУП. "
                                       f"Возможно был неверно распознан номер коносамента "
                                       f"({json_data['bill_of_lading']}).")
                    logger.warning(f"⚠️ {warning_message}: {json_data['bill_of_lading']} ({json_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Обновляем JSON-данные дополнительной информацией
                json_data.update({
                    "transaction_numbers": transaction_numbers,
                    "source_file_name": f"КС_{json_data['bill_of_lading']}{source_file.suffix}",
                    "source_file_base64": file_to_base64(source_file),
                })
                # Сохраняем обновленный JSON-файл
                write_json(json_file, json_data)

                # Запрашиваем номера контейнеров по номеру транзакции
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

                # Проверяем успешность получения номеров контейнеров из ЦУП
                if not all(container_numbers_cup):
                    warning_message = "Не удалось получить номера контейнеров по номеру сделки из ЦУП"
                    logger.warning(f"⚠️ {warning_message}: {transaction_numbers} ({source_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Сравниваем номера контейнеров из OCR и ЦУП
                container_numbers_cup_set: set[str] = {x for sublist in container_numbers_cup for x in sublist}
                container_numbers_ocr_set: set[str] = {cont.get("container") for cont in json_data.get("containers")}

                # Проверяем, есть ли пересечение между наборами номеров контейнеров
                if not container_numbers_cup_set & container_numbers_ocr_set:
                    warning_message = "Номера контейнеров из OCR не пересекаются с номерами из ЦУП"
                    logger.warning(f"⚠️ {warning_message}: {transaction_numbers} ({source_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Проверяем наличие контейнеров, которые были распознаны, но отсутствуют в ЦУП
                container_numbers_difference = container_numbers_ocr_set - container_numbers_cup_set
                if container_numbers_difference:
                    # Отправляем сообщение, но не прерываем цикл, так как
                    # некоторые контейнеры были успешно распознаны
                    warning_message = (f"Были распознаны номера контейнеров, которые отсутствуют в ЦУП: "
                                       f"{container_numbers_difference}")
                    logger.warning(f"⚠️ {warning_message}: {transaction_numbers} ({source_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")

                # Отправляем номера пломб в ЦУП, если это включено в настройках
                if CONFIG.enable_send_production_data:
                    if not send_production_data(json_data):
                        warning_message = f"Не удалось загрузить номера пломб в ЦУП"
                        logger.warning(f"❌ {warning_message}: {json_file}")
                        metadata["errors"].append(f"{source_file_name}: Ошибка. {warning_message}")
                        transfer_files([source_file, json_file], error_folder, "move")
                        continue

                # Логируем успешную обработку и перемещаем файлы в директорию успешной обработки
                success_message = "\n".join([
                    f"{source_file_name}",
                    f"bill_of_lading: {json_data['bill_of_lading']}",
                    f"transaction_numbers: {json_data['transaction_numbers']}",
                    f"containers:",
                    *[f"    - {cont['container']}: {cont['seals']}"
                      for cont in json_data["containers"] if
                      cont["container"] not in container_numbers_difference]
                ])
                logger.info(f"✔️ Файл обработан успешно: {source_file}")
                metadata["successes"].append(success_message)
                transfer_files([source_file, json_file], success_folder, "move")
                success_flag = True

            # Сохранение обновленных метаданных после обработки всех файлов в директории
            write_json(folder / "metadata.json", metadata)

            # Начало формирования сообщения
            email_messages: list[str] = [
                f"Здравствуйте!\n"
                f"Это автоматическое уведомление по файлам, полученным от {metadata['sender']}.\n"
                f"Дата получения: {convert_email_date_to_moscow(metadata['date'])}"
            ]

            # Обрабатываем ошибки: копируем/перемещаем метаданные и подготавливаем email уведомление
            if metadata["errors"]:
                # Определяем действие с метаданными: копирование или перемещение
                transfer_files(
                    folder / "metadata.json",
                    error_folder,
                    operation="copy2" if success_flag else "move"
                )

                # Формируем текст письма с перечислением ошибок
                error_list = "\n".join(
                    f"    {i}. {error}" for i, error in enumerate(metadata["errors"], 1)
                )
                email_messages.append(
                    f"⚠️ Возникли ошибки при обработке следующих файлов:\n"
                    f"{error_list}\n\n"
                    f"Копии файлов доступны по пути: {error_folder}"
                )

            # Email уведомление об успешно обработанных файлах.
            # Формируем при включенной настройке в конфиге
            if CONFIG.enable_success_notifications and metadata["successes"]:
                # Формируем нумерованный список успешно обработанных файлов.
                # Каждая строка начинается с "    {i}. ", где i — номер (например: "    1. ").
                # Если в success есть переносы строк, добавляем нужный отступ к каждой новой строке
                # для выравнивания по отступу, соответствующему началу первой строки с нумерацией.
                success_list = "\n\n".join(
                    f"    {i}. {success.replace(chr(10), chr(10) + ' ' * (len(str(i)) + 6))}"  # chr(10) = "\n"
                    for i, success in enumerate(metadata["successes"], 1)  # i — номер с 1, success — текст об обработке
                )
                email_messages.append(
                    f"✅ Успешно обработанные файлы (данные загружены в ЦУП):\n"
                    f"{success_list}"
                )

            # Отправка письма только если есть дополнительная информация (ошибки или успехи)
            if len(email_messages) > 1:
                send_email(
                    email_text="\n\n\n".join(email_messages),
                    # recipient_emails=metadata["sender"],
                    recipient_emails=CONFIG.notification_emails,
                    subject=f"Автоответ от {email_user}",
                    email_user=email_user,
                    email_pass=email_pass,
                    smtp_server=smtp_server,
                    smtp_port=smtp_port,
                    email_format="plain"
                )

            # Если есть успешные файлы, перемещаем метаданные в папку успеха
            if success_flag:
                transfer_files(folder / "metadata.json", success_folder, "move")

            # Очищаем директорию: удаляем, если пуста, или перемещаем остатки
            if is_directory_empty(folder):
                folder.rmdir()
                logger.info(f"✔️ Удалена пустая директория: {folder}")
            else:
                residual_destination = error_folder / f"residual_files"
                shutil.move(folder, residual_destination)
                logger.warning(
                    f"❗❗❗ Остались необработанные файлы в {folder.name}. "
                    f"Перемещены в {residual_destination} для ручной проверки"
                )

        except Exception as e:
            logger.error(f"⛔ Ошибка при обработке директории {folder}: {e}\n{traceback.format_exc()}")
            continue
