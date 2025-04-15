import shutil
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
from src.utils_email import send_email


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
    if folders_for_processing:
        logger.info(f"🔍 Обнаружено директорий для обработки: {len(folders_for_processing)}")
    else:
        logger.info("🗿 Директории для обработки не найдены")
        return

    # Последовательно обрабатываем каждую директорию
    for folder in folders_for_processing:
        try:
            # Читаем метаданные из JSON-файла, содержащего информацию о файлах и ошибках
            metadata: dict = read_json(folder / "metadata.json")
            success_flag: bool = False  # Флаг успешной обработки хотя бы одного файла

            # Формирование путей для папок ошибок и успешной обработки.
            # Используем sanitize_pathname для создания безопасных имен директорий
            error_folder = CONFIG.ERROR_FOLDER / sanitize_pathname(
                folder.name, is_file=False, parent_dir=CONFIG.ERROR_FOLDER
            )
            success_folder = CONFIG.SUCCESS_FOLDER / sanitize_pathname(
                folder.name, is_file=False, parent_dir=CONFIG.SUCCESS_FOLDER
            )

            # Обрабатываем файлы (исходный и JSON), указанные в метаданных
            for source_file_name, json_file_name in metadata["files"]:
                source_file: Path = folder / source_file_name
                json_file: Path = folder / json_file_name

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
                if not (
                        json_data.get("bill_of_lading") and
                        json_data.get("containers") and
                        all(
                            isinstance(cont, dict) and cont.get("container") and cont.get("seals")
                            for cont in json_data["containers"]
                        )
                ):
                    logger.info(f"⚠️ Отсутствуют обязательные поля в {json_file}")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания.")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Запрашиваем номер транзакции из ЦУП по коносаменту
                # Пример получаемого значения: ["АА-0095444 от 14.04.2025"]
                transaction_number_raw: list[str] = cup_http_request(
                    "TransactionNumberFromBillOfLading", json_data["bill_of_lading"]
                )
                if not (transaction_number_raw and isinstance(transaction_number_raw, list)):
                    warning_message = "Не удалось получить номер транзакции из ЦУП"
                    logger.warning(f"❌ {warning_message}: {json_data['bill_of_lading']} ({json_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Извлекаем последний номер транзакции (самый новый по дате)
                transaction_number: str = transaction_number_raw[-1]
                # Запрашиваем номера контейнеров по номеру транзакции
                container_numbers_cup: list[str] = cup_http_request(
                    "GetTransportPositionNumberByTransactionNumber",
                    # Берем только часть с номером "АА-0095444" игнорируя "от 14.04.2025"
                    transaction_number.split()[0],
                    encode=False
                )

                # Обновляем JSON-данные дополнительной информацией
                json_data.update({
                    "transaction_number": transaction_number,
                    "source_file_base64": file_to_base64(source_file),
                    "source_file_name": source_file.name,
                })
                # Сохраняем обновленный JSON-файл
                write_json(json_file, json_data)

                # Проверяем успешность получения номеров контейнеров из ЦУП
                if not container_numbers_cup:
                    warning_message = "Не удалось получить номера контейнеров по номеру сделки из ЦУП"
                    logger.warning(f"❌ {warning_message}: {transaction_number} ({source_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Сравниваем номера контейнеров из OCR и ЦУП
                container_numbers_cup_set: set[str] = set(container_numbers_cup)
                container_numbers_ocr_set: set[str] = {cont.get("container") for cont in json_data.get("containers")}

                # Проверяем, есть ли пересечение между наборами номеров контейнеров
                if not container_numbers_cup_set & container_numbers_ocr_set:
                    warning_message = "Номера контейнеров из OCR не пересекаются с номерами из ЦУП"
                    logger.warning(f"❌ {warning_message}: {transaction_number} ({source_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")
                    continue

                # Проверяем наличие недостающих контейнеров
                container_numbers_difference = container_numbers_ocr_set - container_numbers_cup_set
                if container_numbers_difference:
                    # Отправляем сообщение, но не прерываем цикл, так как
                    # некоторые контейнеры были успешно распознаны
                    warning_message = (f"Были распознаны номера контейнеров, которые отсуствуют в ЦУП: "
                                       f"{container_numbers_difference}")
                    logger.warning(f"❌ {warning_message}: {transaction_number} ({source_file})")
                    metadata["errors"].append(f"{source_file_name}: Ошибка распознавания. {warning_message}")
                    transfer_files([source_file, json_file], error_folder, "move")

                # Отправляем номера пломб в ЦУП
                # При неудаче логируем
                # if not send_production_data(json_data):
                #     warning_message = f"Не удалось загрузить номера пломб в ЦУП"
                #     logger.warning(f"❌ {warning_message}: {json_file}")
                #     metadata["errors"].append(f"{source_file_name}: Ошибка. {warning_message}")
                #     transfer_files([source_file, json_file], error_folder, "move")
                #     continue

                # Логируем успешную обработку и перемещаем файлы в папку успешной обработки
                logger.info(f"✔️ Файл Файл обработан успешно: {source_file}")
                transfer_files([source_file, json_file], success_folder, "move")
                success_flag = True

            # Сохранение обновленных метаданных после обработки всех файлов в директории
            write_json(folder / "metadata.json", metadata)

            # Обрабатываем ошибки: копируем/перемещаем метаданные и отправляем уведомления на email отправителя
            if metadata["errors"]:
                # Определяем действие с метаданными: копирование или перемещение
                metadata_action = "copy2" if success_flag else "move"
                transfer_files(folder / "metadata.json", error_folder, metadata_action)

                # Формируем текст письма с перечислением ошибок
                error_files_text = "\n".join(
                    f"    {i}.  {error}"
                    for i, error in enumerate(metadata["errors"], 1)
                )
                email_text = (
                    f"В сообщении от {metadata['date']} следующие файлы не удалось распознать:\n"
                    f"{error_files_text}\n\n"
                    f"Копии файлов доступны по пути: {error_folder}"
                )
                send_email(
                    email_text=email_text,
                    recipient_email=metadata["sender"],
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
            logger.error(f"⛔ Ошибка при обработке директории {folder}: {e}")
            continue
