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
from src.utils_1c import cup_http_request
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
    # Получение списка директорий с проверкой наличия metadata.json
    folders_for_processing: list[Path] = [
        folder for folder in CONFIG.OUT_OCR_FOLDER.iterdir()
        if folder.is_dir() and (folder / "metadata.json").exists()
    ]

    if folders_for_processing:
        logger.info(f"Обнаружено директорий для обработки: {len(folders_for_processing)}")
    else:
        logger.info("Директории для обработки не найдены")
        return

    # Последовательно обрабатываем каждую директорию
    for folder in folders_for_processing:
        # Чтение метаданных из файла metadata.json
        # Метаданные содержат информацию о файлах и ошибках
        metadata: dict = read_json(folder / "metadata.json")
        success_flag: bool = False  # Флаг успешной обработки хотя бы одного файла

        # Формирование путей для папок ошибок и успешной обработки.
        # Используем sanitize_pathname для безопасных имен директорий
        error_folder = CONFIG.ERROR_FOLDER / sanitize_pathname(
            folder.name, is_file=False, parent_dir=CONFIG.ERROR_FOLDER
        )
        success_folder = CONFIG.SUCCESS_FOLDER / sanitize_pathname(
            folder.name, is_file=False, parent_dir=CONFIG.SUCCESS_FOLDER
        )

        # Обрабатываем файлы, указанные в метаданных
        for source_file_name, json_file_name in metadata["files"]:
            source_file: Path = folder / source_file_name
            json_file: Path = folder / json_file_name

            # Проверяем существование исходного файла
            if not source_file.exists():
                logger.warning(f"Отсутствует исходный файл {source_file} из metadata.json")
                metadata["errors"].append(f"{source_file_name}: Ошибка распознавания.")
                transfer_files([source_file, json_file], error_folder, "move")
                continue

            # Проверяем существование JSON файла
            if not json_file.exists():
                logger.info(f"Отсутствует json file {json_file}")
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
                logger.info(f"Отсутствуют обязательные поля в {json_file}")
                metadata["errors"].append(f"{source_file_name}: Ошибка распознавания.")
                transfer_files([source_file, json_file], error_folder, "move")
                continue

            # Запрос номера транзакции из ЦУП
            transaction_number_raw = cup_http_request(
                "TransactionNumberFromBillOfLading", json_data["bill_of_lading"]
            )
            if not (transaction_number_raw and isinstance(transaction_number_raw, list)):
                logger.info(
                    f"Не удалось получить номер транзакции для коносамента "
                    f"{json_data['bill_of_lading']} из ЦУП: {json_file}"
                )
                metadata["errors"].append(
                    f"{source_file_name}: Ошибка распознавания. "
                    f"Не удалось получить номер транзакции из ЦУП."
                )
                transfer_files([source_file, json_file], error_folder, "move")
                continue

            # Обновляем данные в JSON файле
            json_data.update({
                "transaction_number": transaction_number_raw[0],
                "source_file_base64": file_to_base64(source_file),
                "source_file_ext": source_file.suffix,
            })
            # Логируем успешную обработку
            logger.info(f"Файл Файл обработан успешно: {source_file}")
            # Сохранение обновленного JSON
            write_json(json_file, json_data)
            # Перемещение файлов в папку успешной обработки
            transfer_files([source_file, json_file], success_folder, "move")
            success_flag = True

        # Сохранение обновленных метаданных после обработки всех файлов в директории
        write_json(folder / "metadata.json", metadata)

        # Обработка ошибок: копирование метаданных и отправка уведомления на email отправителя
        if metadata["errors"]:
            transfer_files(folder / "metadata.json", error_folder, "copy2" if success_flag else "move")

            error_files_text = "\n".join(
                f"    {i}.  {error_file}"
                for i, error_file in enumerate(metadata["errors"], 1)
            )
            email_text = (
                f"В сообщении от {metadata['date']} среди прикрепленных файлов "
                f"следующие не удалось распознать:\n"
                f"{error_files_text}\n\n"
                f"Копии данных файлов можно найти по ссылке: {error_folder}"
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

        # Перемещение метаданных в папку успешной обработки, если были успешные файлы
        if success_flag:
            transfer_files(folder / "metadata.json", success_folder, "move")

        # Очищаем директорию, перемещая остаточные файлы или удаляя пустую папку.
        if is_directory_empty(folder):
            folder.rmdir()
            logger.info(f"Удалена пустая директория: {folder}")
        else:
            residual_destination = error_folder / f"residual_files"
            shutil.move(folder, residual_destination)
            logger.warning(
                f"В директории {folder.name} остались необработанные файлы. "
                f"Перемещены в {residual_destination} для ручной проверки."
            )
