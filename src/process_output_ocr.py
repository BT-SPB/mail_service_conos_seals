from config import CONFIG
from src.logger import logger
from src.utils import read_json, write_json, file_to_base64, transfer_files, sanitize_pathname
from src.utils_1c import cup_http_request
from src.utils_email import send_email


def process_output_ocr():
    folders_for_processing = [
        folder for folder in CONFIG.OUT_OCR_FOLDER.iterdir()
        if folder.is_dir() and (folder / "metadata.json").exists()
    ]

    if folders_for_processing:
        logger.print(f"Обнаружено директорий для обработки: {len(folders_for_processing)}")

    for folder in folders_for_processing:
        metadata = read_json(folder / "metadata.json")

        success_flag = False

        error_folder = CONFIG.ERROR_FOLDER / sanitize_pathname(folder.name, is_file=False,
                                                               parent_dir=CONFIG.ERROR_FOLDER)

        success_folder = CONFIG.SUCCESS_FOLDER / sanitize_pathname(folder.name, is_file=False,
                                                                   parent_dir=CONFIG.SUCCESS_FOLDER)

        for source_file_name, json_file_name in metadata["files"]:
            source_file = folder / source_file_name
            json_file = folder / json_file_name

            if not source_file.exists():
                logger.print(f"Отсутствует исходный файл {source_file}, который записан в metadata.json")
                transfer_files(
                    file_paths=(source_file, json_file),
                    destination_folder=error_folder,
                    operation="move"
                )
                continue

            if not json_file.exists():
                logger.print(f"Отсутствует json file {json_file}")
                metadata["errors"].append(f"{source_file_name}: ошибка распознавания.")
                transfer_files(
                    file_paths=(source_file, json_file),
                    destination_folder=error_folder,
                    operation="move"
                )
                continue

            json_data = read_json(json_file)
            if not (
                    json_data.get("bill_of_lading") and
                    json_data.get("containers") and
                    all(cont.get("container") and cont.get("seals") for cont in json_data["containers"])
            ):
                logger.print(f"В json файле отсутствует одно из обязательных полей {json_file}")
                metadata["errors"].append(f"{source_file_name}: ошибка распознавания.")
                transfer_files(
                    file_paths=(source_file, json_file),
                    destination_folder=error_folder,
                    operation="move"
                )
                continue

            transaction_number_raw = cup_http_request(
                "TransactionNumberFromBillOfLading",
                json_data["bill_of_lading"]
            )
            if not (transaction_number_raw and isinstance(transaction_number_raw, list)):
                logger.print(
                    f"Для коносамента {json_data['bill_of_lading']} не удалось получить "
                    f"номер транзакции из ЦУП: {json_file}"
                )
                metadata["errors"].append(
                    f"{source_file_name}: ошибка распознавания. "
                    f"Не удалось получить номер транзакции из ЦУП"
                )
                transfer_files(
                    file_paths=(source_file, json_file),
                    destination_folder=error_folder,
                    operation="move"
                )
                continue

            json_data["transaction_number"] = transaction_number_raw[0]
            json_data["source_file_base64"] = file_to_base64(source_file)
            json_data["source_file_ext"] = source_file.suffix
            logger.print(f"TransactionNumber: {json_data['transaction_number']}")
            logger.print(f"Файл успешно обработан: {source_file}")
            write_json(json_file, json_data)
            transfer_files(
                file_paths=(source_file, json_file),
                destination_folder=success_folder,
                operation="move"
            )
            success_flag = True

        write_json(folder / "metadata.json", metadata)

        if metadata["errors"]:
            transfer_files(folder / "metadata.json", error_folder, "copy2")

            error_files_text = "\n".join(
                f"{i}. {error_file}"
                for i, error_file in enumerate(metadata["errors"], 1)
            )
            email_text = (
                f"В сообщении от {metadata['date']} среди прикрепленных файлов "
                f"следующие не удалось распознать:\n"
                f"{error_files_text}\n\n"
                f"Копии самих файлов можно найти по ссылке: {error_folder}"
            )
            print(email_text)
            send_email(
                email_text=email_text,
                recipient_email=metadata["sender"],
                subject=f"Автоответ от {CONFIG.EMAIL_ADDRESS}",
                email_user=CONFIG.EMAIL_ADDRESS,
                email_pass=CONFIG.EMAIL_PASSWORD,
            )

        if success_flag:
            transfer_files(folder / "metadata.json", success_folder, "move")
