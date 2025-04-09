import time
from pathlib import Path

from config import CONFIG
from src.logger import logger
from src.utils import read_json, write_json
from src.process_email_inbox import process_email_inbox
from src.utils_1c import cup_http_request


def main():
    logger.info("\n" + CONFIG.display_config())

    while True:
        # Проверка наличия новых сообщений в почтовом ящике.
        # Извлечение вложений и метаданных из каждого нового сообщения
        # process_email_inbox(
        #     email_user=CONFIG.EMAIL_ADDRESS,
        #     email_pass=CONFIG.EMAIL_PASSWORD,
        #     imap_server=CONFIG.imap_server,
        #     imap_port=CONFIG.imap_port,
        # )

        # Проверка наличия директорий с файлами в EXPORT
        # Получение списка папок с проверкой наличия metadata.json
        folders_for_processing = [
            folder for folder in CONFIG.EXPORT_FOLDER.iterdir()
            if folder.is_dir() and (folder / "metadata.json").exists()
        ]

        if folders_for_processing:
            logger.print(f"Обнаружено директорий для обработки: {len(folders_for_processing)}")

        for folder in folders_for_processing:
            metadata = read_json(folder / "metadata.json")

            files = [file for file in folder.glob("*") if file.suffix in CONFIG.valid_ext]

            for file in files:
                if file.name not in metadata["files"]:
                    logger.print(f"Файл отсутствует в списке в метаданных: {file}")
                    continue

                data_name = f"{file.stem}({file.suffix[1:]}).json"
                data_file = folder / data_name
                data = read_json(data_file)

                bill_of_lading = data["bill_of_lading"]
                transaction_number = cup_http_request("TransactionNumberFromBillOfLading", bill_of_lading)
                logger.print(f"TransactionNumber: {transaction_number}")

        time.sleep(5)


if __name__ == "__main__":
    main()
