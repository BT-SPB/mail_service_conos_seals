import time

from config import CONFIG
from src.logger import logger
from src.process_email_inbox import process_email_inbox
from src.process_output_ocr import process_output_ocr


def main():
    """
    Основная функция программы для обработки входящих email и файлов после OCR.

    Args:
        config: Объект конфигурации с настройками путей и доступов
        logger: Объект логгера для записи сообщений
    """
    logger.info("\n" + CONFIG.display_config())

    while True:
        # ЭТАП 1:
        # Проверка наличия новых сообщений в почтовом ящике.
        # Извлечение вложений и метаданных из каждого нового сообщения
        process_email_inbox(
            email_user=CONFIG.EMAIL_ADDRESS,
            email_pass=CONFIG.EMAIL_PASSWORD,
            imap_server=CONFIG.imap_server,
            imap_port=CONFIG.imap_port,
        )

        # ЭТАП 2 - ОБРАОТКА ДАННЫХ ПОСЛЕ OCR (можно выделить в отдельную функцию)
        # Проверка наличия директорий с файлами в OUT_OCR_FOLDER
        # Получение списка папок с проверкой наличия metadata.json
        process_output_ocr(
            email_user=CONFIG.EMAIL_ADDRESS,
            email_pass=CONFIG.EMAIL_PASSWORD,
            smtp_server=CONFIG.smtp_server,
            smtp_port=CONFIG.smtp_port,
        )

        time.sleep(5)


if __name__ == "__main__":
    main()
