import time
from watchdog.observers import Observer

from threading import Thread
import signal

from config import CONFIG
from src.logger import logger
from src.process_email_inbox import process_email_inbox_simple, EmailMonitor
from src.process_output_ocr import process_output_ocr, FolderWatcher


def main_simple():
    """
    Основная функция программы для обработки входящих email и файлов после OCR.
    """
    logger.info("\n" + CONFIG.display_config())

    while True:
        # ЭТАП 1:
        # Проверка наличия новых сообщений в почтовом ящике.
        # Извлечение вложений и метаданных из каждого нового сообщения
        process_email_inbox_simple(
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

def main_test():
    """
    Основная функция для мониторинга директории и обработки файлов после OCR.

    Запускает наблюдение за папкой CONFIG.OUT_OCR_FOLDER и вызывает функцию обработки
    OCR-результатов при изменениях или каждые 2 минуты, если событий нет.
    """
    logger.info("\n" + CONFIG.display_config())

    # Создаем наблюдатель с передачей callback-функции для OCR-обработки
    watcher = FolderWatcher(
        folder_path=CONFIG.OUT_OCR_FOLDER,
        callback=lambda: process_output_ocr(
            email_user=CONFIG.EMAIL_ADDRESS,
            email_pass=CONFIG.EMAIL_PASSWORD,
            smtp_server=CONFIG.smtp_server,
            smtp_port=CONFIG.smtp_port,
        )
    )
    watcher.monitor()


def main():
    """
    Запускает мониторинг электронной почты.

    Создает экземпляр EmailMonitor с параметрами из конфигурации и запускает мониторинг.
    Логирует конфигурацию перед началом работы.
    """
    logger.info("\n" + CONFIG.display_config())

    # Мониторинг почты
    email_monitor = EmailMonitor(
        email_user=CONFIG.EMAIL_ADDRESS,
        email_pass=CONFIG.EMAIL_PASSWORD,
        imap_server=CONFIG.imap_server,
        imap_port=CONFIG.imap_port
    )

    try:
        email_monitor.monitor()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, останавливаем мониторинг")
        email_monitor.stop()


if __name__ == "__main__":
    main_test()
