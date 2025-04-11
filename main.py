import time
from threading import Thread
import signal

from config import CONFIG
from src.logger import logger
from src.process_email_inbox import process_email_inbox, EmailMonitor
from src.process_output_ocr import process_output_ocr


def main_simple():
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

        time.sleep(1)


def main():
    """Запускает мониторинг почты и папки OUT_OCR в отдельных потоках."""
    logger.info("\n" + CONFIG.display_config())

    # Мониторинг почты
    email_monitor = EmailMonitor(
        email_user=CONFIG.EMAIL_ADDRESS,
        email_pass=CONFIG.EMAIL_PASSWORD,
        imap_server=CONFIG.imap_server,
        imap_port=CONFIG.imap_port,
        callback=lambda: process_email_inbox(
            email_user=CONFIG.EMAIL_ADDRESS,
            email_pass=CONFIG.EMAIL_PASSWORD,
            imap_server=CONFIG.imap_server,
            imap_port=CONFIG.imap_port,
        ),
    )

    # Запуск мониторинга почты
    email_thread = Thread(target=email_monitor.monitor)
    email_thread.start()
    logger.info("Начато наблюдение за почтой")

    def shutdown():
        """Останавливает мониторинг при завершении программы."""
        logger.info("Завершение работы...")
        email_monitor.stop()
        # observer.stop()
        # observer.join()
        email_thread.join()
        logger.info("Программа завершена")

    # Обработка сигналов для graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: shutdown())

    try:
        email_thread.join()
        # observer.join()
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main_simple()
