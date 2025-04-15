import time
from threading import Thread

from config import CONFIG
from src.logger import logger
from src.process_email_inbox import process_email_inbox_simple, EmailMonitor
from src.process_output_ocr import process_output_ocr
from src.folder_watcher import FolderWatcher


def main() -> None:
    """
    Основная функция приложения для совместного мониторинга почты и папок.

    Инициализирует и запускает EmailMonitor и FolderWatcher в отдельных потоках,
    обеспечивая параллельную работу. Логирует конфигурацию и управляет завершением
    процессов при получении сигнала прерывания.

    Raises:
        KeyboardInterrupt: При прерывании пользователем (Ctrl+C).
    """
    # Логируем конфигурацию перед началом работы.
    logger.info("\n" + CONFIG.display_config())

    # Инициализация EmailMonitor
    email_monitor = EmailMonitor(
        email_user=CONFIG.EMAIL_ADDRESS,
        email_pass=CONFIG.EMAIL_PASSWORD,
        imap_server=CONFIG.imap_server,
        imap_port=CONFIG.imap_port
    )

    # Инициализация FolderWatcher с callback для обработки OCR-результатов
    folder_watcher = FolderWatcher(
        folder_path=CONFIG.OUT_OCR_FOLDER,
        callback=lambda: process_output_ocr(
            email_user=CONFIG.EMAIL_ADDRESS,
            email_pass=CONFIG.EMAIL_PASSWORD,
            smtp_server=CONFIG.smtp_server,
            smtp_port=CONFIG.smtp_port,
        )
    )

    # Создаем потоки для каждого монитора
    email_thread = Thread(target=email_monitor.monitor, name="EmailMonitor")
    folder_thread = Thread(target=folder_watcher.monitor, name="FolderWatcher")

    # Устанавливаем потоки как демоны, чтобы они автоматически завершались при выходе основного процесса
    email_thread.daemon = True
    folder_thread.daemon = True

    try:
        # Запускаем потоки
        email_thread.start()
        folder_thread.start()
        logger.info("Запущены отдельные процессы для мониторинга почты и директории OUT_OCR")

        # Основной поток ожидает завершения (например, через Ctrl+C)
        while True:
            time.sleep(1)  # Небольшая пауза для снижения нагрузки на CPU
            # Проверяем, что потоки живы (для отладки или дополнительных действий)
            if not email_thread.is_alive():
                logger.error("Поток мониторинга почты завершился неожиданно")
                break
            if not folder_thread.is_alive():
                logger.error("Поток мониторинга папок завершился неожиданно")
                break

    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, останавливаем приложение")
        # Устанавливаем флаг остановки для EmailMonitor
        email_monitor.stop()
        # FolderWatcher останавливается через внутреннюю обработку KeyboardInterrupt.
        # Ждем завершения потоков (демоны завершатся автоматически, но ждем для чистоты)
        email_thread.join(timeout=5.0)
        folder_thread.join(timeout=5.0)
        logger.info("Приложение завершено корректно")


def main_fallback() -> None:
    """Запускает упрощенный мониторинг почты и папок в аварийном режиме.

    Функция выполняет последовательную обработку входящих писем и файлов после OCR
    в бесконечном цикле с заданной паузой между итерациями. Используется как запасной
    вариант при сбоях основного механизма мониторинга (EmailMonitor и FolderWatcher).
    Каждый этап (проверка почты и обработка файлов) выполняется с обработкой ошибок,
    чтобы исключения не прерывали цикл.

    Логирует конфигурацию при старте, начало каждого этапа и ошибки, если они возникают.
    Поддерживает корректное завершение при прерывании пользователем (Ctrl+C).

    Returns:
        NoReturn: Функция работает бесконечно и не возвращает управление,
            за исключением случаев прерывания.

    Raises:
        KeyboardInterrupt: При прерывании пользователем (Ctrl+C), что приводит
            к корректному завершению.
    """
    # Логируем конфигурацию приложения для отладки и контроля
    logger.info("\n" + CONFIG.display_config())
    logger.info("Запущен упрощенный режим мониторинга (fallback)")

    try:
        while True:
            try:
                # ЭТАП 1:
                # Проверка наличия новых сообщений в почтовом ящике.
                # Извлечение вложений и метаданных из каждого нового сообщения
                process_email_inbox_simple(
                    email_user=CONFIG.EMAIL_ADDRESS,
                    email_pass=CONFIG.EMAIL_PASSWORD,
                    imap_server=CONFIG.imap_server,
                    imap_port=CONFIG.imap_port,
                )

                # ЭТАП 2:
                # Обработка результатов после OCR
                # Проверка наличия директорий с файлами в OUT_OCR
                process_output_ocr(
                    email_user=CONFIG.EMAIL_ADDRESS,
                    email_pass=CONFIG.EMAIL_PASSWORD,
                    smtp_server=CONFIG.smtp_server,
                    smtp_port=CONFIG.smtp_port,
                )

            except Exception as e:
                # Логируем ошибку, но продолжаем цикл, чтобы сохранить устойчивость
                logger.error(f"Ошибка в упрощенном режиме: {type(e).__name__}: {e}")

            # Пауза между итерациями
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Упрощенный режим мониторинга остановлен пользователем")
        raise  # Вызываем исключение для выхода


# --- Отдельные функции для изолированного запуска каждого модуля ---

def test_email_monitor() -> None:
    """Запускает изолированный мониторинг электронной почты для тестирования и отладки.

    Инициализирует экземпляр EmailMonitor с параметрами из конфигурации и запускает
    мониторинг входящих писем через IMAP. Логирует конфигурацию и ключевые события
    (запуск, ошибки, завершение). Предназначена для проверки работы почтового модуля
    независимо от мониторинга папок. Обрабатывает ошибки соединения и прерывания
    пользователем (Ctrl+C) для безопасного завершения.

    Returns:
        NoReturn: Функция работает бесконечно и не возвращает управление,
            за исключением случаев прерывания или критических ошибок.

    Raises:
        KeyboardInterrupt: При прерывании пользователем (Ctrl+C), что приводит
            к корректному завершению.
        Exception: При непредвиденных ошибках (например, сбой IMAP-соединения),
            логируется с последующим завершением.
    """
    # Логируем конфигурацию для отладки и контроля параметров
    logger.info("\n" + CONFIG.display_config())
    logger.info("Запущен изолированный мониторинг почты (тестовый режим)")

    # Инициализируем EmailMonitor
    email_monitor = EmailMonitor(
        email_user=CONFIG.EMAIL_ADDRESS,
        email_pass=CONFIG.EMAIL_PASSWORD,
        imap_server=CONFIG.imap_server,
        imap_port=CONFIG.imap_port
    )

    try:
        # Запускаем мониторинг почты
        email_monitor.monitor()
    except KeyboardInterrupt:
        # Обрабатываем прерывание пользователем (Ctrl+C)
        logger.info("Мониторинг почты остановлен пользователем")
        email_monitor.stop()
    except Exception as e:
        # Логируем непредвиденные ошибки и завершаем работу
        logger.error(f"Критическая ошибка в мониторинге почты: {type(e).__name__}: {e}")
        email_monitor.stop()
        raise  # Повторно вызываем исключение для уведомления вызывающего кода


def test_folder_monitor() -> None:
    """Запускает изолированный мониторинг папок для тестирования и отладки.

    Инициализирует экземпляр FolderWatcher для отслеживания изменений в папке
    CONFIG.OUT_OCR_FOLDER и вызывает callback-функцию process_output_ocr при событиях
    или периодически. Логирует конфигурацию и ключевые события (запуск, ошибки, завершение).
    Предназначена для проверки работы модуля мониторинга папок независимо от мониторинга
    почты. Обрабатывает ошибки наблюдателя и прерывания пользователем (Ctrl+C) для
    безопасного завершения.

    Returns:
        NoReturn: Функция работает бесконечно и не возвращает управление,
            за исключением случаев прерывания или критических ошибок.

    Raises:
        KeyboardInterrupt: При прерывании пользователем (Ctrl+C), что приводит
            к корректному завершению.
        Exception: При непредвиденных ошибках (например, сбой watchdog),
            логируется с последующим завершением.
    """
    # Логируем конфигурацию для отладки и контроля параметров
    logger.info("\n" + CONFIG.display_config())
    logger.info("Запущен изолированный мониторинг папок (тестовый режим)")

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

    try:
        # Запускаем мониторинг папки
        watcher.monitor()
    except KeyboardInterrupt:
        # Обрабатываем прерывание пользователем (Ctrl+C)
        # FolderWatcher сам останавливается в monitor(), но логируем для ясности
        logger.info("Мониторинг папок остановлен пользователем")
    except Exception as e:
        # Логируем непредвиденные ошибки и завершаем работу
        logger.error(f"Критическая ошибка в мониторинге папок: {type(e).__name__}: {e}")
        raise  # Повторно вызываем исключение для уведомления вызывающего кода


if __name__ == "__main__":
    main()  # Основной режим (оба монитора в параллельных потоках)
    # main_fallback()  # Аварийный режим (последовательный запуск двух упрощенных модулей в бесконечном цикле)
    # test_email_monitor()  # Тест только почты
    # test_folder_monitor()  # Тест только папок
