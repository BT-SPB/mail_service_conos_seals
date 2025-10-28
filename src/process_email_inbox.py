import email
# import imaplib
import time
import logging
from pathlib import Path
from email.message import Message
from email.utils import parseaddr
import ssl

from imapclient import IMAPClient

from config import config
from src.utils import sanitize_pathname
from src.utils_email import (
    convert_email_date_to_moscow,
    decode_subject,
    extract_text_content,
    extract_attachments,
)
from src.models.metadata_model import StructuredMetadata

logger = logging.getLogger(__name__)


def process_unseen_email_inbox(server: IMAPClient) -> None:
    """
    Обрабатывает непрочитанные письма в папке INBOX.

    Ищет непрочитанные письма, извлекает их метаданные и вложения, сохраняет вложения в
    уникальную папку, создает файл метаданных metadata.json и отмечает письма
    как прочитанные после обработки.

    Использует peek для получения писем без автоматической отметки как прочитанных,
    что позволяет контролировать этот процесс вручную. Пропускает письма с ошибками,
    логируя их, чтобы продолжить обработку остальных.


    Returns:
        None
    """
    try:
        # Поиск непрочитанных писем
        message_ids = server.search(["UNSEEN"])
        if not message_ids:
            logger.debug("➖ Новых писем нет")
            return

        logger.info(f"📧 Обнаружено непрочитанных писем: {len(message_ids)}")

        # Последовательная обработка каждого письма
        for msg_id in message_ids:
            try:
                # Получаем данные письма без изменения статуса (BODY.PEEK)
                msg_data = server.fetch(msg_id, ["BODY.PEEK[]"])
                if not msg_data or msg_id not in msg_data:
                    logger.error(f"❌ Не удалось получить данные письма (id {msg_id})")
                    continue

                # Парсим письмо в объект Message для удобной работы с содержимым
                email_message: Message = email.message_from_bytes(msg_data[msg_id][b"BODY[]"])

                # Собираем метаданные письма
                metadata = StructuredMetadata(
                    subject=decode_subject(email_message.get("Subject", "")),
                    sender=parseaddr(email_message.get("From", ""))[1],
                    date=email_message.get("Date", "Unknown date"),
                    text_content=extract_text_content(email_message) or "No text content",
                )

                # Извлечение и обработка вложений
                attachments: list[tuple[str, bytes]] = extract_attachments(email_message)

                if not attachments:
                    logger.info(f"📧 Письмо от {metadata.sender} (id {msg_id}) не содержит вложений")
                    # Отметка письма как прочитанного
                    server.add_flags(msg_id, ["\\Seen"])
                    continue

                # Обработка вложений при их наличии
                logger.info(
                    f"📧 В письме от {metadata.sender} (id {msg_id}) найдено вложений: {len(attachments)}"
                )

                # Формирование уникального имени папки на основе даты и времени отправки письма
                date_time = convert_email_date_to_moscow(metadata.date, "%y%m%d_%H%M%S")
                save_dir = sanitize_pathname(
                    config.INPUT_DIR,
                    f"{date_time}_{metadata.sender}",
                    is_file=False
                )

                # Создание директории
                save_dir.mkdir(exist_ok=True, parents=True)
                logger.debug(f"✔️ Создана директория: {save_dir}")

                # Последовательная обработка каждого вложения
                for file_name, content in attachments:
                    file_ext = Path(file_name).suffix.lower()
                    if file_ext not in config.valid_ext:
                        valid_ext_text = ", ".join(f"'*{ext}'" for ext in config.valid_ext)
                        warning_message = (
                            f"Неподдерживаемое расширение. Допустимые: {valid_ext_text}."
                        )
                        metadata.errors[file_name].add(warning_message)
                        logger.warning(f"❌ {warning_message}")
                        continue

                    # Создание безопасного имени файла
                    file_path = sanitize_pathname(save_dir, file_name, is_file=True)

                    try:
                        # Сохраняем файл
                        file_path.write_bytes(content)
                        # Записываем в метаданные имя исходного файла
                        metadata.files.append(f"{file_path.name}")
                        logger.info(f"💾 Файл сохранен: {file_path}")
                    except OSError as e:
                        logger.error(f"⛔ Ошибка при сохранении файла {file_path}: {e}")

                # Сохранение метаданных
                metadata_path = save_dir / "metadata.json"
                metadata.save(metadata_path)
                logger.info(f"💾 Сохранены метаданные: {metadata_path}")

                # Отмечаем письмо как прочитанное после успешной обработки
                server.add_flags(msg_id, ["\\Seen"])
                logger.info(f"✔️ Письмо (id {msg_id}) обработано и отмечено как прочитанное")

            except Exception as e:
                logger.exception(f"⛔ Ошибка обработки письма (id {msg_id}): {e}")

    except Exception as e:
        logger.exception(f"⛔ Произошла ошибка при обработке писем: {e}")


class EmailMonitor:
    """
    Мониторит новые письма с использованием IMAP IDLE и периодической проверки.

    Особенности:
    - Использует IDLE для мгновенных уведомлений.
    - Автоматически переподключается при обрыве SSL/IMAP-сессии.
    - Регулярно перезапускает IDLE (чтобы избежать таймаутов сервера).
    - Делает принудительную проверку каждые forced_timeout секунд.
    """

    def __init__(
            self,
            email_user: str = config.email_address,
            email_pass: str = config.email_password,
            imap_server: str = config.imap_server,
            imap_port: int = config.imap_port,
            idle_timeout: int = 10,
            forced_timeout: int = 25,
            reconnect_timeout: int = 1500,
    ) -> None:
        """
        Инициализирует мониторинг с параметрами IMAP-соединения.

        Args:
            email_user: Логин почты
            email_pass: Пароль
            imap_server: IMAP-сервер
            imap_port: Порт
            idle_timeout: Время ожидания внутри IDLE (сек)
            forced_timeout: Период полной проверки писем (сек)
            reconnect_timeout: Макс. длительность одной IDLE-сессии (сек)
        """
        self.email_user = email_user
        self.email_pass = email_pass
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.idle_timeout = idle_timeout
        self.forced_timeout = forced_timeout
        self.reconnect_timeout = reconnect_timeout

        # Инициализация состояния мониторинга
        self.running: bool = False
        self.server: IMAPClient | None = None

    # --- Подключение ---
    def connect(self):
        """
        Устанавливает защищенное соединение с IMAP-сервером.

        Создает новый клиент IMAPClient с SSL, выполняет вход с использованием учетных данных
        и выбирает папку INBOX для мониторинга.
        """
        # Если соединение уже установлено — закрываем его перед повторным подключением
        self.disconnect()

        try:
            self.server = IMAPClient(
                host=self.imap_server,
                port=self.imap_port,
                ssl=True,
                ssl_context=ssl.create_default_context()
            )

            self.server.login(self.email_user, self.email_pass)
            self.server.select_folder("INBOX")  # Выбираем папку INBOX для обработки входящих писем
            logger.info("📡 Подключено к IMAP-серверу")
        except Exception as e:
            logger.exception(f"⛔ Ошибка подключения к IMAP-серверу: {e}")
            self.server = None

    def disconnect(self) -> None:
        """Закрывает соединение с IMAP (безопасно)."""
        if not self.server:
            return

        try:
            self.server.idle_done()
        except Exception as e:
            logger.debug(f"⚠️ Неудачное завершение IDLE: {e}")

        try:
            self.server.logout()
            logger.info("🔔 Соединение закрыто")
        except Exception as e:
            logger.exception(f"⛔ Ошибка при logout: {e}")
        finally:
            self.server = None

    def reconnect(self, timeout: int = 0) -> None:
        """
        Переподключение к серверу IMAP.

        Args:
            timeout: Пауза перед переподключением (сек). По умолчанию 0.
        """
        logger.debug(f"🔄 Переподключение к IMAP-серверу (пауза {timeout}s)")
        self.disconnect()
        if self.running:
            if timeout > 0: time.sleep(timeout)
            self.connect()

    def stop(self) -> None:
        """
        Завершает мониторинг, закрывает соединение и сбрасывает флаг активности.
        """
        if not self.running:
            logger.debug("🔔 Мониторинг почты УЖЕ остановлен")
            return

        self.running = False
        self.disconnect()
        logger.info("🔔 Мониторинг почты остановлен")

    def monitor(self) -> None:
        """
        Запускает постоянный мониторинг почты, используя режим IMAP IDLE и периодическую проверку.

        Алгоритм:
        1. Подключается к IMAP-серверу.
        2. Выполняет принудительную проверку писем каждые forced_timeout секунд.
        3. Слушает события через IDLE и проверяет письма при поступлении уведомлений.
        4. Периодически перезапускает сессию по reconnect_timeout.
        5. При любых ошибках выполняет безопасное переподключение и продолжает мониторинг.

        Работает до вызова stop().
        """
        self.running = True
        last_check = 0
        last_reconnect = time.time()

        try:
            self.connect()
            logger.info(
                f"🔄 Старт мониторинга (idle={self.idle_timeout}s, "
                f"forced={self.forced_timeout}s, reconnect={self.reconnect_timeout}s)"
            )

            while self.running:
                try:
                    # Принудительная проверка каждые forced_timeout
                    if time.time() - last_check >= self.forced_timeout:
                        logger.debug("🕒 Принудительная проверка email по таймеру")
                        # Выполняем проверку
                        process_unseen_email_inbox(self.server)
                        last_check = time.time()

                    # Перезапуск сессии по истечении цикла каждые reconnect_timeout
                    if time.time() - last_reconnect >= self.reconnect_timeout:
                        logger.debug("🔄 Перезапуск сессии")
                        self.reconnect()
                        last_reconnect = time.time()
                        continue

                    # Входим в режим IDLE — ожидание новых писем от сервера
                    self.server.idle()
                    responses = self.server.idle_check(timeout=self.idle_timeout)
                    logger.debug("responses: %s", responses)

                    try:
                        self.server.idle_done()
                    except ssl.SSLEOFError:
                        logger.debug("⚠️ Сервер закрыл SSL-соединение (SSLEOFError)")
                        raise
                    except Exception as e:
                        logger.debug(f"⚠️ Ошибка при завершении IDLE: {e}")
                        raise
                    finally:
                        # Если есть новые события, инициируем повторную проверку
                        if responses:
                            logger.info(f"🔔 IDLE уведомления: {responses}")
                            process_unseen_email_inbox(self.server)
                            last_check = time.time()

                except Exception as e:
                    if not self.running:
                        # Прерывание в момент остановки — не логируем
                        break

                    logger.exception(f"⛔ Ошибка в IDLE-цикле: {e}")
                    self.reconnect(timeout=5)
                    if self.running and self.server:
                        logger.info("🔄 Соединение восстановлено после ошибки")
                        process_unseen_email_inbox(self.server)  # После переподключения сразу выполняем проверку
                        last_check = time.time()

        except Exception as e:
            logger.exception(f"⛔ Критическая ошибка мониторинга: {e}")
        finally:
            self.disconnect()
            logger.info("🔔 Мониторинг завершён")
