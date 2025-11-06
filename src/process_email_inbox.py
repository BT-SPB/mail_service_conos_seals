import ssl
import time
import email
import socket
import logging
from pathlib import Path
from typing import Callable
from email.message import Message
from email.utils import parseaddr

from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientAbortError, IMAPClientError

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


def process_unseen_inbox_messages(server: IMAPClient) -> None:
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
    Мониторит входящую почту через IMAP (IMAPClient) с использованием механизма IDLE
    и периодической принудительной проверки.

    Особенности:
        - Использует IDLE для мгновенных уведомлений о новых письмах.
        - Безопасно обрабатывает обрывы SSL/IMAP-сессий и переподключается.
        - Выполняет регулярные forced check и периодический reconnect.
        - Безопасно выполняет callback и защищает основной цикл от падений.
    """

    def __init__(
            self,
            email_user: str = config.email_address,
            email_pass: str = config.email_password,
            imap_server: str = config.imap_server,
            imap_port: int = config.imap_port,
            idle_timeout: int = 10,
            forced_timeout: int = 25,
            reconnect_timeout: int = 86400,  # 24ч
            callback: Callable[[IMAPClient], None] | None = process_unseen_inbox_messages
    ) -> None:
        """
        Инициализирует мониторинг с параметрами IMAP-соединения.

        Args:
            email_user: Логин почтового аккаунта.
            email_pass: Пароль почтового аккаунта.
            imap_server: Хост IMAP сервера.
            imap_port: Порт IMAP сервера.
            idle_timeout: Таймаут для idle_check (сек).
            forced_timeout: Периодическая принудительная проверка (сек).
            reconnect_timeout: Периодическое (регулярное) переподключение с целью избежать долгоживущих сессий (сек).
                               Если установить <= 0, периодический reconnect отключён.
            callback: Функция для обработки входящих писем. Вызывается с аргументом server (IMAPClient).

        Returns:
            None
        """
        # Валидация параметров на базовом уровне
        if idle_timeout <= 0:
            raise ValueError("idle_timeout must be > 0")
        if forced_timeout <= 0:
            raise ValueError("forced_timeout must be > 0")
        # reconnect_timeout может быть <=0 чтобы отключить авто-reconnect

        self.email_user = email_user
        self.email_pass = email_pass
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.idle_timeout = int(idle_timeout)
        self.forced_timeout = int(forced_timeout)
        self.reconnect_timeout = int(reconnect_timeout)
        self.callback = callback or (lambda server: None)

        # Инициализация состояния мониторинга
        self.running: bool = False
        self.server: IMAPClient | None = None

        # Устанавливаем в 0, чтобы первый forced check сработал мгновенно
        self.last_check: float = 0.0
        self.last_reconnect: float | None = None

    # ------------------------------------------------------------------
    # Подключение / Отключение / Переподключение / Остановка
    # ------------------------------------------------------------------
    def connect(self):
        """
        Устанавливает SSL-подключение к IMAP (IMAPClient), логинится и выбирает INBOX.

        Безопасно перезапускает текущее соединение (если есть) перед созданием нового.
        """
        # Перед новым подключением закрываем старое
        self.disconnect()

        try:
            self.server = IMAPClient(
                host=self.imap_server,
                port=self.imap_port,
                ssl=True,
                ssl_context=ssl.create_default_context(),
                timeout=40.0,
            )

            self.server.login(self.email_user, self.email_pass)
            self.server.select_folder("INBOX")  # Выбираем папку INBOX для обработки входящих писем
            self.last_reconnect = time.monotonic()
            logger.info("📡 Подключено к IMAP-серверу %s:%s", self.imap_server, self.imap_port)
        except Exception as e:
            # Не падаем, а сбрасываем сервер и логируем
            logger.exception("⛔ Ошибка подключения к IMAP-серверу: %s", e)
            self.server = None

    def disconnect(self) -> None:
        """
        Безопасно закрывает сессию IMAP: завершает IDLE (если нужно) и выполняет logout.
        Любые исключения в процессе аккуратно логируются и игнорируются.
        """
        if not self.server:
            return

        try:
            # Попытаться корректно завершить IDLE, но игнорировать специфичные ошибки,
            # которые свидетельствуют о том, что соединение уже разорвано.
            try:
                self.server.idle_done()
            except (IMAPClientAbortError, ssl.SSLEOFError, OSError, socket.error) as e:
                # Обычная ситуация при разрыве — логировать на DEBUG (не ошибку)
                logger.debug(
                    "⚠️ Не удалось корректно завершить IDLE (возможно соединение уже закрыто): %s",
                    e
                )
            except Exception as e:
                # Другие исключения — логируем как debug, но продолжаем закрывать соединение
                logger.debug("⚠️ Ошибка при idle_done: %s", e)

            try:
                self.server.logout()
            except (IMAPClientAbortError, ssl.SSLEOFError, OSError) as e:
                # На Windows/Windows OpenSSL часто возникает SSLEOFError при уже разорванном соединении.
                logger.debug("⚠️ Ошибка logout (соединение, вероятно, уже разорвано): %s", e)
            except Exception as e:
                logger.exception("⛔ Непредвиденная ошибка при logout: %s", e)
        finally:
            self.server = None
            logger.info("🔔 Соединение IMAP закрыто")

    def reconnect(self, delay: float = 0.0) -> None:
        """
        Переподключается к IMAP: закрывает текущее соединение и через delay секунд
        предпринимает попытку нового подключения (если монитор всё ещё запущен).

        Args:
            delay: Задержка перед попыткой переподключения (сек).
        """
        logger.debug("🔄 Переподключение к IMAP (пауза %ss)", delay)
        self.disconnect()

        if not self.running:
            logger.debug("🔔 Переподключение отменено — монитор остановлен")
            return

        if delay > 0: time.sleep(delay)
        self.connect()

    def stop(self) -> None:
        """
        Останавливает мониторинг: устанавливает флаг running=False и закрывает соединение.
        """
        if not self.running:
            logger.debug("🔔 Мониторинг уже остановлен — stop() вызван повторно")
            return

        self.running = False
        self.disconnect()
        logger.info("🔔 Мониторинг почты остановлен")

    # ------------------------------------------------------------------
    # Callback
    # ------------------------------------------------------------------
    def execute_callback_safe(self) -> None:
        """
        Безопасно выполняет callback для обработки писем и обновляет время последней проверки.

        Функция защищает основной цикл мониторинга от сбоев внутри callback
        (например, при ошибках обработки писем). Если callback завершается успешно,
        обновляется таймер self.last_check.
        """
        try:
            self.callback(self.server)
        except Exception as e:
            # Логируем ошибку, но не прерываем основной мониторинг
            logger.exception("⛔ Ошибка при выполнении callback: %s", e)
        finally:
            # Всегда обновляем время последней попытки (даже если callback упал)
            self.last_check = time.monotonic()

    # ------------------------------------------------------------------
    # Основной мониторинг
    # ------------------------------------------------------------------
    def monitor(self) -> None:
        """
        Запускает цикл мониторинга: комбинирует IMAP IDLE (быстрая реакция на новые письма)
        и периодическую принудительную проверку (forced check) для надёжности.

        Алгоритм (упрощённо):
        - Подключаемся.
        - В цикле:
            * При необходимости выполняем принудительную проверку callback.
            * Если включён reconnect_timeout и он истёк — переподключаемся.
            * Если есть активное соединение — заходим в IDLE и ждём уведомлений.
            * При получении уведомлений — вызываем callback.
            * Любые сетевые/SSL/imaplib ошибки приводят к безопасному переподключению.
        """
        self.running = True

        logger.info(
            "🔄 Старт мониторинга (idle=%ss, forced=%ss, reconnect=%ss)",
            self.idle_timeout,
            self.forced_timeout,
            self.reconnect_timeout,
        )

        try:
            while self.running:
                try:
                    # Если нет активного соединения — переподключаемся
                    if not self.server:
                        logger.debug("⚠️ Нет активного IMAP-клиента, пытаемся подключиться...")
                        self.connect()
                        # Если предыдущий цикл вызвал ошибку — даём небольшую паузу, чтобы не спамить попытками
                        if not self.server:
                            time.sleep(10.0)
                            continue  # вернуться в цикл и попытаться снова

                    # Периодический (профилактический) перепуск сессии — опционально
                    if (
                            self.reconnect_timeout > 0 and
                            (time.monotonic() - (self.last_reconnect or 0) >= self.reconnect_timeout)
                    ):
                        logger.debug("🔄 Выполняем периодический перезапуск сессии (reconnect timeout)")
                        self.reconnect()
                        continue

                    # Принудительная проверка по таймеру (forced_timeout)
                    if time.monotonic() - self.last_check >= self.forced_timeout:
                        logger.debug("🕒 Выполнение принудительной проверки (forced check)")
                        self.execute_callback_safe()
                        continue

                    # Перед входом в idle убеждаемся, что server всё ещё доступен
                    if not self.server:
                        continue

                    # Запуск IDLE (сервер будет отправлять уведомления)
                    self.server.idle()
                    # Ожидание уведомлений; вернёт [] или список уведомлений
                    responses = self.server.idle_check(timeout=self.idle_timeout)
                    logger.debug("responses: %s", responses)

                    # Попытка корректно выйти из IDLE — это место, где часто возникают ошибки типа EOF
                    try:
                        self.server.idle_done()
                    except (IMAPClientAbortError, ssl.SSLEOFError, OSError, socket.error) as e:
                        # Сервер разорвал соединение — это ожидаемая ситуация; инициируем reconnect
                        logger.debug("⚠️ Ошибка при idle_done (соединение разорвано): %s", e)
                        # Принудительно сбрасываем server — disconnect() дополнительно почистит
                        # и закроет низкоуровневые ресурсы.
                        # Попробуем переподключиться с небольшой паузой
                        self.reconnect(5.0)
                        continue
                    except Exception as e:
                        # Непредвиденные ошибки — логируем и переподключаемся
                        logger.exception("⛔ Непредвиденная ошибка при idle_done: %s", e)
                        self.reconnect(5.0)
                        continue
                    finally:
                        # Если получены события от сервера — выполняем callback
                        if responses:
                            logger.info("🔔 IDLE уведомления: %s", responses)
                            self.execute_callback_safe()

                except Exception as e:
                    logger.exception(f"⛔ Ошибка в IDLE-цикле: {e}")
                    self.reconnect(5.0)
                    continue

                finally:
                    # Если флаг self.running установлен в False - останавливаем цикл
                    if not self.running:
                        break

        except Exception as e:
            logger.exception(f"⛔ Критическая ошибка мониторинга: {e}")
        finally:
            if self.running: self.stop()
