import email
import imaplib
import time
import traceback
from pathlib import Path
from datetime import datetime
from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime

from imapclient import IMAPClient

from config import CONFIG
from src.logger import logger
from src.utils import write_json, sanitize_pathname
from src.utils_email import (
    decode_subject,
    extract_text_content,
    extract_attachments,
)


class EmailMonitor:
    """
    Мониторит новые письма с использованием IMAP IDLE и периодической проверки.
    В качестве imap библиотеки применяется imapclient.

    Этот класс подключается к IMAP-серверу, отслеживает новые непрочитанные письма в папке INBOX,
    обрабатывает их содержимое и вложения, сохраняет данные на диск и отмечает письма как прочитанные.
    Использует IMAP IDLE для мгновенного обнаружения новых писем и периодическую проверку для надежности.
    """

    def __init__(
            self,
            email_user: str,
            email_pass: str,
            imap_server: str,
            imap_port: int,
    ):
        """
        Инициализирует мониторинг почты с заданными параметрами подключения.

        Args:
            email_user: Адрес электронной почты пользователя
            email_pass: Пароль от почтового ящика
            imap_server: Адрес IMAP-сервера
            imap_port: Порт IMAP-сервера

        """
        self.email_user: str = email_user
        self.email_pass: str = email_pass
        self.imap_server: str = imap_server
        self.imap_port: int = imap_port
        self.running: bool = False
        self.server: IMAPClient | None = None
        # Интервал принудительной проверки в секундах (например, каждые 2 минуты)
        self.periodic_check_interval = 120

    def connect(self):
        """
        Устанавливает защищенное соединение с IMAP-сервером.

        Создает новый клиент IMAPClient с SSL, выполняет вход с использованием учетных данных
        и выбирает папку INBOX для мониторинга.
        """
        # Проверяем, не открыто ли уже соединение, чтобы избежать утечек
        if self.server:
            self.disconnect()

        self.server = IMAPClient(self.imap_server, port=self.imap_port, ssl=True)
        self.server.login(self.email_user, self.email_pass)
        self.server.select_folder("INBOX")  # Выбираем папку INBOX для обработки входящих писем
        logger.info("Подключено к IMAP-серверу")

    def disconnect(self) -> None:
        """
        Безопасно закрывает соединение с IMAP-сервером.

        Завершает активный режим IDLE (если он включен), выполняет logout и сбрасывает сервер.
        """
        if not self.server:
            return
        try:
            # Пытаемся завершить режим IDLE, если он активен
            self.server.idle_done()
        except Exception:
            pass  # Игнорируем ошибки завершения IDLE, так как соединение все равно будет закрыто
        try:
            # Выполняем logout для корректного завершения сессии
            self.server.logout()
            logger.info("IMAP-соединение закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии IMAP-соединения: {e}")
        self.server = None

    def stop(self) -> None:
        """
        Останавливает мониторинг и закрывает все ресурсы.

        Отменяет периодическую проверку, завершает IDLE, закрывает соединение
        и сбрасывает флаг работы.
        """
        self.running = False
        self.disconnect()
        logger.info("Мониторинг остановлен")

    def process_unseen_email_inbox(self) -> None:
        """
        Обрабатывает непрочитанные письма в папке INBOX.

        Ищет непрочитанные письма, извлекает их метаданные и вложения, сохраняет вложения в
        уникальную папку, создает файл метаданных metadata.json и отмечает письма
        как прочитанные после обработки.

        Использует peek для получения писем без автоматической отметки как прочитанных,
        что позволяет контролировать этот процесс вручную. Пропускает письма с ошибками,
        логируя их, чтобы продолжить обработку остальных.

        Args:
            None

        Returns:
            None
        """
        try:
            # Поиск непрочитанных писем
            message_ids = self.server.search(["UNSEEN"])
            if not message_ids:
                logger.info("Новых писем нет")
                return

            logger.info(f"Обнаружено непрочитанных писем: {len(message_ids)}")

            # Последовательная обработка каждого письма
            for msg_id in message_ids:
                try:
                    # Получаем данные письма без изменения статуса (BODY.PEEK)
                    msg_data = self.server.fetch(msg_id, ["BODY.PEEK[]"])
                    if not msg_data or msg_id not in msg_data:
                        logger.error(f"Не удалось получить данные письма ID {msg_id}")
                        continue

                    # Парсим письмо в объект Message для удобной работы с содержимым
                    email_message: Message = email.message_from_bytes(msg_data[msg_id][b"BODY[]"])

                    # Собираем метаданные письма для сохранения
                    metadata = {
                        "subject": decode_subject(email_message.get("Subject", "")),
                        "sender": parseaddr(email_message.get("From", ""))[1],
                        "date": email_message.get("Date", "Unknown date"),
                        "text_content": extract_text_content(email_message) or "No text content",
                        "files": [],
                        "errors": []
                    }

                    # Извлечение и обработка вложений
                    attachments: list[tuple[str, bytes]] = extract_attachments(email_message)

                    if not attachments:
                        logger.info(f"Письмо от {metadata['sender']} не содержит вложений")
                        # Отметка письма как прочитанного
                        self.server.add_flags(msg_id, ["\\Seen"])
                        continue

                    # Обработка вложений при их наличии
                    logger.info(f"В письме от {metadata['sender']} найдено вложений: {len(attachments)}")

                    # Формирование уникального имени папки на основе даты и отправителя
                    try:
                        # По возможности в качестве даты и времени берем информацию из метаданных
                        date_time = parsedate_to_datetime(metadata['date']).strftime("%y%m%d_%H%M%S")
                    except (ValueError, TypeError):
                        # Если в метаданных отсутствует дата отправки, то берем текущую дату и время
                        date_time = datetime.now().strftime("%y%m%d_%H%M%S")

                    folder_path = CONFIG.IN_FOLDER / sanitize_pathname(
                        name=f"{date_time}_{metadata['sender']}",
                        is_file=False,
                        parent_dir=CONFIG.IN_FOLDER
                    )
                    # Создание директории
                    folder_path.mkdir(exist_ok=True, parents=True)
                    logger.debug(f"Создана директория: {folder_path}")

                    # Последовательная обработка каждого вложения
                    for file_name, content in attachments:
                        file_ext = Path(file_name).suffix.lower()
                        if file_ext not in CONFIG.valid_ext:
                            valid_ext_text = ", ".join(f"'*{ext}'" for ext in CONFIG.valid_ext)
                            error_msg = (
                                f"{file_name}: Неподдерживаемое расширение. "
                                f"Допустимые: {valid_ext_text}."
                            )
                            metadata["errors"].append(error_msg)
                            logger.warning(error_msg)
                            continue

                        # Создание безопасного имени файла
                        file_path = folder_path / sanitize_pathname(
                            file_name, is_file=True, parent_dir=folder_path
                        )

                        try:
                            # Сохраняем файл
                            file_path.write_bytes(content)
                            # Записываем в метаданные пару: имя исходного файла
                            # и имя для будущего файла с информацией
                            metadata["files"].append((
                                f"{file_path.name}",
                                f"{file_path.stem}({file_path.suffix[1:]}).json"
                            ))
                            logger.info(f"Файл сохранен: {file_path}")
                        except OSError as e:
                            logger.error(f"Ошибка при сохранении файла {file_path}: {e}")

                    # Сохранение метаданных
                    write_json(folder_path / "metadata.json", metadata)
                    logger.info(f"Сохранены метаданные: {folder_path / 'metadata.json'}")

                    # Отмечаем письмо как прочитанное после успешной обработки
                    self.server.add_flags(msg_id, ["\\Seen"])
                    logger.info(f"Письмо ID {msg_id} обработано и отмечено как прочитанное")

                except Exception as e:
                    logger.error(f"Ошибка обработки письма ID {msg_id}: {traceback.format_exc()}")

        except Exception:
            logger.error(f"Произошла ошибка при обработке писем: {traceback.format_exc()}")

    def monitor(self) -> None:
        """
        Запускает мониторинг новых писем с использованием IMAP IDLE и периодической проверки.

        Устанавливает соединение, выполняет начальную проверку писем, запускает периодическую
        проверку и входит в режим IDLE для мгновенного обнаружения новых писем. При ошибках
        пытается переподключиться, сохраняя стабильность работы.
        """
        self.running = True
        try:
            self.connect()
            logger.info("Запущен IDLE-мониторинг почты")
            logger.info(f"Принудительная проверка писем включена "
                        f"с интервалом {self.periodic_check_interval} сек.")

            # Запоминаем время принудительной последней проверки
            last_check_time = 0

            while self.running:
                try:
                    # Выполняем периодическую проверку непрочитанных писем
                    if time.time() - last_check_time >= self.periodic_check_interval:
                        logger.debug("Выполняется принудительная проверка писем")
                        # Выполняем проверку
                        self.process_unseen_email_inbox()
                        last_check_time = time.time()

                    # Входим в режим IDLE для ожидания уведомлений от сервера
                    self.server.idle()
                    # Ждем уведомлений с таймаутом 10 секунд
                    responses = self.server.idle_check(timeout=10)
                    self.server.idle_done()
                    if responses:
                        logger.debug(f"IDLE уведомления: {responses}")
                        self.process_unseen_email_inbox()
                        last_check_time = time.time()

                except Exception as e:
                    logger.error(f"Ошибка в режиме IDLE: {e}")
                    self.disconnect()
                    time.sleep(5)  # Ждем перед переподключением
                    if self.running:
                        self.connect()
                        # После переподключения выполняем проверку сразу
                        self.process_unseen_email_inbox()
                        last_check_time = time.time()

        except Exception as e:
            logger.error(f"Критическая ошибка мониторинга: {e}")
        finally:
            self.stop()


def process_email_inbox_simple(
        email_user: str,
        email_pass: str,
        imap_server: str,
        imap_port: int,
) -> None:
    """
    Обрабатывает новые письма в IMAP-ящике и извлекает вложения.
    В качестве imap библиотеки стандартная imaplib.

    Функция подключается к почтовому ящику, ищет непрочитанные письма, извлекает их метаданные
    и вложения, сохраняет вложения в папку IN_FOLDER с уникальным именем, создает файл
    metadata.json с информацией о письме и файлах. Письма отмечаются как прочитанные после
    успешной обработки.

    Args:
        email_user: Адрес электронной почты пользователя
        email_pass: Пароль от почтового ящика
        imap_server: Адрес IMAP-сервера
        imap_port: Порт IMAP-сервера

    Returns:
        None: Функция не возвращает значений, но сохраняет файлы и метаданные на диск.
    """
    # Подключение к IMAP-серверу и выполнение авторизации
    try:
        mail = imaplib.IMAP4_SSL(imap_server, imap_port)  # Создание SSL соединения
        mail.login(email_user, email_pass)  # Авторизация
        mail.select("INBOX")  # Выбор папки "Входящие"
        logger.info("Установлено соединение с IMAP-сервером")
    except Exception as e:
        logger.error(f"Не удалось подключиться к IMAP-серверу: {e}")
        return

    try:
        # Поиск непрочитанных писем
        status, messages = mail.search(None, "UNSEEN")
        if status != "OK":
            logger.error("Ошибка при поиске непрочитанных писем")
            return
        message_ids: list[bytes] = messages[0].split()
        if not message_ids:
            logger.info("Новых писем нет")
            return

        logger.info(f"Обнаружено новых писем: {len(message_ids)}")

        # Последовательная обработка каждого письма
        for msg_id in message_ids:
            msg_id_str = msg_id.decode('utf-8')
            try:
                # Получение письма без отметки как прочитанное
                status, msg_data = mail.fetch(msg_id_str, 'BODY.PEEK[]')
                if status != 'OK':
                    logger.warning(f"Не удалось получить письмо ID {msg_id_str}")
                    continue

                # Парсинг email-сообщения
                email_message: Message = email.message_from_bytes(msg_data[0][1])

                # Сбор метаданных письма
                metadata = {
                    "subject": decode_subject(email_message.get("Subject", "")),
                    "sender": parseaddr(email_message.get("From", ""))[1],
                    "date": email_message.get("Date", "Unknown date"),
                    "text_content": extract_text_content(email_message) or "No text content",
                    "files": [],
                    "errors": []
                }

                # Извлечение и обработка вложений
                attachments: list[tuple[str, bytes]] = extract_attachments(email_message)

                if not attachments:
                    logger.info(f"Письмо от {metadata['sender']} не содержит вложений")
                    # Отметка письма как прочитанного
                    mail.store(msg_id_str, "+FLAGS", "\\Seen")
                    continue

                # Обработка вложений при их наличии
                logger.info(f"В письме от {metadata['sender']} найдено вложений: {len(attachments)}")

                # Формирование уникального имени папки на основе даты и отправителя
                try:
                    # По возможности в качестве даты и времени берем информацию из метаданных
                    date_time = parsedate_to_datetime(metadata['date']).strftime("%y%m%d_%H%M%S")
                except (ValueError, TypeError):
                    # Если в метаданных отсутствует дата отправки, то берем текущую дату и время
                    date_time = datetime.now().strftime("%y%m%d_%H%M%S")

                folder_path = CONFIG.IN_FOLDER / sanitize_pathname(
                    name=f"{date_time}_{metadata['sender']}",
                    is_file=False,
                    parent_dir=CONFIG.IN_FOLDER
                )
                # Создание директории
                folder_path.mkdir(exist_ok=True, parents=True)
                logger.debug(f"Создана директория: {folder_path}")

                # Последовательная обработка каждого вложения
                for file_name, content in attachments:
                    file_ext = Path(file_name).suffix.lower()
                    if file_ext not in CONFIG.valid_ext:
                        valid_ext_text = ", ".join(f"'*{ext}'" for ext in CONFIG.valid_ext)
                        error_msg = (
                            f"{file_name}: Неподдерживаемое расширение. "
                            f"Допустимые: {valid_ext_text}."
                        )
                        metadata["errors"].append(error_msg)
                        logger.warning(error_msg)
                        continue

                    # Создание безопасного имени файла
                    file_path = folder_path / sanitize_pathname(
                        file_name, is_file=True, parent_dir=folder_path
                    )

                    try:
                        # Сохраняем файл
                        file_path.write_bytes(content)
                        # Записываем в метаданные пару: имя исходного файла
                        # и имя для будущего файла с информацией
                        metadata["files"].append((
                            f"{file_path.name}",
                            f"{file_path.stem}({file_path.suffix[1:]}).json"
                        ))
                        logger.info(f"Файл сохранен: {file_path}")
                    except OSError as e:
                        logger.error(f"Ошибка при сохранении файла {file_path}: {e}")

                # Сохранение метаданных
                write_json(folder_path / "metadata.json", metadata)
                logger.debug(f"Сохранены метаданные: {folder_path / 'metadata.json'}")

                # Отмечаем письмо как прочитанное после успешной обработки
                mail.store(msg_id_str, '+FLAGS', '\\Seen')
                logger.info(f"Письмо ID {msg_id_str} обработано и отмечено как прочитанное")

            except Exception as e:
                logger.error(f"Ошибка обработки письма ID {msg_id_str}: {traceback.format_exc()}")

    except Exception:
        logger.error(f"Произошла ошибка при обработке писем: {traceback.format_exc()}")

    finally:
        # Безопасное завершение соединения
        try:
            mail.close()
            mail.logout()
            logger.info("IMAP-соединение закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии IMAP-соединения: {e}")
