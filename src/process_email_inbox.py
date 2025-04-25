import email
import imaplib
import time
import traceback
from pathlib import Path
from email.message import Message
from email.utils import parseaddr
import ssl

from imapclient import IMAPClient, exceptions

from config import CONFIG
from src.logger import logger
from src.utils import write_json, sanitize_pathname
from src.utils_email import (
    convert_email_date_to_moscow,
    decode_subject,
    extract_text_content,
    extract_attachments,
)


class EmailMonitor:
    """
    Мониторит новые письма с использованием IMAP IDLE и периодической проверки..

    Использует библиотеку imapclient для подключения к IMAP-серверу, отслеживает новые непрочитанные
    письма в папке INBOX, обрабатывает их содержимое и вложения, сохраняет данные на диск и отмечает
    письма как прочитанные. Применяет как асинхронное оповещение (IDLE), так и периодическую
    проверку для устойчивости.
    """

    def __init__(
            self,
            email_user: str,
            email_pass: str,
            imap_server: str,
            imap_port: int,
            idle_timeout: int = 10,
            forced_timeout: int = 300,
    ) -> None:
        """
        Инициализирует мониторинг с параметрами IMAP-соединения.

        Args:
            email_user: Электронная почта пользователя
            email_pass: Пароль к почтовому ящику
            imap_server: Адрес IMAP-сервера
            imap_port: Порт подключения к IMAP-серверу
            idle_timeout: Таймаут ожидания в режиме IDLE, в секундах
            forced_timeout: Интервал принудительной проверки, в секундах
        """
        self.email_user: str = email_user
        self.email_pass: str = email_pass
        self.imap_server: str = imap_server
        self.imap_port: int = imap_port
        self.forced_timeout = forced_timeout
        self.idle_timeout = idle_timeout

        # Инициализация состояния мониторинга
        self.running: bool = False
        self.server: IMAPClient | None = None

    def connect(self):
        """
        Устанавливает защищенное соединение с IMAP-сервером.

        Создает новый клиент IMAPClient с SSL, выполняет вход с использованием учетных данных
        и выбирает папку INBOX для мониторинга.
        """
        # Если соединение уже установлено — закрываем его перед повторным подключением
        if self.server:
            self.disconnect()

        try:
            self.server = IMAPClient(self.imap_server, port=self.imap_port, ssl=True)
            self.server.login(self.email_user, self.email_pass)
            self.server.select_folder("INBOX")  # Выбираем папку INBOX для обработки входящих писем
            logger.info("📡 Подключено к IMAP-серверу")
        except Exception as e:
            logger.error(f"⛔ Ошибка подключения к IMAP-серверу: {e}\n{traceback.format_exc()}")
            self.server = None

    def disconnect(self) -> None:
        """
        Безопасно закрывает соединение с IMAP-сервером, завершая режим IDLE и выполняя logout.
        """
        if not self.server:
            return

        try:
            # Завершаем IDLE, если он активен
            self.server.idle_done()
        except Exception:
            # Ошибки IDLE завершения не критичны — продолжаем отключение
            pass

        try:
            # Выполняем logout для корректного завершения сессии
            self.server.logout()
            logger.info("🔔 IMAP-соединение закрыто")
        except (exceptions.IMAPClientError, OSError, ssl.SSLError) as e:
            logger.warning(f"⚠️ IMAP logout завершился с ошибкой (возможно ожидаемо): {e}")
        except Exception as e:
            logger.error(f"⛔ Ошибка при закрытии IMAP-соединения: {e}\n{traceback.format_exc()}")
        finally:
            self.server = None

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

    def process_unseen_email_inbox(self) -> None:
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
            message_ids = self.server.search(["UNSEEN"])
            if not message_ids:
                logger.info("➖ Новых писем нет")
                return

            logger.info(f"📧 Обнаружено непрочитанных писем: {len(message_ids)}")

            # Последовательная обработка каждого письма
            for msg_id in message_ids:
                try:
                    # Получаем данные письма без изменения статуса (BODY.PEEK)
                    msg_data = self.server.fetch(msg_id, ["BODY.PEEK[]"])
                    if not msg_data or msg_id not in msg_data:
                        logger.error(f"❌ Не удалось получить данные письма ID {msg_id}")
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
                        "errors": [],
                        "successes": [],
                    }

                    # Извлечение и обработка вложений
                    attachments: list[tuple[str, bytes]] = extract_attachments(email_message)

                    if not attachments:
                        logger.info(f"📧 Письмо от {metadata['sender']} не содержит вложений")
                        # Отметка письма как прочитанного
                        self.server.add_flags(msg_id, ["\\Seen"])
                        continue

                    # Обработка вложений при их наличии
                    logger.info(f"📧 В письме от {metadata['sender']} найдено вложений: {len(attachments)}")

                    # Формирование уникального имени папки на основе даты и времени отправки письма
                    date_time = convert_email_date_to_moscow(metadata["date"], "%y%m%d_%H%M%S")
                    folder_path = sanitize_pathname(
                        CONFIG.IN_FOLDER / f"{date_time}_{metadata['sender']}",
                        is_file=False
                    )

                    # Создание директории
                    folder_path.mkdir(exist_ok=True, parents=True)
                    logger.debug(f"✔️ Создана директория: {folder_path}")

                    # Последовательная обработка каждого вложения
                    for file_name, content in attachments:
                        file_ext = Path(file_name).suffix.lower()
                        if file_ext not in CONFIG.valid_ext:
                            valid_ext_text = ", ".join(f"'*{ext}'" for ext in CONFIG.valid_ext)
                            warning_message = (
                                f"{file_name}: Неподдерживаемое расширение. "
                                f"Допустимые: {valid_ext_text}."
                            )
                            metadata["errors"].append(warning_message)
                            logger.warning(f"❌ {warning_message}")
                            continue

                        # Создание безопасного имени файла
                        file_path = sanitize_pathname(folder_path / file_name, is_file=True)

                        try:
                            # Сохраняем файл
                            file_path.write_bytes(content)
                            # Записываем в метаданные имя исходного файла
                            metadata["files"].append(f"{file_path.name}")
                            logger.info(f"💾 Файл сохранен: {file_path}")
                        except OSError as e:
                            logger.error(f"⛔ Ошибка при сохранении файла {file_path}: {e}")

                    # Сохранение метаданных
                    write_json(folder_path / "metadata.json", metadata)
                    logger.info(f"💾 Сохранены метаданные: {folder_path / 'metadata.json'}")

                    # Отмечаем письмо как прочитанное после успешной обработки
                    self.server.add_flags(msg_id, ["\\Seen"])
                    logger.info(f"✔️ Письмо ID {msg_id} обработано и отмечено как прочитанное")

                except Exception as e:
                    logger.error(f"⛔ Ошибка обработки письма ID {msg_id}: {e}\n{traceback.format_exc()}")

        except Exception as e:
            logger.error(f"⛔ Произошла ошибка при обработке писем: {e}\n{traceback.format_exc()}")

    def monitor(self) -> None:
        """
        Запускает постоянный мониторинг почты, используя режим IDLE и периодическую проверку.

        Производит начальную проверку писем, затем в цикле ожидает новых писем через IDLE.
        Периодически выполняется ручная проверка писем на случай потери IDLE-событий.
        При возникновении ошибок выполняется переподключение.
        """
        self.running = True
        last_check_time = 0

        try:
            self.connect()
            logger.info(f"🔄 IDLE-мониторинг почты (принудительный таймаут {self.forced_timeout} сек)")

            while self.running:
                try:
                    # Выполняем периодическую проверку непрочитанных писем
                    if time.time() - last_check_time >= self.forced_timeout:
                        logger.debug("🕒 Принудительная проверка email по таймеру")
                        # Выполняем проверку
                        self.process_unseen_email_inbox()
                        last_check_time = time.time()

                    # Входим в режим IDLE — ожидание новых писем от сервера
                    self.server.idle()
                    responses = self.server.idle_check(timeout=self.idle_timeout)
                    self.server.idle_done()

                    # Если есть новые события, инициируем повторную проверку
                    if responses:
                        logger.debug(f"🔔 IDLE уведомления: {responses}")
                        self.process_unseen_email_inbox()
                        last_check_time = time.time()

                except Exception as e:
                    if not self.running:
                        # Прерывание в момент остановки — не логируем
                        break

                    logger.error(f"⛔ Ошибка в IDLE-цикле: {e}\n{traceback.format_exc()}")
                    self.disconnect()
                    time.sleep(5)  # Даем время серверу перед переподключением
                    if self.running:
                        self.connect()
                        logger.info("🔄 Соединение восстановлено после ошибки")
                        self.process_unseen_email_inbox()  # После переподключения сразу выполняем проверку
                        last_check_time = time.time()

        except Exception as e:
            logger.error(f"⛔ Критическая ошибка мониторинга: {e}\n{traceback.format_exc()}")

# --- ЗАПАСНАЯ ФУНКЦИЯ ---
# Предназначена для упрощенного мониторинга, используя вечный цикл
# def process_email_inbox_simple(
#         email_user: str,
#         email_pass: str,
#         imap_server: str,
#         imap_port: int,
# ) -> None:
#     """
#     Обрабатывает новые письма в IMAP-ящике и извлекает вложения.
#     В качестве imap библиотеки стандартная imaplib.
#
#     Функция подключается к почтовому ящику, ищет непрочитанные письма, извлекает их метаданные
#     и вложения, сохраняет вложения в папку IN_FOLDER с уникальным именем, создает файл
#     metadata.json с информацией о письме и файлах. Письма отмечаются как прочитанные после
#     успешной обработки.
#
#     Args:
#         email_user: Адрес электронной почты пользователя
#         email_pass: Пароль от почтового ящика
#         imap_server: Адрес IMAP-сервера
#         imap_port: Порт IMAP-сервера
#
#     Returns:
#         None: Функция не возвращает значений, но сохраняет файлы и метаданные на диск.
#     """
#     # Подключение к IMAP-серверу и выполнение авторизации
#     try:
#         mail = imaplib.IMAP4_SSL(imap_server, imap_port)  # Создание SSL соединения
#         mail.login(email_user, email_pass)  # Авторизация
#         mail.select("INBOX")  # Выбор папки "Входящие"
#         logger.info("Установлено соединение с IMAP-сервером")
#     except Exception as e:
#         logger.error(f"Не удалось подключиться к IMAP-серверу: {e}")
#         return
#
#     try:
#         # Поиск непрочитанных писем
#         status, messages = mail.search(None, "UNSEEN")
#         if status != "OK":
#             logger.error("Ошибка при поиске непрочитанных писем")
#             return
#         message_ids: list[bytes] = messages[0].split()
#         if not message_ids:
#             logger.info("Новых писем нет")
#             return
#
#         logger.info(f"Обнаружено новых писем: {len(message_ids)}")
#
#         # Последовательная обработка каждого письма
#         for msg_id in message_ids:
#             msg_id_str = msg_id.decode('utf-8')
#             try:
#                 # Получение письма без отметки как прочитанное
#                 status, msg_data = mail.fetch(msg_id_str, 'BODY.PEEK[]')
#                 if status != 'OK':
#                     logger.warning(f"Не удалось получить письмо ID {msg_id_str}")
#                     continue
#
#                 # Парсинг email-сообщения
#                 email_message: Message = email.message_from_bytes(msg_data[0][1])
#
#                 # Сбор метаданных письма
#                 metadata = {
#                     "subject": decode_subject(email_message.get("Subject", "")),
#                     "sender": parseaddr(email_message.get("From", ""))[1],
#                     "date": email_message.get("Date", "Unknown date"),
#                     "text_content": extract_text_content(email_message) or "No text content",
#                     "files": [],
#                     "errors": []
#                 }
#
#                 # Извлечение и обработка вложений
#                 attachments: list[tuple[str, bytes]] = extract_attachments(email_message)
#
#                 if not attachments:
#                     logger.info(f"Письмо от {metadata['sender']} не содержит вложений")
#                     # Отметка письма как прочитанного
#                     mail.store(msg_id_str, "+FLAGS", "\\Seen")
#                     continue
#
#                 # Обработка вложений при их наличии
#                 logger.info(f"В письме от {metadata['sender']} найдено вложений: {len(attachments)}")
#
#                 # Формирование уникального имени папки на основе даты и времени отправки письма
#                 date_time = convert_email_date_to_moscow(metadata["date"], "%y%m%d_%H%M%S")
#
#                 folder_path = CONFIG.IN_FOLDER / sanitize_pathname(
#                     name=f"{date_time}_{metadata['sender']}",
#                     is_file=False,
#                     parent_dir=CONFIG.IN_FOLDER
#                 )
#                 # Создание директории
#                 folder_path.mkdir(exist_ok=True, parents=True)
#                 logger.debug(f"Создана директория: {folder_path}")
#
#                 # Последовательная обработка каждого вложения
#                 for file_name, content in attachments:
#                     file_ext = Path(file_name).suffix.lower()
#                     if file_ext not in CONFIG.valid_ext:
#                         valid_ext_text = ", ".join(f"'*{ext}'" for ext in CONFIG.valid_ext)
#                         error_msg = (
#                             f"{file_name}: Неподдерживаемое расширение. "
#                             f"Допустимые: {valid_ext_text}."
#                         )
#                         metadata["errors"].append(error_msg)
#                         logger.warning(error_msg)
#                         continue
#
#                     # Создание безопасного имени файла
#                     file_path = folder_path / sanitize_pathname(
#                         file_name, is_file=True, parent_dir=folder_path
#                     )
#
#                     try:
#                         # Сохраняем файл
#                         file_path.write_bytes(content)
#                         # Записываем в метаданные пару: имя исходного файла
#                         # и имя для будущего файла с информацией
#                         metadata["files"].append((
#                             f"{file_path.name}",
#                             f"{file_path.stem}({file_path.suffix[1:]}).json"
#                         ))
#                         logger.info(f"Файл сохранен: {file_path}")
#                     except OSError as e:
#                         logger.error(f"Ошибка при сохранении файла {file_path}: {e}")
#
#                 # Сохранение метаданных
#                 write_json(folder_path / "metadata.json", metadata)
#                 logger.debug(f"Сохранены метаданные: {folder_path / 'metadata.json'}")
#
#                 # Отмечаем письмо как прочитанное после успешной обработки
#                 mail.store(msg_id_str, '+FLAGS', '\\Seen')
#                 logger.info(f"Письмо ID {msg_id_str} обработано и отмечено как прочитанное")
#
#             except Exception as e:
#                 logger.error(f"Ошибка обработки письма ID {msg_id_str}: {traceback.format_exc()}")
#
#     except Exception:
#         logger.error(f"Произошла ошибка при обработке писем: {traceback.format_exc()}")
#
#     finally:
#         # Безопасное завершение соединения
#         try:
#             mail.close()
#             mail.logout()
#             logger.info("IMAP-соединение закрыто")
#         except Exception as e:
#             logger.error(f"Ошибка при закрытии IMAP-соединения: {e}")
