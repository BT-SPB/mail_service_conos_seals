import email
import imaplib
import time
import traceback
from pathlib import Path
from datetime import datetime
from typing import Callable
from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime

import imaplib2
from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientError

from config import CONFIG
from src.logger import logger
from src.utils import write_json, sanitize_pathname
from src.utils_email import (
    decode_subject,
    extract_text_content,
    extract_attachments,
)


class EmailMonitor:
    """Мониторит новые письма с использованием IMAP IDLE."""

    def __init__(
            self,
            email_user: str,
            email_pass: str,
            imap_server: str,
            imap_port: int,
            callback: Callable[[], None],
    ):
        """Инициализирует мониторинг почты."""
        self.email_user = email_user
        self.email_pass = email_pass
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.callback = callback
        self.running = False
        self.server = None

    def connect(self):
        """Устанавливает соединение с IMAP-сервером."""
        self.server = IMAPClient(self.imap_server, port=self.imap_port, ssl=True)
        self.server.login(self.email_user, self.email_pass)
        self.server.select_folder("INBOX")
        logger.info("Подключено к IMAP-серверу и запущен IDLE-мониторинг")

    def monitor(self):
        """Запускает мониторинг новых писем с использованием IDLE."""
        self.running = True
        self.connect()

        while self.running:
            try:
                # Входим в режим IDLE
                self.server.idle()
                # Ждём уведомлений до 30 секунд
                responses = self.server.idle_check(timeout=1)
                # Завершаем IDLE
                self.server.idle_done()

                # Проверяем, есть ли уведомления о новых письмах
                if responses:
                    # Логируем полученные уведомления для отладки
                    logger.debug(f"IDLE ответы: {responses}")
                    # unseen_messages = self.server.search('UNSEEN', None)
                    # logger.debug(unseen_messages)
                    for response in responses:
                        if isinstance(response, tuple) and response[1] == b"EXISTS":
                            logger.info("Обнаружено новое письмо через IDLE")
                            self.callback()
                            break  # Выходим из цикла обработки, чтобы сразу начать новый IDLE

            except Exception as e:
                logger.error(f"Ошибка мониторинга: {e}")
                self.stop()  # Закрываем текущее соединение
                time.sleep(5)  # Ждём перед переподключением
                if self.running:  # Переподключаемся только если мониторинг не остановлен
                    self.connect()

    def stop(self):
        """Останавливает мониторинг."""
        self.running = False
        if self.server:
            try:
                self.server.idle_done()  # Завершаем IDLE, если активно
            except:
                pass
            try:
                self.server.logout()
                logger.info("IMAP-соединение закрыто")
            except Exception as e:
                logger.error(f"Ошибка при закрытии IMAP: {e}")
            self.server = None


def process_email_inbox(
        email_user: str,
        email_pass: str,
        imap_server: str,
        imap_port: int,
) -> None:
    """
    Обрабатывает новые письма в IMAP-ящике и извлекает вложения.

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
        logger.debug(mail.search(None, "UNSEEN"))
        if status != "OK":
            logger.error("Ошибка при поиске непрочитанных писем")
            return
        message_ids: list[bytes] = messages[0].split()
        if not message_ids:
            logger.info("Новых писем нет")
            return

        logger.info(f"Обнаружено {len(message_ids)} новых писем")

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
                logger.info(f"В письме от {metadata['sender']} найдено {len(attachments)} вложений")

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
