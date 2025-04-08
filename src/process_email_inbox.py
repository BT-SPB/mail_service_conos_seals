import email
import imaplib
import time
import traceback
from pathlib import Path
from datetime import datetime

from email.message import Message
from email.utils import parseaddr, parsedate_to_datetime

from config import CONFIG
from src.logger import logger
from src.utils import (
    write_json,
    sanitize_pathname,
    connect_to_imap,
    get_unseen_messages,
    decode_subject,
    extract_text_content,
    extract_attachments,
)


def process_email_inbox(
        email_user: str,
        email_pass: str,
        imap_server: str,
        imap_port: int,
) -> None:
    """Проверяет и обрабатывает новые письма в IMAP-ящике.

    Args:
        email_user: Адрес электронной почты пользователя.
        email_pass: Пароль от почтового ящика.
        imap_server: Адрес IMAP-сервера.
        imap_port: Порт IMAP-сервера.

    Returns:
        None
    """
    # Подключение к серверу
    mail: imaplib.IMAP4_SSL | None = connect_to_imap(
        email_user, email_pass, imap_server, imap_port
    )
    if not mail:
        logger.print('Нет соединения')
        return

    try:
        # Получение списка непрочитанных сообщений
        message_ids: list[bytes] = get_unseen_messages(mail)
        if not message_ids:
            logger.print("Новых писем нет")
            return

        logger.print(f"Обнаружено новых писем: {len(message_ids)}")

        # Последовательная обработка каждого письма
        for msg_id in message_ids:
            msg_id_str = msg_id.decode('utf-8')
            # Получение письма без отметки как прочитанное
            status, msg_data = mail.fetch(msg_id_str, 'BODY.PEEK[]')
            if status != 'OK':
                continue

            # Парсинг email-сообщения
            email_message: Message = email.message_from_bytes(msg_data[0][1])

            # Извлечение html части
            # html_content: Optional[str] = extract_html_content(email_message)

            # Сбор метаданных письма
            email_metadata = {
                "subject": decode_subject(email_message.get("Subject", "")),
                "sender": parseaddr(email_message.get("From", ""))[1],
                "date": email_message.get("Date", "Unknown date"),
                "text_content": extract_text_content(email_message) or "No text content"
            }

            # Извлечение и обработка вложений
            attachments = extract_attachments(email_message)

            if attachments:
                try:
                    date_time = parsedate_to_datetime(email_metadata['date']).strftime("%y%m%d_%H%M%S")
                except (ValueError, TypeError):
                    date_time = datetime.now().strftime("%y%m%d_%H%M%S")

                folder_name = f"{date_time}_{email_metadata['sender']}"
                folder_name = sanitize_pathname(folder_name, is_file=False, parent_dir=CONFIG.IN_FOLDER)

                folder_path = CONFIG.IN_FOLDER / folder_name
                folder_path.mkdir(exist_ok=True, parents=True)

                # Сохранение метаданных письма
                write_json(folder_path / "metadata.json", email_metadata)

                # Проходим по всем вложениям
                for file_name, content in attachments:
                    file_name1 = sanitize_pathname(file_name, is_file=True, parent_dir=folder_path)
                    file_path = folder_path / file_name1

                    try:
                        # Записываем содержимое в файл
                        file_path.write_bytes(content)
                        logger.print(f"Сохранен файл: {file_path}")
                    except OSError as e:
                        logger.print(f"Ошибка при сохранении файла {file_path}: {e}")


            # # Отметить как прочитанное
            # mail.store(msg_id_str, '+FLAGS', '\\Seen')

    except Exception:
        logger.print(f"Произошла ошибка: {traceback.format_exc()}")
        raise

    finally:
        logger.print("Закрытие соединения с сервером...")
        mail.close()
        mail.logout()
