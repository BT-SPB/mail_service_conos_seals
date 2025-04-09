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
from src.utils import write_json, sanitize_pathname
from src.utils_email import (
    connect_to_imap,
    get_unseen_messages,
    decode_subject,
    extract_text_content,
    extract_attachments,
    send_email,
)


def process_email_inbox(
        email_user: str,
        email_pass: str,
        imap_server: str,
        imap_port: int,
) -> None:
    """Обрабатывает новые письма в IMAP-ящике и извлекает вложения.

    Args:
        email_user: Адрес электронной почты пользователя
        email_pass: Пароль от почтового ящика
        imap_server: Адрес IMAP-сервера
        imap_port: Порт IMAP-сервера

    Returns:
        None: Функция не возвращает значений, но сохраняет файлы и метаданные на диск.
    """
    # Установка защищенного соединения с IMAP-сервером
    mail: imaplib.IMAP4_SSL | None = connect_to_imap(
        email_user, email_pass, imap_server, imap_port
    )
    if not mail:
        logger.print("Не удалось установить соединение с сервером")
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
                # Генерация уникального имени папки на основе даты и отправителя
                try:
                    date_time = parsedate_to_datetime(email_metadata['date']).strftime("%y%m%d_%H%M%S")
                except (ValueError, TypeError):
                    date_time = datetime.now().strftime("%y%m%d_%H%M%S")

                folder_name = f"{date_time}_{email_metadata['sender']}"
                folder_name = sanitize_pathname(folder_name, is_file=False, parent_dir=CONFIG.IN_FOLDER)
                folder_path = CONFIG.IN_FOLDER / folder_name

                # Список для имен файлов с неподдерживаемыми расширениями
                unsupported_files: list[str] = []

                # Проходим по всем вложениям
                for file_name, content in attachments:
                    file_ext = Path(file_name).suffix
                    if file_ext not in CONFIG.valid_ext:
                        unsupported_files.append(file_name)
                        logger.print(f"Unsupported file: {file_name}")
                        continue

                    # Создание директории и безопасного имени файла
                    folder_path.mkdir(exist_ok=True, parents=True)
                    file_name = sanitize_pathname(file_name, is_file=True, parent_dir=folder_path)
                    file_path = folder_path / file_name

                    try:
                        # Сохраняем файл
                        file_path.write_bytes(content)
                        email_metadata.setdefault("files", []).append(file_name)
                        logger.print(f"Файл сохранен: {file_path}")
                    except OSError as e:
                        logger.print(f"Ошибка при сохранении файла {file_path}: {e}")

                # Сохранение метаданных, если есть обработанные файлы
                if email_metadata.get("files"):
                    write_json(folder_path / "metadata.json", email_metadata)

                # Отправка уведомления на email об неподдерживаемых файлах
                if unsupported_files:
                    unsupported_files_text = "\n".join(
                        f"{i}. {file_name}" for i, file_name in enumerate(unsupported_files, 1))
                    valid_ext_text = ", ".join(f"'*{ext}'" for ext in CONFIG.valid_ext)
                    email_text = (
                        f"В сообщении от {email_metadata['date']} были прикреплены следующие "
                        f"неподдерживаемые файлы:\n\n{unsupported_files_text}\n\n"
                        f"Система автораспознавания информации с коносаментов поддерживает "
                        f"следующие типы файлов: {valid_ext_text}."
                    )
                    send_email(
                        email_text=email_text,
                        recipient_email=email_metadata["sender"],
                        subject=f"Автоответ от {email_user}",
                        email_user=email_user,
                        email_pass=email_pass,
                    )

            # Отмечаем письмо как прочитанное после успешной обработки
            mail.store(msg_id_str, '+FLAGS', '\\Seen')

    except Exception:
        logger.print(f"Произошла ошибка: {traceback.format_exc()}")
        raise

    finally:
        # Безопасное завершение соединения
        logger.print("Закрытие соединения с сервером...")
        mail.close()
        mail.logout()
