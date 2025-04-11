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
            metadata = {
                "subject": decode_subject(email_message.get("Subject", "")),
                "sender": parseaddr(email_message.get("From", ""))[1],
                "date": email_message.get("Date", "Unknown date"),
                "text_content": extract_text_content(email_message) or "No text content",
                "files": [],
                "errors": []
            }

            # Извлечение и обработка вложений
            attachments = extract_attachments(email_message)

            if attachments:
                logger.print(f"В письме от {metadata['sender']} обнаружено вложений: {len(attachments)}")
                # Генерация уникального имени папки на основе даты и отправителя
                try:
                    date_time = parsedate_to_datetime(metadata['date']).strftime("%y%m%d_%H%M%S")
                except (ValueError, TypeError):
                    date_time = datetime.now().strftime("%y%m%d_%H%M%S")

                folder_name = f"{date_time}_{metadata['sender']}"
                folder_name = sanitize_pathname(folder_name, is_file=False, parent_dir=CONFIG.IN_FOLDER)
                folder_path = CONFIG.IN_FOLDER / folder_name
                # Создание директории
                folder_path.mkdir(exist_ok=True, parents=True)

                # Проходим по всем вложениям
                for file_name, content in attachments:
                    file_ext = Path(file_name).suffix.lower()
                    if file_ext not in CONFIG.valid_ext:
                        valid_ext_text = ", ".join(f"'*{ext}'" for ext in CONFIG.valid_ext)
                        metadata["errors"].append(
                            f"{file_name}: Неподдерживаемое расширение файла. Допустимые значения: {valid_ext_text}."
                        )
                        logger.print(f"Неподдерживаемый файл: {file_name}")
                        continue

                    # Создание безопасного имени файла
                    file_name = sanitize_pathname(file_name, is_file=True, parent_dir=folder_path)
                    file_path = folder_path / file_name

                    try:
                        # Сохраняем файл
                        file_path.write_bytes(content)
                        metadata["files"].append((str(file_name), f"{file_name.stem}({file_name.suffix[1:]}).json"))
                        logger.print(f"Файл сохранен: {file_path}")
                    except OSError as e:
                        logger.print(f"Ошибка при сохранении файла {file_path}: {e}")

                # Сохранение метаданных
                write_json(folder_path / "metadata.json", metadata)

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
