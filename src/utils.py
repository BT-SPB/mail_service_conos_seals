import traceback
import json
import smtplib
import chardet
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union, Literal

import imaplib
from email.message import Message
from email.header import decode_header
from email.mime.text import MIMEText


def write_json(file_path: Path | str, data: Any) -> None:
    """Записывает данные в JSON файл с форматированием.

    Args:
        file_path: Путь к файлу (строка или объект Path)
        data: Данные для записи в JSON формате

    Returns:
        None
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def connect_to_imap(email_user: str, email_pass: str, imap_server: str,
                    imap_port: int = 993) -> Optional[imaplib.IMAP4_SSL]:
    """Устанавливает соединение с IMAP сервером и выполняет авторизацию"""
    try:
        mail = imaplib.IMAP4_SSL(imap_server, imap_port)  # Создание SSL соединения
        mail.login(email_user, email_pass)  # Авторизация
        mail.select("inbox")  # Выбор папки "Входящие"
        return mail
    except Exception as e:
        raise Exception(f"Ошибка подключения к IMAP: {str(e)}")


def get_unseen_messages(mail: imaplib.IMAP4_SSL) -> List[bytes]:
    """Возвращает список ID непрочитанных писем"""
    status, messages = mail.search(None, 'UNSEEN')  # Поиск непрочитанных писем
    if status != 'OK':
        print("Ошибка при поиске писем")
        return []
    message_ids: List[bytes] = messages[0].split()  # Разделение строки ID на список
    return message_ids


def detect_encoding(body: bytes) -> str:
    """Определяет кодировку для переданных байтов"""

    # 1. Определяем через chardet
    detection = chardet.detect(body)
    encoding = detection['encoding'] if detection['confidence'] > 0.7 else None
    if encoding:
        try:
            body.decode(encoding)  # Проверяем, работает ли
            return encoding
        except UnicodeDecodeError:
            pass

    # 2. Fallback-кодировки
    for fallback_encoding in ('utf-8', 'windows-1251', 'iso-8859-1'):
        try:
            body.decode(fallback_encoding)  # Проверяем
            return fallback_encoding
        except UnicodeDecodeError:
            continue

    # 3. Если ничего не подошло, возвращаем utf-8
    return 'utf-8'


def decode_subject(subject: Optional[str]) -> str:
    """Декодирует тему письма из закодированного формата"""
    if not subject:
        return "(Без темы)"
    decoded: List[Tuple[Union[bytes, str], Optional[str]]] = decode_header(subject)
    subject_text: str = ""
    for text, encoding in decoded:
        if isinstance(text, bytes):
            subject_text += text.decode(encoding or 'utf-8', errors='ignore')
        else:
            subject_text += text
    return subject_text


def extract_text_content(email_message: Message) -> str | None:
    """
    Извлекает текстовую часть из email-сообщения.

    Args:
        email_message: Объект email-сообщения для обработки.

    Returns:
        Optional[str]: Текстовая часть письма или None, если текст не найден.
    """
    # Проверяем, является ли сообщение многосоставным (multipart): текст + HTML + вложения + ...
    if email_message.is_multipart():
        # Проходим по всем частям сообщения с помощью walk()
        for part in email_message.walk():
            # Ищем часть с типом text/plain (обычный текст)
            if part.get_content_type() == "text/plain":
                # Декодируем содержимое в байты
                body: bytes = part.get_payload(decode=True)
                if body:
                    # Определяем кодировку и преобразуем байты в строку
                    encoding = detect_encoding(body)
                    return body.decode(encoding, errors='ignore')
    else:
        # Обрабатываем случай, если сообщение не многосоставное
        body = email_message.get_payload(decode=True)
        if body:
            encoding = detect_encoding(body)
            return body.decode(encoding, errors='ignore')
    return None


def extract_html_content(email_message: Message) -> str | None:
    """
    Извлекает HTML-часть из email-сообщения.

    Args:
        email_message: Объект email-сообщения для обработки.

    Returns:
        Optional[str]: HTML-часть письма или None, если HTML не найден.
    """
    # Инициализируем переменную для хранения HTML-контента
    html_content: bytes | None = None
    if email_message.is_multipart():
        # Проходим по всем частям сообщения для поиска HTML
        for part in email_message.walk():
            if part.get_content_type() == "text/html":
                html_content: bytes = part.get_payload(decode=True)
                # Выходим из цикла после нахождения первой HTML-части
                break
    # Если сообщение не многосоставное, проверяем его тип напрямую
    elif email_message.get_content_type() == "text/html":
        html_content: bytes = email_message.get_payload(decode=True)

    # Обрабатываем найденный HTML-контент
    if html_content:
        encoding = detect_encoding(html_content)
        html_decoded: str = html_content.decode(encoding, errors='ignore')
        return html_decoded
    return None


def extract_attachments(email_message: Message) -> list[tuple[str, bytes]]:
    """
    Извлекает вложения из email-сообщения.

    Args:
        email_message: Объект email-сообщения для обработки.

    Returns:
        list[tuple[str, bytes]]: Список кортежей, содержащих имя файла и его содержимое в байтах.
    """
    attachments: list[tuple[str, bytes]] = []

    # Проверяем, является ли сообщение многосоставным
    if not email_message.is_multipart():
        return attachments  # Возвращаем пустой список, если нет частей

    # Проходим по всем частям сообщения
    for part in email_message.walk():
        # Проверяем, является ли часть вложением
        content_disposition = part.get("Content-Disposition")
        if content_disposition and "attachment" in content_disposition.lower():
            filename = part.get_filename()  # Получаем имя файла вложения
            payload = part.get_payload(decode=True)  # Получаем содержимое вложения

            if filename and payload:
                # Добавляем кортеж (имя файла, содержимое) в список вложений
                attachments.append((filename, payload))

    return attachments


def send_email(email_text: str,
               email_format: Literal['plain', 'html'],
               recipient_email: str,
               subject: str,
               email_user: str,
               email_pass: str,
               smtp_server: str = "smtp.gmail.com",
               smtp_port: int = 587) -> bool:
    """
    Отправляет email с заданным текстом на указанный адрес

    Args:
        email_text: Текст письма
        email_format: Тип письма plain / html
        recipient_email: Адрес получателя
        subject: Тема письма
        email_user: Адрес отправителя/логин
        email_pass: Пароль отправителя
        smtp_server: SMTP сервер (по умолчанию Gmail)
        smtp_port: SMTP порт (по умолчанию 587)

    Returns:
        bool: Успешность отправки
    """
    try:
        # Создаем объект письма
        msg = MIMEText(email_text, email_format, 'utf-8')
        msg['Subject'] = subject
        msg['From'] = email_user
        msg['To'] = recipient_email

        # Устанавливаем соединение с SMTP сервером
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Запускаем шифрование
            server.login(email_user, email_pass)  # Авторизуемся
            server.send_message(msg)  # Отправляем письмо

        return True

    except Exception:
        print(traceback.format_exc())
        return False
