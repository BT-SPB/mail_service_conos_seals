import smtplib
import traceback
import chardet
from typing import Literal, List, Optional, Tuple, Union
from collections.abc import Sequence
from zoneinfo import ZoneInfo

from email.message import Message
from email.header import decode_header
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime

from src.logger import logger


def convert_email_date_to_moscow(
        date_mail: str,
        fmt: str = "%Y-%m-%d %H:%M:%S %Z"
) -> str:
    """
    Преобразует строку с датой из заголовка email в строку с датой и временем по Москве.

    Args:
        - date_mail: Строка даты из заголовка письма (email_message.get("Date"))
        - fmt: Формат выходной строки (по умолчанию "%Y-%m-%d %H:%M:%S %Z")

    Returns:
        str: Строка с датой и временем в московском часовом поясе
    """
    try:
        dt = parsedate_to_datetime(date_mail)
        moscow_dt = dt.astimezone(ZoneInfo("Europe/Moscow"))
        return moscow_dt.strftime(fmt)
    except Exception as e:
        print(e)
        print(date_mail)
        return "Unknown date"


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

    def decode_filename(filename_raw: str) -> str:
        """Декодирует имя файла, если оно содержит не-ASCII символы."""
        decoded_parts = decode_header(filename_raw)
        return ''.join([
            part.decode(encoding or 'utf-8') if isinstance(part, bytes) else part
            for part, encoding in decoded_parts
        ])

    attachments: list[tuple[str, bytes]] = []

    # Проверяем, является ли сообщение многосоставным
    if not email_message.is_multipart():
        return attachments  # Возвращаем пустой список, если нет частей

    # Проходим по всем частям сообщения
    for part in email_message.walk():
        # Проверяем, является ли часть вложением
        content_disposition = part.get("Content-Disposition")
        if content_disposition and "attachment" in content_disposition.lower():
            filename_raw = part.get_filename()  # Получаем имя файла вложения
            payload = part.get_payload(decode=True)  # Получаем содержимое вложения

            if filename_raw and payload:
                filename = decode_filename(filename_raw)
                # Добавляем кортеж (имя файла, содержимое) в список вложений
                attachments.append((filename, payload))

    return attachments


def send_email(
        email_text: str,
        recipient_emails: str | Sequence[str],
        subject: str,
        email_user: str,
        email_pass: str,
        smtp_server: str,
        smtp_port: int,
        email_format: Literal["plain", "html"] = "plain",
) -> None:
    """
    Отправляет email с заданным текстом одному или нескольким получателям.

    Эта функция поддерживает отправку писем как одному адресату, так и списку
    адресатов через SMTP-сервер с использованием TLS-шифрования. Логирует
    успешные отправки и обрабатывает ошибки, возвращая статус выполнения.

    Args:
        email_text: Текст письма
        recipient_emails: Адрес получателя или список адресов получателей
        subject: Тема письма
        email_user: Адрес отправителя (используется для авторизации)
        email_pass: Пароль отправителя для авторизации на SMTP-сервере
        smtp_server: Адрес SMTP-сервера (например, "smtp.gmail.com")
        smtp_port: Порт SMTP-сервера (обычно 587 для TLS)
        email_format: Формат письма ("plain" для обычного текста или "html")

    Returns:
        None
    """
    # Нормализация входных данных: преобразование строки в список, если передан один адрес
    recipients = (
        [recipient_emails]
        if isinstance(recipient_emails, str)
        else recipient_emails
    )

    # Проверка корректности списка получателей
    if not recipients or not all(isinstance(email, str) and email for email in recipients):
        logger.error(f"Некорректный список получателей: {recipients}")
        return

    try:
        # Создание объекта письма с указанным форматом и кодировкой UTF-8
        msg = MIMEText(email_text, email_format, 'utf-8')
        msg['Subject'] = subject

        # # Установка соединения с SMTP-сервером с использованием контекстного менеджера
        # with smtplib.SMTP(smtp_server, smtp_port, timeout=10) as server:
        #     # Активация TLS-шифрования для безопасной передачи данных
        #     server.starttls()
        #     # Аутентификация на сервере с использованием логина и пароля
        #     server.login(email_user, email_pass)
        #     # Отправка письма всем получателям
        #     server.send_message(msg, from_addr=email_user, to_addrs=recipients)

        # Логирование успешной отправки письма
        logger.info(
            f"\nИСХОДЯЩИЙ EMAIL:\n"
            f"{'-' * 80}\n"
            f"Адрес(а) получателя(ей): {', '.join(recipients)}\n"
            f"Тема письма: {subject}\n"
            f"Текст письма:\n{email_text}\n"
            f"{'-' * 80}"
        )

    except smtplib.SMTPException as smtp_error:
        # Обработка специфичных ошибок SMTP (например, неверные учетные данные)
        logger.error(f"Ошибка SMTP при отправке письма: {str(smtp_error)}")
        logger.debug(f"Полная трассировка ошибки:\n{traceback.format_exc()}")
        return
    except Exception as e:
        # Обработка остальных возможных ошибок
        logger.error(f"Неожиданная ошибка при отправке письма: {str(e)}")
        logger.debug(f"Полная трассировка ошибки:\n{traceback.format_exc()}")
        return
