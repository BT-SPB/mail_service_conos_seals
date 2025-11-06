import re
import time
import random
import smtplib
import chardet
import logging
import mimetypes
from pathlib import Path
from typing import Literal, Sequence
from zoneinfo import ZoneInfo

from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate, make_msgid, parsedate_to_datetime
from email.header import Header, decode_header

from config import config
from src.utils import write_text

logger = logging.getLogger(__name__)

AttachmentsType = Path | str | Sequence[Path | str] | None


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
        error_message = "Unknown date"
        logger.exception(f"{error_message}: {e}")
        return error_message


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


def decode_subject(subject: str | None) -> str:
    """Декодирует тему письма из закодированного формата"""
    if not subject:
        return "(Без темы)"
    decoded: list[tuple[bytes | str, str | None]] = decode_header(subject)
    subject_text: str = ""
    for text, encoding in decoded:
        if isinstance(text, bytes):
            subject_text += text.decode(encoding or 'utf-8', errors='ignore')
        else:
            subject_text += text
    return subject_text.strip()


def extract_text_content(email_message: Message) -> str | None:
    """
    Извлекает текстовую часть из email-сообщения.

    Args:
        email_message: Объект email-сообщения для обработки.

    Returns:
        str | None: Текстовая часть письма или None, если текст не найден.
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
                    return body.decode(encoding, errors='ignore').strip()
    else:
        # Обрабатываем случай, если сообщение не многосоставное
        body = email_message.get_payload(decode=True)
        if body:
            encoding = detect_encoding(body)
            return body.decode(encoding, errors='ignore').strip()
    return None


def extract_html_content(email_message: Message) -> str | None:
    """
    Извлекает HTML-часть из email-сообщения.

    Args:
        email_message: Объект email-сообщения для обработки.

    Returns:
        str | None: HTML-часть письма или None, если HTML не найден.
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


def decode_filename(filename_raw: str) -> str:
    """Декодирует имя файла, если оно содержит не-ASCII символы."""
    decoded_parts = decode_header(filename_raw)
    return ''.join([
        part.decode(encoding or 'utf-8') if isinstance(part, bytes) else part
        for part, encoding in decoded_parts
    ])


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
            filename_raw = part.get_filename()  # Получаем имя файла вложения
            payload = part.get_payload(decode=True)  # Получаем содержимое вложения

            if filename_raw and payload:
                filename = decode_filename(filename_raw)
                # Добавляем кортеж (имя файла, содержимое) в список вложений
                attachments.append((filename, payload))

    # Сортируем вложения по имени файла
    attachments.sort(key=lambda x: x[0])
    return attachments


# def extract_attachments(email_message: Message) -> list[tuple[str, bytes]]:
#     """
#     Извлекает вложения из email-сообщения, включая вложенные письма.
#
#     Args:
#         email_message: Объект email-сообщения для обработки.
#
#     Returns:
#         list[tuple[str, bytes]]: Список кортежей, содержащих имя файла и его содержимое в байтах.
#     """
#     attachments: list[tuple[str, bytes]] = []
#
#     def extract(msg: Message) -> None:
#         """Внутренняя рекурсивная функция для извлечения вложений."""
#         if not msg.is_multipart():
#             return
#
#         for part in msg.walk():
#             content_type = part.get_content_type()
#             content_disposition = part.get("Content-Disposition", "")
#
#             # Вложение-файл
#             if content_disposition and "attachment" in content_disposition.lower():
#                 filename_raw = part.get_filename()  # Получаем имя файла вложения
#                 payload = part.get_payload(decode=True)  # Получаем содержимое вложения
#
#                 if filename_raw and payload:
#                     filename = decode_filename(filename_raw)
#                     # Добавляем кортеж (имя файла, содержимое) в список вложений
#                     attachments.append((filename, payload))
#
#             # Вложенное письмо (message/rfc822)
#             elif content_type == "message/rfc822":
#                 # Вложенное письмо может быть вложено в виде списка
#                 nested_payload = part.get_payload()
#                 if isinstance(nested_payload, list):
#                     for sub_msg in nested_payload:
#                         extract(sub_msg)
#                 elif isinstance(nested_payload, Message):
#                     extract(nested_payload)
#
#     extract(email_message)
#     attachments.sort(key=lambda x: x[0])
#     return attachments


def _normalize_recipients(recipient_emails: str | Sequence[str]) -> list[str]:
    """Нормализует получателей в список уникальных адресов.

    Функция понимает:
      - одиночную строку с одним адресом,
      - строку со списком адресов (через запятую, точку с запятой и/или пробелы),
      - последовательность строк (list/tuple и т.п.).

    Адреса приводятся к виду без окружающих пробелов, пустые элементы исключаются.
    Порядок сохраняется, дубликаты удаляются.

    Args:
        recipient_emails: Строка (один/несколько email) или последовательность email-строк.

    Returns:
        list[str]: Список нормализованных email-адресов (может быть пустым).
    """
    items: list[str]
    if isinstance(recipient_emails, str):
        # Разбиваем по запятым/точкам с запятой/пробельным символам.
        # Это назад-совместимо с передачей одиночной строки (без разделителей).
        raw = re.split(r"[;,\s]+", recipient_emails.strip())
        items = [x.strip() for x in raw if x and x.strip()]
    elif isinstance(recipient_emails, Sequence):
        # Приводим все элементы к строкам и чистим пробелы.
        items = [str(x).strip() for x in recipient_emails if x and str(x).strip()]
    else:
        # Теоретически сюда не попадём из-за аннотаций, но добавим безопасный фолбэк.
        return []

    # Удаление дубликатов с сохранением порядка.
    return list(dict.fromkeys(items))


def _normalize_attachments(attachments: AttachmentsType) -> list[Path]:
    """Нормализует attachments в список Path, существующих и указывающих на файлы.

    - Поддерживает None, одиночный Path/str и последовательности Path/str.
    - Пути расширяются по домашнему каталогу (expanduser) и приводятся к абсолютным (resolve).
    - Дубликаты удаляются (по нормализованному абсолютному пути).
    - Элементы, которые не являются файлами, отбрасываются без поднятия исключений.

    Args:
        attachments: None, Path/str или последовательность Path/str.

    Returns:
        list[Path]: Список валидных путей к существующим файлам.
    """
    if not attachments:
        return []

    # Преобразуем одиночные Path/str в список
    if isinstance(attachments, (str, Path)):
        attachments_list = [attachments]
    else:
        attachments_list = list(attachments)

    normalized: list[Path] = []
    for item in attachments_list:
        p = Path(item)
        if not p.is_file():
            # Не включаем несуществующие элементы — это обработается логированием в основном коде.
            continue
        normalized.append(p)

    # Удаление дубликатов с сохранением порядка.
    return list(dict.fromkeys(normalized))


def _make_attachment_part(file_path: Path) -> MIMEBase:
    """Создаёт MIME-часть для вложения с корректным типом и кодировкой.

    Пытается корректно обработать наиболее частые типы:
    - text/* -> MIMEText (utf-8 c «replace» при ошибках декодирования),
    - image/* -> MIMEImage,
    - audio/* -> MIMEAudio,
    - application/* -> MIMEApplication,
    - прочие типы -> MIMEBase с base64.

    Args:
        file_path: Путь к файлу вложения (существующий файл).

    Returns:
        MIMEBase: Готовая MIME-часть для присоединения к сообщению.
    """
    # Определяем тип содержимого по расширению/сигнатурам.
    ctype, encoding = mimetypes.guess_type(file_path.as_posix())
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)

    # Читаем данные единым блоком: для формирования MIME-части это стандартно.
    with file_path.open("rb") as f:
        data = f.read()

    # Для каждого мейнтипа выбираем подходящий MIME-класс.
    if maintype == "text":
        # Пытаемся декодировать как utf-8, при ошибке — «заменяем» проблемные символы.
        try:
            text = data.decode("utf-8")
        except Exception:
            text = data.decode("utf-8", errors="replace")
        part = MIMEText(text, _subtype=subtype, _charset="utf-8")

    elif maintype == "image":
        # Изображения передаём «как есть».
        part = MIMEImage(data, _subtype=subtype)

    elif maintype == "audio":
        # Аудио так же «как есть».
        part = MIMEAudio(data, _subtype=subtype)

    elif maintype == "application":
        # Большинство бинарных форматов: PDF/DOCX/ZIP и т.п.
        part = MIMEApplication(data, _subtype=subtype)

    else:
        # Нечто экзотическое: используем базовый контейнер с base64.
        part = MIMEBase(maintype, _subtype=subtype)
        part.set_payload(data)
        encoders.encode_base64(part)

    # Корректный заголовок Content-Disposition с названием файла.
    part.add_header(
        "Content-Disposition",
        f'attachment',
        filename=str(Header(file_path.name, "utf-8")),
    )
    return part


def send_email(
        email_text: str,
        recipient_emails: str | Sequence[str],
        subject: str,
        email_user: str = config.email_address,
        email_pass: str = config.email_password,
        smtp_server: str = config.smtp_server,
        smtp_port: int = config.smtp_port,
        email_format: Literal["plain", "html"] = "plain",
        attachments: AttachmentsType = None,
        timeout: int = 30,
        max_retries: int = 4,
        retry_delay: int = 10,
        trace_folder: Path | None = None,
) -> None:
    """Отправляет email с текстом и (опционально) вложениями.

    Функция формирует корректную MIME-структуру:
    - внешний контейнер multipart/mixed (для вложений),
    - внутри multipart/alternative (для plain/html тела).

    Поддерживаются два метода отправки:
    - "smtp": SMTP с STARTTLS, явные EHLO до/после TLS, аутентификация login,
    - "gmail_api": отправка через Gmail API по ранее полученному токену.

    Args:
        email_text: Текст письма (plain или html согласно параметру `email_format`).
        recipient_emails: Адрес(а) получателя(ей) — строка или последовательность строк.
        subject: Тема письма.
        email_user: Адрес отправителя (используется для авторизации и заголовка From).
        email_pass: Пароль/апп-пароль отправителя для SMTP-аутентификации.
        smtp_server: Адрес SMTP-сервера (например, "smtp.gmail.com").
        smtp_port: Порт SMTP-сервера (обычно 587 для STARTTLS).
        email_format: Формат тела письма: "plain" или "html".
        attachments: Путь/список путей к файлам для вложения (Path или str).
        timeout: Таймаут в секундах для сетевых операций SMTP.
        max_retries: Количество попыток отправки.
        retry_delay: Задержка между попытками (секунды).
        trace_folder: Папка для трейсинга текущего документа.

    Returns:
        None
    """
    # ──────────────────────────────────────────────────────────────────────────────
    # Шаг 1 — нормализация и валидация входных данных
    # ──────────────────────────────────────────────────────────────────────────────
    # нормализация получателей
    recipients = _normalize_recipients(recipient_emails)

    # Валидация получателей: список не пуст и все строки непусты.
    if not recipients:
        logger.error("Некорректные адреса получателей: %r", recipient_emails)
        return

    # Валидация формата тела письма на случай вызова без статической типизации.
    if email_format not in {"plain", "html"}:
        logger.warning("Неизвестный email_format=%r. Используем 'plain' по умолчанию.", email_format)
        email_format = "plain"

    def format_email_log(title: str) -> str:
        """Функция форматирования информации для логирования."""
        log_data = (
            f"{title}\n"
            f"{'-' * 60}\n"
            f"Получатели: {', '.join(recipients)}\n"
            f"Тема: {subject}\n"
            f"Вложения: {attachments}\n"
            f"Текст:\n{email_text[:500]}\n"
            f"{'-' * 60}"
        )

        if trace_folder and config.enable_tracing:
            write_text(trace_folder / "email_data.txt", log_data)

        return log_data

    # Глобальная блокировка отправки
    if not config.enable_email_notification:
        logger.info(format_email_log(f"📧 Отправка email ЗАБЛОКИРОВАНА настройкой `enable_email_notification`"))
        return

    # ──────────────────────────────────────────────────────────────────────────────
    # Шаг 2 — подготовка MIME-сообщения (multipart/mixed -> multipart/alternative)
    # ──────────────────────────────────────────────────────────────────────────────
    # Внешняя оболочка — 'mixed' (для вложений), внутри — 'alternative' (plain/html)
    msg = MIMEMultipart("mixed")
    msg["From"] = email_user
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    # Явно проставляем дату и Message-ID для корректного отображения в клиентах.
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    # Многоформатное тело письма (альтернатива plain/html).
    alternative = MIMEMultipart("alternative")
    alternative.attach(MIMEText(email_text, email_format, "utf-8"))
    msg.attach(alternative)

    # ──────────────────────────────────────────────────────────────────────────────
    # Шаг 3 — добавление вложений (если есть)
    # ──────────────────────────────────────────────────────────────────────────────
    attachments_list = _normalize_attachments(attachments)
    # Логируем отсутствующие вложения отдельно (если пользователь передал non-empty attachments)
    if attachments and not attachments_list:
        # Пользователь явно что-то передал, но ничего не нашлось.
        logger.warning("⚠️ Все указанные вложения не найдены или недоступны: %r", attachments)

    for file_path in attachments_list:
        try:
            part = _make_attachment_part(file_path)
            msg.attach(part)
        except Exception as e:
            # Не прерываем обработку всех вложений — логируем проблему и продолжаем.
            logger.exception("Ошибка при обработке вложения %s: %s", file_path, e)

    # ──────────────────────────────────────────────────────────────────────────────
    # Шаг 4 — отправка письма с повторными попытками
    # ──────────────────────────────────────────────────────────────────────────────
    for attempt in range(1, max_retries + 1):
        # Отправка через SMTP с STARTTLS.
        try:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=timeout) as server:
                # Корректный протокольный цикл: приветствие -> TLS -> повторное приветствие.
                server.ehlo()
                server.starttls()
                server.ehlo()

                server.login(email_user, email_pass)
                server.send_message(msg, from_addr=email_user, to_addrs=recipients)

            # Логирование успешной отправки (включает сводную информацию).
            logger.info(format_email_log(f"📧 Email успешно отправлен (попытка {attempt}/{max_retries})"))
            return  # Успешная отправка → выходим из функции

        except smtplib.SMTPAuthenticationError as auth_err:
            logger.exception("⛔ Ошибка авторизации SMTP для %s: %s", email_user, auth_err)
            return  # Бесполезно повторять при ошибке авторизации

        except (smtplib.SMTPException, TimeoutError, OSError) as smtp_err:
            logger.warning("⚠️ Ошибка SMTP (попытка %d/%d): %s", attempt, max_retries, smtp_err)

        except Exception as e:
            logger.exception("⛔ Неожиданная ошибка (попытка %d/%d): %s", attempt, max_retries, e)
            # Здесь можно return, если считаем, что retry не нужен
            # return

        # Если сюда дошли — значит был сбой, но есть шанс повторить
        if attempt < max_retries:
            # Экспоненциальная задержка с джиттером
            delay = retry_delay * (2 ** (attempt - 1))
            delay += random.uniform(0, 4) * attempt  # джиттер
            logger.info("⏳ Повторная попытка через %.1f секунд...", delay)
            time.sleep(delay)
        else:
            logger.error("❌ Все %d попыток отправки письма исчерпаны.", max_retries)
