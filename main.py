import time
import email
import imaplib
import traceback
from pathlib import Path

from email.message import Message
from email.utils import parseaddr

import config
from src.utils import (
    write_json,
    connect_to_imap,
    get_unseen_messages,
    decode_subject,
    extract_text_content,
    extract_html_content,
    extract_attachments,
    send_email
)

OUTPUT_DIR = Path("OUTPUT")


def main(
        email_user: str,
        email_pass: str,
        imap_server: str,
        imap_port: int = 993
):
    """Проверяет и обрабатывает новые письма в IMAP ящике.

    Args:
        email_user: Адрес электронной почты пользователя.
        email_pass: Пароль от почтового ящика.
        imap_server: Адрес IMAP сервера.
        imap_port: Порт IMAP сервера (по умолчанию 993).

    Returns:
        list: Список результатов обработки писем (пока пустой, для будущей расширяемости).
    """

    result: list = []

    # Подключение к серверу
    mail: imaplib.IMAP4_SSL | None = connect_to_imap(email_user, email_pass, imap_server, imap_port)
    if not mail:
        print('Нет соединения')
        return result

    try:
        # Получение списка непрочитанных сообщений
        message_ids: list[bytes] = get_unseen_messages(mail)
        if not message_ids:
            print("Новых писем нет")
            return result

        print(f"Найдено новых писем: {len(message_ids)}")

        # Последовательная обработка каждого письма
        for msg_id in message_ids:
            msg_id_str = msg_id.decode('utf-8')
            # Получение письма без отметки как прочитанное
            status, msg_data = mail.fetch(msg_id_str, 'BODY.PEEK[]')
            if status != 'OK':
                continue

            # Парсинг письма
            email_message: Message = email.message_from_bytes(msg_data[0][1])

            # Извлечение html части
            # html_content: Optional[str] = extract_html_content(email_message)

            # Собираем основную информацию о письме
            email_info = {
                "subject": decode_subject(email_message.get("Subject", "")),
                "sender": parseaddr(email_message.get("From", ""))[1] or "Неизвестный отправитель",
                "date": email_message.get("Date", "Дата неизвестна"),
                "text_content": extract_text_content(email_message) or "No text content"
            }

            # Извлечение и обработка вложений
            attachments = extract_attachments(email_message)

            if attachments:
                # Проходим по всем вложениям
                for filename, content in attachments:
                    # Формирование путей для сохранения
                    file_path = OUTPUT_DIR / Path(filename).stem / Path(filename).name
                    output_path = file_path.parent
                    # Создание директории, если она не существует
                    output_path.mkdir(parents=True, exist_ok=True)

                    # Сохранение метаданных письма
                    write_json(output_path / "info.json", email_info)

                    try:
                        # Записываем содержимое в файл
                        file_path.write_bytes(content)
                        print(f"Сохранен файл: {file_path}")
                    except OSError as e:
                        print(f"Ошибка при сохранении файла {file_path}: {e}")

            # Отметить как прочитанное
            mail.store(msg_id_str, '+FLAGS', '\\Seen')

            # # Отправка ответного письма
            # if html_content:
            #     email_text = "\n+\n".join(map(format_csv_to_table, email_data.rate_tables_csv))
            #     send_email(email_text=email_text,
            #                email_format='html',
            #                recipient_email=email_data.sender_address,
            #                subject=f'Автоответ от {email_user}',
            #                email_user=email_user,
            #                email_pass=email_pass,
            #                )

        return result

    except Exception:
        print(traceback.format_exc())
        return []

    finally:
        print("Закрытие соединения...")
        mail.close()
        mail.logout()


if __name__ == "__main__":

    IMAP_SERVER: str = "imap.gmail.com"

    while True:
        result = main(email_user=config.EMAIL_ADDRESS,
                      email_pass=config.EMAIL_PASSWORD,
                      imap_server=IMAP_SERVER)
        print(result)
        time.sleep(5)
