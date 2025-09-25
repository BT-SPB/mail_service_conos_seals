from io import StringIO
from pathlib import Path

from dotenv import dotenv_values
from cryptography.fernet import Fernet
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Класс конфигурации для управления настройками приложения.

    Этот класс определяет пути к директориям, настройки email, параметры подключения
    к серверам IMAP/SMTP, списки email для уведомлений, а также методы для инициализации
    директорий и загрузки зашифрованных настроек.

    Для переопределения настроек, заданных по умолчанию, можно создать файл `config.env`
    в директории `CONFIG_DIR`. Значения из этого файла будут иметь приоритет над значениями,
    указанными в коде конфигурации.

    Args:
        CONFIG_DIR (Path): Путь к директории конфигурации, вычисляется как родительская директория текущего файла
        WORK_DIR (Path): Рабочая директория проекта, по умолчанию корень проекта
        IN_FOLDER (Path | None): Входная папка для обработки файлов
        OUT_OCR_FOLDER (Path | None): Папка для результатов OCR
        SUCCESS_FOLDER (Path | None): Папка для успешно обработанных файлов
        ERROR_FOLDER (Path | None): Папка для файлов с ошибками
        LOG_FOLDER (Path | None): Папка для хранения логов
        email_address (str | None): Email-адрес для приема сообщений и отправки уведомлений
        email_password (str | None): Пароль для email-аккаунта
        user_1c (str | None): Имя пользователя для системы 1C
        password_1c (str | None): Пароль для системы 1C
        imap_server (str): Адрес IMAP-сервера
        imap_port (int): Порт IMAP-сервера
        smtp_server (str): Адрес SMTP-сервера
        smtp_port (int): Порт SMTP-сервера
        notification_emails (list[str]): Список email-адресов для уведомлений
        enable_email_notification (bool): # Флаг для блокировки отправки ЛЮБЫХ email-уведомлений
        enable_success_notifications (bool): Флаг отправки уведомлений об успешной обработке
        enable_send_production_data (bool): Флаг для включения отправки номеров пломб и файлов коносаментов в ЦУП
        valid_images (set[str]): Допустимые расширения файлов изображений
        valid_ext (set[str]): Допустимые расширения всех файлов (включая PDF)
    """
    # Путь к директории конфигурации, вычисляется как родительская директория текущего файла
    CONFIG_DIR: Path = Path(__file__).parent
    # Абсолютный путь к корню проекта
    PROJECT_DIR: Path = CONFIG_DIR.parent

    # Загрузка переменных окружения из файла config.env
    model_config = SettingsConfigDict(
        env_file=CONFIG_DIR / "config.env",
        env_file_encoding="utf-8",
        extra="ignore"  # Игнорировать неизвестные переменные в .env
    )

    # Путь к рабочей директории, по умолчанию - корень проекта
    WORK_DIR: Path = PROJECT_DIR / "FILES"

    project_name: str = PROJECT_DIR.name

    # Пути к рабочим директориям, инициализируются как None
    IN_FOLDER: Path | None = None
    OUT_OCR_FOLDER: Path | None = None
    SUCCESS_FOLDER: Path | None = None
    ERROR_FOLDER: Path | None = None
    LOG_FOLDER: Path | None = None

    # Учетные данные для email и 1C
    email_address: str | None = None
    email_password: str | None = None
    user_1c: str | None = None
    password_1c: str | None = None

    # Настройки почтовых серверов
    imap_server: str = "imap.gmail.com"
    imap_port: int = 993
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587

    # Список email-адресов для отправки уведомлений о событиях (успехи, ошибки)
    notification_emails: list[str] = [
        "vpon@sdrzbt.ru",  # Пантелеева Елена
        "oga@sdrzbt.ru",  # Ганенко Олег
        "aby@sdrzbt.ru",  # Быстрова Арина
    ]

    # Глобальный флаг для включения отправки email-уведомлений
    enable_email_notification: bool = True

    # Флаг для включения уведомлений об успешной обработке
    # True - уведомления отправляются при успехе и ошибках
    # False — только при ошибках.
    enable_success_notifications: bool = True

    # Флаг для включения отправки номеров пломб и файлов коносаментов в ЦУП
    enable_send_production_data: bool = False

    # Допустимые расширения файлов для обработки
    valid_images: set = {".png", ".jpg", ".jpeg"}
    valid_ext: set = valid_images | {".pdf"}

    tg_alert_token: str | None = None
    tg_alert_chat_id: str | None = None
    enable_tg_alert_notification: bool = True

    def load_encrypted_settings(self) -> None:
        """Загружает и расшифровывает конфиденциальные настройки из зашифрованного файла.

        Читает ключ шифрования из crypto.key, расшифровывает данные из encrypted.env
        и загружает их как переменные окружения. Обновляет соответствующие поля конфигурации.

        Raises:
            FileNotFoundError: Если отсутствует crypto.key или encrypted.env.
            Exception: Если произошла ошибка при расшифровке данных.
        """
        key_path = self.CONFIG_DIR / "crypto.key"
        enc_path = self.CONFIG_DIR / "encrypted.env"

        try:
            # Чтение ключа и инициализация шифровальщика
            fernet = Fernet(key_path.read_text(encoding="utf-8").strip())
            # Чтение и расшифровка зашифрованного файла
            decrypted_text = fernet.decrypt(enc_path.read_bytes()).decode("utf-8")
            # Парсим .env в словарь: {'OPENAI_API_KEY': '...', ...}
            raw: dict[str, str | None] = dotenv_values(stream=StringIO(decrypted_text))

            # Обновление конфигурационных полей
            self.email_address = raw.get("EMAIL_ADDRESS")
            self.email_password = raw.get("EMAIL_PASSWORD")
            self.user_1c = raw.get("USER_1C")
            self.password_1c = raw.get("PASSWORD_1C")
            self.tg_alert_token = raw.get("TG_ALERT_TOKEN")
            self.tg_alert_chat_id = raw.get("TG_ALERT_CHAT_ID")

        except FileNotFoundError as e:
            print(f"Не найден файл: {e}")
        except Exception as e:
            print(f"Ошибка при расшифровке: {e}")

    def dir_init(self) -> None:
        """Инициализирует рабочие директории.

        Для каждого поля создаётся папка:
        - Если путь указан явно — используется он.
        - Если None — берётся WORK_DIR + вложенные имена.
        Атрибуты обновляются до конечных путей.
        """
        dir_map: dict[str, tuple[str, ...]] = {
            "IN_FOLDER": ("WORKFLOW", "IN"),
            "OUT_OCR_FOLDER": ("WORKFLOW", "OUT_OCR"),
            "LOG_FOLDER": ("WORKFLOW", "logs"),
            "SUCCESS_FOLDER": ("WORKFLOW", "SUCCESS"),
            "ERROR_FOLDER": ("ERROR",),
        }

        for attr, names in dir_map.items():
            current = getattr(self, attr)
            if current is None:
                path = self.WORK_DIR.joinpath(*names)
            else:
                path = Path(current)

            path.mkdir(parents=True, exist_ok=True)
            setattr(self, attr, path)

    def model_post_init(self, context: object = None) -> None:
        """
        Метод вызывается автоматически после инициализации модели Pydantic.
        """
        # Выполнение загрузки зашифрованных настроек
        self.load_encrypted_settings()
        self.dir_init()

    def display_config(self) -> str:
        """Формирует строковое представление конфигурации для отображения.

        Создает читаемое представление всех настроек, исключая конфиденциальные данные,
        такие как пароли. Формат включает заголовок, параметры и завершающую строку.

        Returns:
            str: Строковое представление конфигурации.
        """
        # Формирование заголовка и завершающей линии
        title = "⚙️ Config parameters"
        sep = "─" * 60

        # Исключение конфиденциальных полей из вывода
        config_dict = self.model_dump(
            exclude={
                "user_1c", "password_1c", "email_password",
                "tg_alert_token", "tg_alert_chat_id",
            }
        )

        # Формирование списка строк для каждого параметра
        params = [f"{k}: {v}" for k, v in config_dict.items()]
        # Объединение всех частей в итоговую строку
        return "\n".join([title, sep, *params, sep])


# Создание экземпляра конфига
config = Config()

if __name__ == "__main__":
    print(config.display_config())
