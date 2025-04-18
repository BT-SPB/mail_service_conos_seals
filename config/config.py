import os
from pathlib import Path
from io import StringIO

from dotenv import load_dotenv
from cryptography.fernet import Fernet
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.logger import logger


class Config(BaseSettings):
    """Класс конфигурации для управления настройками приложения.

    Этот класс определяет пути к директориям, настройки email, параметры подключения
    к серверам IMAP/SMTP, списки email для уведомлений, а также методы для инициализации
    директорий и загрузки зашифрованных настроек.

    Args:
        CONFIG_DIR (Path): Путь к директории конфигурации, вычисляется автоматически.
        WORK_DIR (Path): Рабочая директория проекта, по умолчанию корень проекта.
        IN_FOLDER (Path | None): Входная папка для обработки файлов.
        OUT_OCR_FOLDER (Path | None): Папка для результатов OCR.
        SUCCESS_FOLDER (Path | None): Папка для успешно обработанных файлов.
        ERROR_FOLDER (Path | None): Папка для файлов с ошибками.
        EMAIL_ADDRESS (str | None): Email-адрес для отправки уведомлений.
        EMAIL_PASSWORD (str | None): Пароль для email-аккаунта.
        USER_1C (str | None): Имя пользователя для системы 1C.
        PASSWORD_1C (str | None): Пароль для системы 1C.
        imap_server (str): Адрес IMAP-сервера.
        imap_port (int): Порт IMAP-сервера.
        smtp_server (str): Адрес SMTP-сервера.
        smtp_port (int): Порт SMTP-сервера.
        notification_emails (list[str]): Список email-адресов для уведомлений.
        enable_success_notifications (bool): Флаг отправки уведомлений об успешной обработке.
        valid_images (set[str]): Допустимые расширения файлов изображений.
        valid_ext (set[str]): Допустимые расширения всех файлов (включая PDF).

    Returns:
        None
    """
    # Путь к директории конфигурации, вычисляется как родительская директория текущего файла
    CONFIG_DIR: Path = Path(__file__).resolve().parent

    # Загрузка переменных окружения из файла config.env
    model_config = SettingsConfigDict(
        env_file=CONFIG_DIR / "config.env",
        env_file_encoding="utf-8",
        extra="ignore"  # Игнорировать неизвестные переменные в .env
    )

    # Путь к рабочей директории, по умолчанию - корень проекта
    WORK_DIR: Path = Path(__file__).resolve().parent.parent

    # Пути к рабочим директориям, инициализируются как None
    IN_FOLDER: Path | None = None
    OUT_OCR_FOLDER: Path | None = None
    SUCCESS_FOLDER: Path | None = None
    ERROR_FOLDER: Path | None = None

    # Учетные данные для email и 1C
    EMAIL_ADDRESS: str | None = None
    EMAIL_PASSWORD: str | None = None
    USER_1C: str | None = None
    PASSWORD_1C: str | None = None

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

    # Флаг для включения уведомлений об успешной обработке
    # True - уведомления отправляются при успехе и ошибках
    # False — только при ошибках.
    enable_success_notifications: bool = True

    # Флаг для включения отправки номеров пломб и файлов коносаментов в ЦУП
    enable_send_production_data: bool = False

    # Допустимые расширения файлов для обработки
    valid_images: set = {".png", ".jpg", ".jpeg"}
    valid_ext: set = valid_images | {".pdf"}

    def setup_directories(self) -> None:
        """Создает необходимые директории, если они отсутствуют. """
        # Список директорий для создания
        directories = [
            self.IN_FOLDER,
            self.OUT_OCR_FOLDER,
            self.SUCCESS_FOLDER,
            self.ERROR_FOLDER
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def load_encrypted_settings(self) -> None:
        """Загружает и расшифровывает конфиденциальные настройки из зашифрованного файла.

        Читает ключ шифрования из crypto.key, расшифровывает данные из encrypted.env
        и загружает их как переменные окружения. Обновляет соответствующие поля конфигурации.

        Raises:
            FileNotFoundError: Если отсутствует crypto.key или encrypted.env.
            Exception: Если произошла ошибка при расшифровке данных.
        """
        try:
            # Чтение ключа шифрования
            key_path = self.CONFIG_DIR / "crypto.key"
            with open(key_path, mode="r", encoding="utf-8") as key_file:
                crypto_key = key_file.read().strip()
        except FileNotFoundError as e:
            logger.error("Не найден файл crypto.key")
            logger.error(e)
            return

        # Чтение и расшифровка зашифрованного файла
        try:
            # Инициализация шифровальщика
            fernet = Fernet(crypto_key)
            with open(self.CONFIG_DIR / "encrypted.env", "rb") as encrypted_file:
                encrypted_data = encrypted_file.read()

            # Расшифровка данных и преобразование в строку
            decrypted_bytes = fernet.decrypt(encrypted_data)
            decrypted_text = decrypted_bytes.decode("utf-8")

            # Загрузка переменных окружения из расшифрованных данных
            string_stream = StringIO(decrypted_text)
            load_dotenv(stream=string_stream)

            # Обновление конфигурационных полей
            self.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
            self.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
            self.USER_1C = os.getenv("USER_1C")
            self.PASSWORD_1C = os.getenv("PASSWORD_1C")

        except FileNotFoundError:
            logger.print("Не найден файл encrypted.env")
        except Exception as e:
            logger.print(f"Ошибка при расшифровке: {e}")

    def model_post_init(self, __context) -> None:
        """Инициализирует конфигурацию после создания экземпляра класса.

        Устанавливает пути к рабочим директориям, загружает зашифрованные настройки
        и создает необходимые директории. Выполняется автоматически после инициализации
        объекта благодаря механизму pydantic.

        Args:
            __context: Контекст инициализации, передаваемый pydantic (не используется).

        Примечание: Этот метод вызывается автоматически после создания экземпляра.
        """
        # Установка путей к рабочим директориям относительно WORK_DIR
        self.IN_FOLDER = self.WORK_DIR / "WORKFLOW" / "IN"
        self.OUT_OCR_FOLDER = self.WORK_DIR / "WORKFLOW" / "OUT_OCR"
        self.SUCCESS_FOLDER = self.WORK_DIR / "WORKFLOW" / "SUCCESS"
        self.ERROR_FOLDER = self.WORK_DIR / "ERROR"

        # Выполнение загрузки зашифрованных настроек и создание директорий
        self.load_encrypted_settings()
        self.setup_directories()
        logger.info("Инициализация конфига завершена")

    def display_config(self) -> str:
        """Формирует строковое представление конфигурации для отображения.

        Создает читаемое представление всех настроек, исключая конфиденциальные данные,
        такие как пароли. Формат включает заголовок, параметры и завершающую строку.

        Returns:
            str: Строковое представление конфигурации.
        """
        # Формирование заголовка и завершающей линии
        title = "  CONFIG PARAMS  ".center(80, "=")
        end = "=" * 80

        # Исключение конфиденциальных полей из вывода
        config_dict = self.model_dump(
            exclude={"EMAIL_PASSWORD", "PASSWORD_1C"}
        )

        # Формирование списка строк для каждого параметра
        params = [f"{k}: {v}" for k, v in config_dict.items()]
        # Объединение всех частей в итоговую строку
        return "\n".join((title, *params, end))


CONFIG = Config()

if __name__ == "__main__":
    # print(CONFIG)
    print(CONFIG.display_config())
