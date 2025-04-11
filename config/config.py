import os
from pathlib import Path
from io import StringIO

from dotenv import load_dotenv
from cryptography.fernet import Fernet
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.logger import logger


class Config(BaseSettings):
    CONFIG_DIR: Path = Path(__file__).resolve().parent

    # Подгружаем настройки из config.env
    model_config = SettingsConfigDict(
        env_file=CONFIG_DIR / "config.env",
        env_file_encoding="utf-8"
    )

    # Путь к рабочей директории (можно переопределить в .env)
    # По умолчанию - корень проекта
    WORK_DIR: Path = Path(__file__).resolve().parent.parent

    IN_FOLDER: Path | None = None
    OUT_OCR_FOLDER: Path | None = None
    SUCCESS_FOLDER: Path | None = None
    ERROR_FOLDER: Path | None = None

    EMAIL_ADDRESS: str | None = None
    EMAIL_PASSWORD: str | None = None

    USER_1C: str | None = None
    PASSWORD_1C: str | None = None

    imap_server: str = "imap.gmail.com"
    imap_port: int = 993
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587

    # File extensions
    valid_images: set = {".png", ".jpg", ".jpeg"}
    valid_ext: set = valid_images | {".pdf"}

    def setup_directories(self) -> None:
        """Создает необходимые директории, если они отсутствуют. """
        for directory in [self.IN_FOLDER, self.OUT_OCR_FOLDER, self.SUCCESS_FOLDER, self.ERROR_FOLDER]:
            directory.mkdir(parents=True, exist_ok=True)

    def load_encrypted_settings(self) -> None:
        """Загружает и расшифровывает переменные окружения из зашифрованного файла.

        Читает ключ шифрования из crypto.key, расшифровывает данные из encrypted.env
        и загружает переменные окружения в конфигурацию.

        Raises:
            FileNotFoundError: Если один из файлов (crypto.key или encrypted.env) не найден.
            Exception: Если произошла ошибка при расшифровке данных.
        """
        # Чтение ключа шифрования из файла
        try:
            with open(self.CONFIG_DIR / "crypto.key", mode="r", encoding="utf-8") as key_file:
                crypto_key = key_file.read().strip()
        except FileNotFoundError as e:
            logger.print(e)
            logger.print("Не найден файл crypto.key")
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

            # Установка значений в конфигурацию
            self.EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
            self.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

            self.USER_1C = os.getenv("USER_1C")
            self.PASSWORD_1C = os.getenv("PASSWORD_1C")

        except FileNotFoundError:
            logger.print("Не найден файл encrypted.env")
        except Exception as e:
            logger.print(f"Ошибка при расшифровке: {e}")

    def model_post_init(self, __context) -> None:
        """Инициализирует конфигурацию после создания экземпляра."""
        # Установка путей к рабочим директориям
        self.IN_FOLDER = self.WORK_DIR / "IN"
        self.OUT_OCR_FOLDER = self.WORK_DIR / "OUT_OCR"
        self.SUCCESS_FOLDER = self.WORK_DIR / "SUCCESS"
        self.ERROR_FOLDER = self.WORK_DIR / "ERROR"

        # Загрузка зашифрованных настроек и создание директорий
        self.load_encrypted_settings()
        self.setup_directories()

    def display_config(self) -> str:
        """Формирует читаемое строковое представление настроек конфигурации."""
        title = "  CONFIG PARAMS  ".center(80, "=")
        end = "=" * 80

        config_dict = self.model_dump(
            exclude={"EMAIL_PASSWORD", "PASSWORD_1C"}
        )

        params = [f"{k}: {v}" for k, v in config_dict.items()]
        return "\n".join((title, *params, end))


CONFIG = Config()

if __name__ == "__main__":
    # print(CONFIG)
    print(CONFIG.display_config())
