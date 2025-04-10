import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class Logger(logging.Logger):
    """Логгер с поддержкой ротации файлов и вывода в консоль.

    Этот класс расширяет стандартный logging.Logger, добавляя поддержку ротации
    лог-файлов и одновременного вывода сообщений в консоль. Логи записываются
    в файл с указанным максимальным размером, а при превышении размера создаются
    резервные копии.

    Args:
        log_file: Путь к файлу логов (строка или объект Path)
        max_log_size: Максимальный размер файла логов в байтах (по умолчанию 10MB)
        backup_count: Количество резервных копий лог-файлов (по умолчанию 10)
        file_level: Уровень логирования для файла (по умолчанию DEBUG)
        console_level: Уровень логирования для консоли (по умолчанию DEBUG)
    """

    def __init__(
            self,
            log_file: Path | str,
            max_log_size: int = 10 * 1024 * 1024,
            backup_count: int = 10,
            file_level: int = logging.DEBUG,
            console_level: int = logging.DEBUG,
    ) -> None:
        """Инициализирует логгер с ротацией файлов и выводом в консоль."""
        super().__init__("CustomLogger")

        # Проверка корректности входных параметров
        self._validate_log_level(file_level)
        self._validate_log_level(console_level)

        # Преобразуем путь в объект Path для удобной работы с файловой системой
        log_file = Path(log_file)
        # Создаем директорию для файла логов, если она не существует
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Настраиваем формат сообщений
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(log_format, datefmt=date_format)

        # Настраиваем обработчик для записи логов в файл с ротацией
        self.file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_log_size,
            backupCount=backup_count,
            encoding="utf-8"
        )
        self.file_handler.setLevel(file_level)
        self.file_handler.setFormatter(formatter)

        # Настраиваем обработчик для вывода логов в консоль
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(console_level)
        self.console_handler.setFormatter(formatter)

        # Добавляем обработчики к логгеру
        self.addHandler(self.file_handler)
        self.addHandler(self.console_handler)

    def print(self, message: str) -> None:
        """Записывает информационное сообщение в лог.

        Args:
            message (str): Сообщение для логирования.
        """
        self.info(message)

    def _validate_log_level(self, level: int) -> None:
        """Проверяет корректность уровня логирования."""
        valid_levels = {
            logging.DEBUG, logging.INFO, logging.WARNING,
            logging.ERROR, logging.CRITICAL
        }
        if level not in valid_levels:
            raise ValueError(
                f"Уровень логирования должен быть одним из: "
                f"DEBUG ({logging.DEBUG}), INFO ({logging.INFO}), "
                f"WARNING ({logging.WARNING}), ERROR ({logging.ERROR}), "
                f"CRITICAL ({logging.CRITICAL}). "
                f"Передано неверное значение: {level}."
            )


logger = Logger(log_file=Path(__file__).resolve().parent.parent / "logs" / "app.log")
