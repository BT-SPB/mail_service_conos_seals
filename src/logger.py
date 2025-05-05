import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class Logger(logging.Logger):
    """Логгер с поддержкой ротации файлов, двух логов и вывода в консоль.

    Этот класс расширяет стандартный logging.Logger, добавляя поддержку ротации
    лог-файлов, записи основного лога и опционального подробного лога (DEBUG и выше),
    а также вывода сообщений в консоль. Основной лог хранится в указанной директории,
    подробный — в подпапке 'detailed', если включен и основной лог не на уровне DEBUG.
    Логи записываются в файл с указанным максимальным размером, а при превышении
    размера создаются резервные копии.

    Args:
        log_dir: Путь к директории для логов (строка или объект Path)
        log_name: Имя файла логов (без расширения)
        max_log_size: Максимальный размер файла логов в байтах (по умолчанию 10MB)
        backup_count: Количество резервных копий лог-файлов (по умолчанию 10)
        main_file_level: Уровень логирования для основного файла (по умолчанию INFO)
        console_level: Уровень логирования для консоли (по умолчанию INFO)
        enable_detailed_logging: Флаг для включения подробного лога (по умолчанию True)
        detailed_dir_name: Имя подпапки для подробного лога (по умолчанию 'detailed')
    """

    def __init__(
            self,
            log_dir: Path | str,
            log_name: str,
            max_log_size: int = 10 * 1024 * 1024,
            backup_count: int = 10,
            main_file_level: int = logging.INFO,
            console_level: int = logging.INFO,
            enable_detailed_logging: bool = True,
            detailed_dir_name: str = "detailed",
    ) -> None:
        """Инициализирует логгер с ротацией файлов и выводом в консоль."""
        super().__init__(__name__)

        # Проверка корректности входных параметров
        self._validate_log_level(main_file_level)
        self._validate_log_level(console_level)

        # Преобразуем путь в объект Path
        log_dir = Path(log_dir)
        # Создаем основную директорию, если она не существует
        log_dir.parent.mkdir(parents=True, exist_ok=True)

        # Настраиваем формат сообщений
        log_format = '%(asctime)s [%(levelname)7s] - %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(log_format, datefmt=date_format)

        # Настраиваем обработчик для вывода логов в консоль
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(console_level)
        self.console_handler.setFormatter(formatter)
        self.addHandler(self.console_handler)

        # Настраиваем обработчик для основного лога
        main_log_file = log_dir / f"{log_name}.log"
        self.main_file_handler = RotatingFileHandler(
            filename=main_log_file,
            maxBytes=max_log_size,
            backupCount=backup_count,
            encoding="utf-8"
        )
        self.main_file_handler.setLevel(main_file_level)
        self.main_file_handler.setFormatter(formatter)
        self.addHandler(self.main_file_handler)

        # Настраиваем обработчик для подробного лога, если:
        # 1. Включен флаг enable_detailed_logging
        # 2. Уровень основного лога выше DEBUG
        if enable_detailed_logging and main_file_level > logging.DEBUG:
            detailed_dir = log_dir / detailed_dir_name
            detailed_dir.mkdir(parents=True, exist_ok=True)
            detailed_log_file = detailed_dir / f"{log_name}.log"
            self.detailed_file_handler = RotatingFileHandler(
                filename=detailed_log_file,
                maxBytes=max_log_size,
                backupCount=backup_count,
                encoding="utf-8"
            )
            self.detailed_file_handler.setLevel(logging.DEBUG)
            self.detailed_file_handler.setFormatter(formatter)
            self.addHandler(self.detailed_file_handler)

    def print(self, message: str) -> None:
        """Записывает информационное сообщение в лог.

        Args:
            message (str): Сообщение для логирования.
        """
        self.info(message)

    @staticmethod
    def _validate_log_level(level: int) -> None:
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


# Глобальная переменная для хранения экземпляра логгера
logger: Logger | None = None


def init_logger(*args, **kwargs) -> None:
    """Инициализирует глобальный логгер.

    Создаёт экземпляр класса Logger и присваивает его глобальной переменной logger.
    Должна вызываться только один раз из config.py после определения пути для логов.
    Передаёт log_file и дополнительные параметры в конструктор Logger.
    """
    global logger
    if logger is None:
        logger = Logger(*args, **kwargs)


def get_logger() -> Logger:
    """Получает инициализированный глобальный логгер.

    Используется только в config.py для безопасного доступа к логгеру.
    В других модулях следует использовать прямой импорт logger.

    Returns:
        Logger: Инициализированный экземпляр логгера.

    Raises:
        RuntimeError: Если логгер не был инициализирован.
    """
    if logger is None:
        raise RuntimeError("Логгер не инициализирован. Сначала вызовите init_logger().")
    return logger
