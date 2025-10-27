import html
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from config import config


class TelegramHandler(logging.Handler):
    """
    Лог-хендлер, отправляющий записи в Telegram через Bot API.

    Реализация ориентирована на:
    - Повторное использование TCP-соединений через requests.Session и HTTPAdapter.
    - Умные ретраи для временных ошибок (HTTP 429/5xx).
    - Экранирование HTML и безопасное усечение сообщения до допустимого лимита Telegram.
    - Защиту от ошибок логирования (чтобы ошибки отправки не вызывали рекурсивное логирование).

    Args:
        token (str | None): Telegram bot token. Обычно берётся из config.tg_alert_token.
        chat_id (str | None): ID чата/канала для отправки (config.tg_alert_chat_id).
        project_name (str | None): Краткое имя проекта (используется в заголовке сообщения).
        level (int): Уровень логирования для хендлера (по умолчанию logging.WARNING).
        timeout (float | tuple[float, float] | None): Таймаут для requests.post (connect, read) или float.
        max_retries (int): Количество автоматических попыток при временных ошибках.
        backoff_factor (float): Backoff factor для Retry.
    Returns:
        None
    """
    TELEGRAM_MESSAGE_LIMIT = 4096  # жёсткий лимит Telegram по символам

    def __init__(
            self,
            token: str | None = config.tg_alert_token,
            chat_id: str | None = config.tg_alert_chat_id,
            project_name: str | None = config.project_name,
            level: int = logging.WARNING,
            timeout: float | tuple[float, float] | None = (5.0, 10.0),
            max_retries: int = 2,
            backoff_factor: float = 0.3,
    ) -> None:
        super().__init__(level)

        # Конфигурация базовых параметров
        self.token: str | None = token or None
        self.chat_id: str | None = str(chat_id) if chat_id else None
        self.project_name: str = project_name or "project"
        self.timeout = timeout

        # Если нет токена или chat_id — отключаем отправку, чтобы не ломать приложение.
        if not self.token or not self.chat_id:
            # Конфигурация отсутствует — хендлер остаётся работоспособным, но не отправляет сообщения.
            self._session = None
            return

        # Формируем URL один раз
        self._url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        # Создаём сессию для повторного использования соединений (меньше накладных расходов)
        session = requests.Session()
        # Настраиваем стратегию ретраев: реагируем на 429/5xx ошибки.
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset({"POST"}),
        )

        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        # Заголовки помогут в отладки и идентификации запросов на стороне API
        session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": f"telegram-logger/{self.project_name}",
            }
        )

        self._session: requests.Session = session

    @staticmethod
    def format_message(header: str, text: str) -> str:
        return (
            f"<b>#{header}</b>\n"
            f"<pre>{text}</pre>"
        )

    def emit(self, record: logging.LogRecord) -> None:
        """
        Отправляет форматированную запись лога в Telegram.

        Args:
            record (logging.LogRecord): Объект записи лога, который нужно отправить.

        Returns:
            None
        """
        # Если хендлер отключён (нет конфигурации) — ничего не делаем.
        if not self._session:
            return

        try:
            # Получаем уже отформатированную строку (используется стандартный Formatter)
            log_entry = self.format(record)

            # Экранируем project_name для HTML
            esc_project = html.escape(self.project_name)
            esc_log = html.escape(log_entry)

            # Подстраховка: рассчитываем запас на HTML-теги и дополнительные данные
            estimated_overhead = len(self.format_message(esc_project, "")) + 64
            max_payload_chars = max(0, self.TELEGRAM_MESSAGE_LIMIT - estimated_overhead)

            payload = {
                "chat_id": self.chat_id,
                "text": self.format_message(esc_project, esc_log[:max_payload_chars]),
                "parse_mode": "HTML",
            }

            # Выполняем POST-запрос через сессию (таймаут задаётся при инициализации).
            response = self._session.post(self._url, json=payload, timeout=self.timeout)
            # Если ответ HTTP-кодом не 2xx — будет возбуждено исключение.
            response.raise_for_status()

        except Exception:
            # В случае любой ошибки используем стандартный механизм обработки ошибок Handler,
            # чтобы избежать рекурсивного логирования (например, логирование ошибки
            # отправки не должно снова вызывать TelegramHandler).
            # handleError распечатает стек в stderr, если logging.raiseExceptions == True,
            # и при этом не будет пытаться логировать через getLogger -> избегаем рекурсии.
            self.handleError(record)

    def close(self) -> None:
        """
        Закрывает сессию и вызывает close() базового класса.
        """
        try:
            if getattr(self, "_session", None):
                try:
                    self._session.close()
                except Exception:
                    pass  # Ошибки при закрытии сессии игнорируем
        finally:
            super().close()


def setup_logging(
        log_dir: Path | str = config.LOG_DIR,
        backup_log_dir: Path | str | None = config.BACKUP_LOG_DIR,
        file_log_name: str = config.project_name,
        max_log_size: int = 10 * 1024 * 1024,  # 10 mb
        backup_count: int = 20,
        file_level: int = logging.INFO,
        console_level: int = logging.INFO,
        enable_telegram_notification: bool = config.enable_tg_alert_notification,
):
    """Настройка логирования для всего проекта.

    Создаёт:
      - основной файл логов в log_dir;
      - резервный файл логов в backup_log_dir (если указан);
      - вывод логов в консоль;
      - опциональные уведомления в Telegram.

    Args:
        log_dir: Путь к основной директории для логов (строка или Path)
        backup_log_dir: Путь к резервной директории для логов (строка, Path или None)
        file_log_name: Имя файла логов (без расширения)
        max_log_size: Максимальный размер файла логов в байтах
        backup_count: Количество резервных копий лог-файлов
        file_level: Уровень логирования для файлов
        console_level: Уровень логирования для консоли
        enable_telegram_notification: Включение уведомлений в Telegram
    """
    # Приведение путей к Path
    log_dir = Path(log_dir)
    backup_log_dir = Path(backup_log_dir) if backup_log_dir else None

    # Единый формат вывода логов
    formatter = logging.Formatter(
        fmt="[{asctime}] {levelname:8} {name}:{lineno:<3} | {message}",
        datefmt="%Y-%m-%d %H:%M:%S",
        style="{"
    )

    root_logger = logging.getLogger()
    # Установка минимального уровня для root-логгера.
    root_logger.setLevel(min(file_level, console_level))

    # Очистка старых хендлеров (чтобы избежать дублирования при повторной инициализации)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # --- Консольный хендлер ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # --- Основной файловый хендлер ---
    log_dir.mkdir(parents=True, exist_ok=True)
    main_log_file = log_dir / f"{file_log_name}.log"
    main_file_handler = RotatingFileHandler(
        filename=main_log_file,
        maxBytes=max_log_size,
        backupCount=backup_count,
        encoding="utf-8"
    )
    main_file_handler.setLevel(file_level)
    main_file_handler.setFormatter(formatter)
    root_logger.addHandler(main_file_handler)

    # --- Резервный файловый хендлер (при необходимости) ---
    if backup_log_dir:
        backup_log_dir.mkdir(parents=True, exist_ok=True)
        backup_log_file = backup_log_dir / "app.log"
        backup_file_handler = RotatingFileHandler(
            filename=backup_log_file,
            maxBytes=max_log_size,
            backupCount=backup_count,
            encoding="utf-8"
        )
        backup_file_handler.setLevel(file_level)
        backup_file_handler.setFormatter(formatter)
        root_logger.addHandler(backup_file_handler)

    # --- Telegram-хендлер (если включено) ---
    if enable_telegram_notification:
        tg_handler = TelegramHandler()
        tg_handler.setFormatter(formatter)
        root_logger.addHandler(tg_handler)
