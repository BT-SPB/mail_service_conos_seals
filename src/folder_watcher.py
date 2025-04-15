import time
from pathlib import Path
from typing import Callable

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.logger import logger


class FolderWatcher(FileSystemEventHandler):
    """Класс для мониторинга изменений в заданной папке и выполнения callback-функции."""

    def __init__(
            self,
            folder_path: str | Path,
            callback: Callable[[], None],
            periodic_interval: float | int = 120,  # 2 минуты
            event_delay: float | int = 3
    ):
        """Класс для мониторинга изменений в заданной папке и выполнения callback-функции.

        Этот класс отслеживает изменения в указанной директории (создание, изменение файлов)
        и вызывает callback-функцию по событиям файловой системы с заданной задержкой,
        а так же периодически через определенные интервалы времени.

        Args:
            folder_path (str | Path): Путь к директории, которую необходимо мониторить
            callback (Callable[[], None]): Функция, вызываемая для обработки изменений
            periodic_interval (float | int): Интервал периодического вызова callback (в секундах).
                По умолчанию 120 секунд (2 минуты)
            event_delay (float | int): Задержка перед обработкой событий файловой системы (в секундах).
                По умолчанию 3 секунды
        """
        # Приведение входного пути к объекту Path для унификации
        self.folder_path: Path = Path(folder_path)
        self.callback: Callable[[], None] = callback
        self.periodic_interval: float = float(periodic_interval)
        self.event_delay: float = float(event_delay)

        # Инициализация состояния мониторинга
        self.event_triggered: bool = False  # Флаг, указывающий на наличие необработанного события
        self.last_event_time: float = 0.0  # Время последнего зарегистрированного события
        self.is_processing: bool = False  # Флаг, указывающий, выполняется ли обработка в данный момент
        self.observer: Observer | None = None  # Объект наблюдателя файловой системы. Будет инициализирован в методе monitor

    def on_any_event(self, event) -> None:
        """
        Стандартный метод класса FileSystemEventHandler.
        Метод вызывается при изменении файловой системы в целевой директории (создание, изменение).

        Игнорирует события удаления файлов и изменения временных файлов (с расширениями .tmp, .part, ~),
        чтобы избежать ненужной обработки. При валидном событии устанавливает флаг и фиксирует время.

        Args:
            event (FileSystemEvent): Событие файловой системы, содержащее информацию о типе события
                и пути к файлу.
        """
        # Пропускаем события удаления и временные файлы
        ignored_extensions = {".tmp", ".part", "~"}
        if (
                event.event_type == "deleted"
                or any(event.src_path.endswith(ext) for ext in ignored_extensions)
        ):
            return

        logger.info(f"🔍 Обнаружено изменение: {event.src_path} ({event.event_type})")
        # Устанавливаем флаг события и фиксируем текущее время
        self.event_triggered = True
        self.last_event_time = time.time()

    def monitor(self):
        """Запускает мониторинг директории и обработку изменений.

        Метод настраивает наблюдатель файловой системы (watchdog), запускает его и выполняет
        обработку изменений:
        - По событиям файловой системы (с задержкой event_delay).
        - Периодически (с интервалом periodic_interval), если событий не было.
        Обработка защищена от параллельного выполнения и включает обработку ошибок.

        Raises:
            Exception: При критических ошибках в процессе мониторинга или выполнения callback.
        """
        # Инициализация и запуск наблюдателя
        self.observer = Observer()
        try:
            # Настраиваем наблюдатель для отслеживания событий в директории (без рекурсии)
            self.observer.schedule(self, str(self.folder_path), recursive=False)
            self.observer.start()
            logger.info(f"🔍 Запущен мониторинг директории: {self.folder_path}")
        except Exception as e:
            logger.error(f"⛔ Ошибка запуска наблюдателя: {e}")
            return

        try:
            last_processed_time: float = 0.0
            while True:
                current_time = time.time()

                # Пропускаем цикл, если идет обработка, чтобы избежать параллельного выполнения
                if self.is_processing:
                    time.sleep(.1)  # Небольшая пауза для снижения нагрузки на CPU
                    continue

                # Проверяем условия для запуска обработки:
                # 1. Было событие, и прошло достаточно времени с его фиксации
                # 2. Прошло достаточно времени для периодической обработки
                should_process = (
                                         self.event_triggered and
                                         current_time - self.last_event_time >= self.event_delay
                                 ) or (current_time - last_processed_time >= self.periodic_interval)

                if should_process:
                    logger.info("▶️ Запуск обработки (по событию или таймеру)")
                    self.is_processing = True
                    try:
                        # Выполняем callback для обработки изменений
                        self.callback()
                    except Exception as e:
                        # Логируем ошибки callback, чтобы они не прерывали мониторинг
                        logger.error(f"⛔ Ошибка в callback: {type(e).__name__}: {e}")
                    finally:
                        # Сбрасываем флаги и обновляем время обработки
                        self.event_triggered = False
                        self.is_processing = False
                        last_processed_time = current_time

                # Небольшая пауза для предотвращения чрезмерной нагрузки на CPU
                time.sleep(.1)

        except KeyboardInterrupt:
            logger.info("✔️ Мониторинг остановлен пользователем")
        except Exception as e:
            logger.error(f"⛔ Критическая ошибка в мониторинге: {type(e).__name__}: {e}")
        finally:
            # Гарантируем корректное завершение наблюдателя
            if self.observer:
                self.observer.stop()
                self.observer.join()
            logger.info("✔️ Мониторинг директории завершен")
