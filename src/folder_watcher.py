import time
import traceback
from pathlib import Path
from typing import Callable

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.logger import logger


class FolderWatcher(FileSystemEventHandler):
    """Класс для мониторинга изменений в заданной папке и выполнения callback-функции.

    Этот класс позволяет отслеживать изменения в целевой директории (создание и изменение файлов)
    и вызывать заданную функцию-обработчик (callback). Обработка может быть запущена:
    - по событиям файловой системы, после того как активность прекратилась на заданное время
    - принудительно через определенные промежутки времени, даже если событий не было
        (для повышения устойчивости)
    """

    def __init__(
            self,
            folder_path: str | Path,
            callback: Callable[[], None],
            forced_timeout: float | int = 300,
            event_delay: float | int = 3
    ):
        """Инициализация наблюдателя за папкой.

        Args:
            folder_path: Путь к директории, за которой необходимо следить
            callback: Функция, вызываемая при обнаружении изменений
            forced_timeout: Периодический интервал вызова callback, даже если нет новых событий (в секундах)
            event_delay: Задержка перед вызовом callback после последнего события (в секундах).

                При возникновении изменений в папке (создание или изменение файлов) запуск callback
                не происходит немедленно. Вместо этого система ждёт, чтобы с момента последнего события
                прошло не менее `event_delay` секунд.

                Это предотвращает преждевременную обработку файлов, которые ещё могут быть не до конца
                скопированы, загружены или созданы. Если в директории продолжают появляться новые события,
                то отсчёт времени откладывается, и callback будет вызван только тогда, когда активность
                прекратится как минимум на `event_delay` секунд.
        """
        self.folder_path: Path = Path(folder_path)
        self.callback: Callable[[], None] = callback
        self.forced_timeout: float = float(forced_timeout)
        self.event_delay: float = float(event_delay)

        # Инициализация состояния мониторинга
        self.event_detected: bool = False  # Флаг, указывающий на наличие необработанного события
        self.last_event_time: float = 0.0  # Время последнего события
        self.is_processing: bool = False  # Флаг, указывающий, выполняется ли обработка в данный момент
        self.observer: Observer | None = None  # Объект наблюдателя файловой системы. Будет инициализирован в методе monitor

    def on_any_event(self, event) -> None:
        """
        Стандартный метод класса FileSystemEventHandler.
        Метод вызывается при изменении файловой системы в целевой директории (создание, изменение).

        Игнорирует события удаления файлов и изменения временных файлов (с расширениями .tmp, .part, ~),
        чтобы избежать ненужной обработки. При валидном событии устанавливает флаг и фиксирует время.

        Args:
            event: Событие от watchdog, содержащее тип события и путь к файлу.
        """
        # Пропускаем события удаления и временные файлы
        ignored_extensions = {".tmp", ".part", "~"}
        if (
                event.event_type == "deleted"
                or any(event.src_path.endswith(ext) for ext in ignored_extensions)
        ):
            return

        logger.debug(f"📁 Обнаружено изменение: {event.src_path} ({event.event_type})")
        # Устанавливаем флаг события и фиксируем текущее время
        self.event_detected = True
        self.last_event_time = time.time()

    def stop(self) -> None:
        """Останавливает мониторинг директории и завершает наблюдатель.

        Безопасно останавливает наблюдатель файловой системы, дожидается завершения его работы
        и сбрасывает состояние. Если наблюдатель уже остановлен, метод не выполняет действий.
        """
        if not self.observer:
            logger.debug("🔔 Мониторинг директории УЖЕ остановлен")
            return

        try:
            self.observer.stop()
            self.observer.join()
            logger.info("🔔 Мониторинг директории остановлен")
        except Exception as e:
            logger.error(f"⛔ Ошибка при остановке наблюдателя: {e}\n{traceback.format_exc()}")
        finally:
            self.observer = None

    def monitor(self):
        """Основной цикл мониторинга и обработки изменений.

        - Запускает наблюдатель за указанной директорией.
        - Обрабатывает события с задержкой или через заданный период.
        - Обеспечивает безопасность при многократном срабатывании событий и ошибках.

        Raises:
            Exception: При критических ошибках во время мониторинга.
        """
        # Инициализация и запуск наблюдателя
        self.observer = Observer()
        try:
            # Настраиваем наблюдатель для отслеживания событий в директории (без рекурсии)
            self.observer.schedule(self, str(self.folder_path), recursive=False)
            self.observer.start()
            logger.info(
                f"📁 Мониторинг директории (принудительный таймаут {self.forced_timeout:.0f} сек): {self.folder_path}")
        except Exception as e:
            logger.error(f"⛔ Ошибка запуска наблюдателя: {e}\n{traceback.format_exc()}")
            self.stop()
            return

        try:
            last_callback_time: float = 0.0
            while True:
                # Небольшая пауза для предотвращения чрезмерной нагрузки на CPU
                time.sleep(.1)
                current_time = time.time()

                # Пропускаем итерацию, если уже выполняется callback
                if self.is_processing:
                    continue

                # Условие запуска callback:
                # 1. Было событие и прошла задержка event_delay
                # 2. Или прошёл интервал forced_timeout с последнего запуска
                event_ready = (
                        self.event_detected and
                        (current_time - self.last_event_time >= self.event_delay)
                )

                timeout_ready = (
                        not self.event_detected and
                        current_time - last_callback_time >= self.forced_timeout
                )

                if event_ready or timeout_ready:
                    if event_ready:
                        logger.info("▶️ Обработка директории по событию")
                    elif timeout_ready:
                        logger.info("🕒 Принудительная обработка директории по таймеру")

                    self.is_processing = True
                    try:
                        # Выполняем callback для обработки изменений
                        self.callback()
                    except Exception as e:
                        # Логируем ошибки callback, чтобы они не прерывали мониторинг
                        logger.error(f"⛔ Ошибка в callback: {e}\n{traceback.format_exc()}")
                    finally:
                        # Сбрасываем флаги и обновляем время обработки
                        self.event_detected = False
                        self.is_processing = False
                        last_callback_time = current_time

        except Exception as e:
            logger.error(f"⛔ Критическая ошибка в мониторинге: {e}\n{traceback.format_exc()}")
