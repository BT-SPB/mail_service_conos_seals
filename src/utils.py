import re
import json
import base64
import binascii
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Iterable, Literal, Any

from dateutil.parser import parse

from config import config

logger = logging.getLogger(__name__)


class UniqueList(list):
    """Список, сохраняющий порядок, но не допускающий дубликаты."""

    def append(self, item):
        if item not in self:
            super().append(item)


# --- READERS AND WRITERS ---

def write_json(file_path: Path | str, data: Any) -> None:
    """Записывает данные в JSON файл с форматированием.

    Args:
        file_path: Путь к файлу (строка или объект Path)
        data: Данные для записи в JSON формате

    Returns:
        None
    """
    if not data:
        return

    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)  # type: ignore


def read_json(file_path: Path | str) -> dict:
    """Читает данные из JSON файла.

    Args:
        file_path: Путь к файлу (строка или объект Path)

    Returns:
        dict: Содержимое JSON файла в виде словаря
    """
    file_path = Path(file_path)
    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            # Загружаем содержимое JSON файла
            return json.load(file)
    except (json.JSONDecodeError, IOError):
        # В случае ошибок декодирования JSON или отсутствия файла
        # возвращаем пустой словарь как значение по умолчанию
        return {}


def write_text(file_path: Path | str, data: str) -> None:
    """Записывает текстовые данные в файл.

    Args:
        file_path: Путь к файлу (строка или объект Path)
        data: Текст для записи

    Returns:
        None
    """
    if not data:
        return

    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(data, encoding="utf-8")


def read_text(file_path: Path | str) -> str:
    """Читает текстовое содержимое из файла.

    Args:
        file_path: Путь к файлу (строка или объект Path)

    Returns:
        str: Содержимое файла в виде строки
    """
    file_path = Path(file_path)
    return file_path.read_text(encoding="utf-8")


# --- CODERS ---

def file_to_base64(file_path: str | Path) -> str:
    """
    Преобразует файл в строку base64.

    Args:
        file_path: Путь к файлу

    Returns:
        str: Строка, закодированная в base64
    """
    try:
        # Открываем файл в бинарном режиме и читаем его содержимое
        with open(file_path, "rb") as file:
            # Кодируем содержимое в base64 и преобразуем в строку
            base64_encoded = base64.b64encode(file.read()).decode("utf-8")
        return base64_encoded

    except FileNotFoundError:
        logger.exception(f"Файл не найден: {file_path}")
        raise
    except OSError as e:
        logger.exception(f"Ошибка при чтении файла {file_path}: {e}")
        raise


def base64_to_file(base64_string: str, output_path: str | Path) -> None:
    """
    Декодирует строку base64 и сохраняет результат в файл.

    Args:
        base64_string: Строка, закодированная в base64
        output_path: Путь, по которому будет сохранен файл

    Returns:
        None
    """
    try:
        # Декодируем строку base64 в байты
        file_data = base64.b64decode(base64_string)

        # Записываем байты в файл
        with open(output_path, "wb") as file:
            file.write(file_data)

    except (ValueError, binascii.Error) as e:
        logger.exception(f"Неверный формат строки base64: {e}")
        raise
    except OSError as e:
        logger.exception(f"Ошибка при записи файла {output_path}: {e}")
        raise


# --- FILES ---

def sanitize_pathname(
        parent_path: Path | str,
        name: str,
        is_file: bool = True,
        max_length: int = 50,
) -> Path:
    """
    Очищает и нормализует имя файла или директории, обеспечивая его допустимость,
    читаемость и уникальность в рамках файловой системы.

    Функция обрабатывает имя файла/папки следующим образом:
    - Удаляет недопустимые символы и управляющие коды.
    - Приводит имя к безопасному виду.
    - Приводит расширения файлов к нижнему регистру.
    - Обрезает имя до указанной длины, сохраняя расширение для файлов.
    - Избегает конфликтов с зарезервированными именами Windows.
    - Гарантирует уникальность имени в родительской директории.

    Args:
        parent_path: Родительский путь к директории, где будет располагаться файл или папка
        name: Исходное имя файла или директории
        is_file: Флаг, указывающий, является ли имя файлом (True) или директорией (False)
        max_length: Максимально допустимая длина итогового имени, включая расширение (для файлов)

    Returns:
        Path: Безопасный и уникальный путь в родительской директории.
    """
    # Преобразуем родительский путь в объект Path
    parent_path = Path(parent_path)

    # Удаляем недопустимые символы и управляющие коды (0x00–0x1F), заменяя их на пробелы
    clean_name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', " ", name)
    # Заменяем все последовательности пробелов и пробельных символов на одинарное подчёркивание
    clean_name = re.sub(r"\s+", "_", clean_name.strip())

    # Проверяем, что имя после очистки не пустое
    if not clean_name:
        raise ValueError("После очистки имя не может быть пустым")

    # Разделяем имя на основу и расширение в зависимости от типа (файл или директория)
    if is_file:
        # Для файлов извлекаем основу имени и расширение, приводя последнее к нижнему регистру
        path_obj = Path(clean_name)
        stem = path_obj.stem
        ext = path_obj.suffix.lower()
    else:
        # Для директорий: удаляем точки в начале и конце и устанавливаем расширение как пустую строку
        stem = clean_name.strip(".")
        ext = ""

    # Ограничиваем длину имени с учётом максимальной длины и расширения
    if is_file:
        # Для файлов резервируем место под расширение, оставляя минимум 1 символ для основы
        max_stem_len = max(1, max_length - len(ext))
        stem = stem[:max_stem_len]
    else:
        # Для директорий просто обрезаем имя до максимальной длины
        stem = stem[:max_length]

    # Проверка на зарезервированные имена Windows
    reserved_names = {
        'CON', 'PRN', 'AUX', 'NUL',
        *(f'COM{i}' for i in range(1, 10)),
        *(f'LPT{i}' for i in range(1, 10)),
    }
    if stem.upper() in reserved_names:
        # Добавляем подчеркивание в начало имени для избежания конфликтов
        stem = f"_{stem}"

    # Проверка на уникальность имени в родительской директории
    final_name = f"{stem}{ext}"
    counter = 1
    # Проверяем существование пути и добавляем числовой суффикс при необходимости
    while (parent_path / final_name).exists():
        # Добавляем суффикс перед расширением (для файлов) или в конец имени (для директорий)
        final_name = f"{stem}_{counter}{ext}"
        counter += 1

    # Возвращаем финальный путь, объединяя родительский путь с уникальным именем
    return parent_path / final_name


def transfer_files(
        file_paths: Iterable[str | Path] | str | Path,
        destination_folder: str | Path,
        operation: Literal["copy2", "copy", "move"] = "copy2",
        block_transfer: bool = config.block_processed_files_to_output,
) -> None:
    """
    Перемещает или копирует файлы из указанной коллекции путей в папку назначения.

    Args:
        file_paths: Коллекция путей к файлам (список, кортеж, генератор и т.д.) или одиночный путь
        destination_folder: Путь к папке назначения
        operation: Операция для выполнения: "copy2" (по умолчанию), "copy", "move"
        block_transfer: флаг для принудительной блокировки всех операций
    """
    if block_transfer:
        return None

    # Проверяем, является ли file_paths одиночным путем (str или Path)
    if isinstance(file_paths, (str, Path)):
        file_paths = [file_paths]  # Оборачиваем в список

    # Преобразуем destination_folder в Path объект
    destination_folder = Path(destination_folder)
    # Создаем папку назначения, если она не существует
    destination_folder.mkdir(parents=True, exist_ok=True)

    # Получаем метод из shutil через getattr
    file_operation = getattr(shutil, operation)

    # Проходим по всем путям в коллекции
    for file_path in file_paths:
        try:
            # Преобразуем путь в Path объект
            src_path = Path(file_path)
            # Пропускаем, если файла не существует
            if not src_path.is_file():
                # logger.info(f"Файл не существует: {src_path}")
                continue

            # Формируем новый путь
            new_path = destination_folder / src_path.name

            # Выполняем операцию (копирование или перемещение)
            file_operation(src_path, new_path)

        except PermissionError as e:
            logger.error(f"Нет прав доступа: {e} - {file_path}")
        except shutil.Error as e:
            logger.error(f"Ошибка операции ({operation}): {e} - {file_path}")
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e} - {file_path}")


def is_directory_empty(path: Path | str) -> bool:
    """
    Проверяет, является ли указанная директория пустой (не содержит файлов и поддиректорий).

    Args:
        path: Путь к директории для проверки

    Returns:
        bool: True, если директория пуста или не существует, False в противном случае
    """
    # Преобразуем путь в объект Path, если он передан как строка
    path = Path(path)

    # Проверяем, существует ли директория
    if not path.exists() or not path.is_dir():
        return True

    # Пытаемся получить первый элемент содержимого директории
    # Если содержимое есть, next() вернет его, и функция вернет False
    # Если директория пуста, next() вызовет StopIteration, и мы вернем True
    try:
        next(path.iterdir())
        return False
    except StopIteration:
        return True


def parse_datetime(date_string: str) -> datetime | None:
    """
    Парсит строку с датой и временем в объект datetime.

    Функция принимает строку с датой и временем в произвольном формате и пытается преобразовать
    её в объект datetime, используя dateutil.parser с приоритетом дня (формат ДД.ММ.ГГГГ).
    Если строка пустая или парсинг не удался, возвращается None.

    Args:
        date_string: Строка, содержащая дату и время в произвольном формате.

    Returns:
        datetime | None: Объект datetime, если парсинг успешен, иначе None.
    """
    # Проверка на пустую строку или строку, содержащую только пробелы
    if not date_string.strip():
        return None

    try:
        # Парсинг строки в объект datetime с приоритетом дня
        return parse(date_string, dayfirst=True)
    except Exception as e:
        # Логирование ошибки с указанием проблемной строки и причины
        logger.exception("Ошибка парсинга строки '%s' в дату: %s", date_string, e)
        return None


def parse_and_format_datetime(
        date_string: str,
        output_format: str = "%d.%m.%Y %H:%M:%S",
) -> str:
    """
    Форматирует строку с датой и временем в заданный формат.

    Функция принимает строку с датой и временем в произвольном формате, преобразует её
    в объект datetime с помощью функции parse_datetime, а затем возвращает строку,
    отформатированную в указанном формате. При ошибке парсинга или пустой строке
    возвращается пустая строка.

    Args:
        date_string: Строка, содержащая дату и время в произвольном формате.
        output_format: Формат для возвращаемой строки (по умолчанию: "%d.%m.%Y %H:%M:%S").

    Returns:
        str: Отформатированная строка с датой и временем или пустая строка при ошибке.
    """
    # Проверка на пустую строку для предотвращения лишних операций
    if not date_string.strip():
        return ""

    # Использование parse_datetime для получения объекта datetime
    parsed_date = parse_datetime(date_string)
    if parsed_date is None:
        return ""

    try:
        # Форматирование объекта datetime в строку по заданному формату
        return parsed_date.strftime(output_format)
    except ValueError as e:
        # Логирование ошибки форматирования
        logger.exception(
            "Ошибка форматирования даты '%s' в формат '%s': %s",
            parsed_date, output_format, e
        )
        return ""


# if __name__ == "__main__":
#     from ordered_set import OrderedSet
#     from collections import defaultdict
#     a = OrderedSet((1, 2, 3, 3))
#     a.update([2, 3, 4, -10])
#     print(a)
#
#     b = defaultdict(OrderedSet)
#
#     b["1"].update([1, 2, 1, 3, -10])
#
#     print(b)
