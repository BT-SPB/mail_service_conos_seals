import re
import json
import base64
import binascii
import shutil
from pathlib import Path
from typing import Iterable, Literal

from src.logger import logger


# --- READERS AND WRITERS ---

def write_json(file_path: Path | str, data: any) -> None:
    """Записывает данные в JSON файл с форматированием.

    Args:
        file_path: Путь к файлу (строка или объект Path)
        data: Данные для записи в JSON формате

    Returns:
        None
    """
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
            data = json.load(file)
    except (json.JSONDecodeError, IOError):
        # Если файл пустой или поврежден, используем пустой словарь
        data = {}

    return data


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
        with open(output_path, 'wb') as file:
            file.write(file_data)

    except (ValueError, binascii.Error) as e:
        logger.exception(f"Неверный формат строки base64: {e}")
        raise
    except OSError as e:
        logger.exception(f"Ошибка при записи файла {output_path}: {e}")
        raise


# --- FILES ---

def sanitize_pathname(
        path: Path | str,
        is_file: bool = True,
        max_length: int = 50,
) -> Path:
    """
    Очищает и нормализует имя файла или директории, обеспечивая его допустимость,
    читаемость и уникальность в рамках файловой системы.

    Функция удаляет недопустимые символы, приводит имя к допустимой форме,
    обрезает его до заданной длины, избегает зарезервированных имен Windows
    и гарантирует уникальность имени в пределах директории.

    Args:
        path: Исходный путь к файлу или директории (строка или объект Path)
        is_file: Флаг, указывающий, является ли путь файлом (True) или директорией (False)
        max_length: Максимальная допустимая длина имени (включая расширение для файлов)

    Returns:
        Path: Объект Path с безопасным и уникальным именем.
    """
    # Преобразуем входной путь в объект Path для унифицированной обработки
    path = Path(path)
    # Извлекаем имя файла или директории из пути
    original_name = path.name

    # Удаляем недопустимые символы и управляющие коды (0x00–0x1F), заменяя их на пробелы
    clean_name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', " ", original_name)
    # Заменяем все последовательности пробелов и пробельных символов на одинарное подчёркивание
    clean_name = re.sub(r"\s+", "_", clean_name.strip())

    # Проверяем, что имя после очистки не пустое
    if not clean_name:
        raise ValueError("После очистки имя не может быть пустым")

    # Заменяем имя в оригинальном пути на очищенное
    path = path.with_name(clean_name)

    # Обрабатываем имя в зависимости от типа пути (файл или директория)
    if is_file:
        # Для файлов: имя без расширения (stem) + расширение в нижнем регистре
        clean_name = f"{path.stem}{path.suffix.lower()}"
    else:
        # Для директорий: удаляем точки в начале и конце
        clean_name = path.name.strip('.')

    # Обрезаем имя, если оно превышает заданный лимит по длине
    if len(clean_name) > max_length:
        if is_file:
            # Для файлов сохраняем расширение, урезая только имя
            stem, ext = Path(clean_name).stem, Path(clean_name).suffix
            # Если расширение файла занимает всю допустимую длину,
            # мы всё равно должны оставить хотя бы 1 символ в имени.
            max_stem_len = max(1, max_length - len(ext))
            clean_name = f"{stem[:max_stem_len]}{ext}"
        else:
            # Для директорий просто обрезаем имя до максимальной длины
            clean_name = clean_name[:max_length]

    # Проверка на зарезервированные имена Windows
    reserved_names = {
        'CON', 'PRN', 'AUX', 'NUL',
        *(f'COM{i}' for i in range(1, 10)),
        *(f'LPT{i}' for i in range(1, 10)),
    }
    if clean_name.split('.')[0].upper() in reserved_names:
        # Добавляем подчеркивание в начало имени для избежания конфликта
        clean_name = f"_{clean_name}"

    # Проверка на уникальность имени в родительской директории
    parent_path = path.parent
    final_name = clean_name
    counter = 1
    while (parent_path / final_name).exists():
        # Если имя уже существует, добавляем суффикс с номером
        if is_file:
            # Для файлов сохраняем расширение и добавляем суффикс к имени
            stem, ext = Path(clean_name).stem, Path(clean_name).suffix
            final_name = f"{stem}_{counter}{ext}"
        else:
            # Для директорий добавляем суффикс к имени
            final_name = f"{clean_name}_{counter}"
        counter += 1

    # Возвращаем финальный путь с уникальным именем
    return parent_path / final_name


def transfer_files(
        file_paths: Iterable[str | Path] | str | Path,
        destination_folder: str | Path,
        operation: Literal["copy2", "copy", "move"] = "copy2"
) -> None:
    """
    Перемещает или копирует файлы из указанной коллекции путей в папку назначения.

    Args:
        file_paths: Коллекция путей к файлам (список, кортеж, генератор и т.д.) или одиночный путь
        destination_folder: Путь к папке назначения
        operation: Операция для выполнения: "copy2" (по умолчанию), "copy", "move"
    """
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
            # Проверяем, существует ли исходный файл
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
