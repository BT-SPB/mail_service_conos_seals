import re
import json
from pathlib import Path
from typing import Any, Iterable, Literal
import base64
import shutil

from src.logger import logger


# --- READERS AND WRITERS ---

def write_json(file_path: Path | str, data: Any) -> None:
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
        json.dump(data, file, indent=4, ensure_ascii=False)


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
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    except IOError as e:
        raise IOError(f"Ошибка при чтении файла {file_path}: {str(e)}")


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

    except base64.binascii.Error:
        raise ValueError("Неверный формат строки base64")
    except IOError as e:
        raise IOError(f"Ошибка при записи файла {output_path}: {str(e)}")


# --- FILES ---

def sanitize_pathname(
        name: str,
        is_file: bool = True,
        parent_dir: str | Path = ".",
        max_length: int = 50,
) -> Path:
    """Очищает имена файлов или директорий, обеспечивая уникальность и совместимость.

    Args:
        name: Исходное имя файла или директории
        is_file: Указывает, является ли путь файлом (True) или директорией (False)
        parent_dir: Родительская директория для проверки конфликтов
        max_length: Максимальная длина результирующего имени

    Returns:
        Path: Очищенное и уникальное имя в виде объекта Path.
    """
    # Замена недопустимых символов и удаление лишних пробелов
    clean_name = re.sub(r'[<>:"/\\|?*]|\x00-\x1F', " ", name).strip()
    clean_name = re.sub(r"\s+", "_", clean_name)
    if not clean_name:
        raise ValueError("После очистки имя не может быть пустым")

    # Дополнительная обработка в зависимости от типа (файл или директория)
    if is_file:
        path = Path(clean_name)
        # приводим расширение файла к нижнему регистру
        clean_name = path.stem + path.suffix.lower()
    else:
        # Для директорий убираем точки в начале и конце
        clean_name = clean_name.strip('.')

    # Ограничиваем длину имени
    if len(clean_name) > max_length:
        if is_file:
            path = Path(clean_name)
            name = path.stem  # Имя без расширения
            ext = path.suffix  # Расширение с точкой
            max_name_length = max_length - len(ext)
            clean_name = name[:max_name_length] + ext
        else:
            clean_name = clean_name[:max_length]

    # Проверка на зарезервированные имена Windows
    reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                      'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3',
                      'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
    if clean_name.upper().split('.')[0] in reserved_names:
        clean_name = f"_{clean_name}"

    # Обеспечение уникальности имени в директории
    parent_path = Path(parent_dir)
    final_name = clean_name
    counter = 1
    while (parent_path / final_name).exists():
        if is_file:
            path = Path(clean_name)
            final_name = f"{path.stem}_{counter}{path.suffix}"
        else:
            final_name = f"{clean_name}_{counter}"
        counter += 1

    return Path(final_name)


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
                # logger.print(f"Файл не существует: {src_path}")
                continue

            # Формируем новый путь
            new_path = destination_folder / src_path.name

            # Выполняем операцию (копирование или перемещение)
            file_operation(src_path, new_path)

        except PermissionError as e:
            logger.print(f"Нет прав доступа: {e} - {file_path}")
        except shutil.Error as e:
            logger.print(f"Ошибка операции ({operation}): {e} - {file_path}")
        except Exception as e:
            logger.print(f"Неизвестная ошибка: {e} - {file_path}")