import re
import json
from pathlib import Path
from typing import Any


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


# --- FILES ---

def sanitize_pathname(
        name: str,
        is_file: bool = True,
        parent_dir: str | Path = ".",
        max_length: int = 50,
) -> str:
    """
    Универсальная функция для очистки имен файлов и директорий.

    Args:
        name (str): Исходное имя файла или директории
        is_file (bool): True для файлов, False для директорий
        parent_dir (str): Родительская директория для проверки конфликтов

    Returns:
        str: Очищенное уникальное имя
    """
    # Заменяем все недопустимые символы на пробелы
    clean_name = re.sub(r'[<>:"/\\|?*]|\x00-\x1F', " ", name).strip()
    # Заменяем пробелы на "_"
    clean_name = re.sub(r"\s+", "_", clean_name)

    # Дополнительная обработка в зависимости от типа (файл или директория)
    if is_file:
        path = Path(clean_name)
        name = path.stem  # Имя без расширения
        ext = path.suffix  # Расширение с точкой
        clean_name = name + ext.lower()
    else:
        # Для директорий убираем точки в начале и конце
        clean_name = clean_name.strip('.')

    parent_path = Path(parent_dir)

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

    # Проверка на зарезервированные имена (Windows)
    reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                      'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3',
                      'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
    if clean_name.upper().split('.')[0] in reserved_names:
        clean_name = f"_{clean_name}"

    # Проверка на уникальность в родительской директории
    final_name = clean_name
    counter = 1
    while (parent_path / final_name).exists():
        if is_file:
            base_name, ext = Path(clean_name).stem, Path(clean_name).suffix
            final_name = f"{base_name}_{counter}{ext}"
        else:
            final_name = f"{clean_name}_{counter}"
        counter += 1

    return final_name
