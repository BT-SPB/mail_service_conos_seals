import logging
from typing import Callable
from dataclasses import dataclass

import numpy as np
from scipy.optimize import linear_sum_assignment
from rapidfuzz import fuzz, process
from rapidfuzz.distance import Levenshtein

from src.utils_tsup import tsup_http_request
from src.models.document_model import StructuredDocument

logger = logging.getLogger(__name__)


@dataclass
class BillOfLadingStrategy:
    """Класс для хранения стратегии обработки номера коносамента."""
    condition: Callable[[str], bool]
    transform: Callable[[str], str]
    description: str


def fetch_transaction_numbers(
        document: StructuredDocument,
) -> None:
    """
    Извлекает номера транзакций на основе номера коносамента, применяя различные стратегии обработки.

    Описание:
        Функция принимает объект StructuredDocument, содержащий номер коносамента,
        и пытается получить номера транзакций, применяя заданные стратегии преобразования номера.
        Стратегии применяются последовательно, пока не будет найден действительный номер транзакции
        или не будут исчерпаны все стратегии.

    Args:
        document: Объект StructuredDocument с полем bill_of_lading, содержащим номер коносамента.

    Returns:
        None: Функция изменяет входной объект document, добавляя номера транзакций
              в поле transaction_numbers и, при необходимости, обновляя bill_of_lading.
    """
    # Определение стратегий обработки номера коносамента
    strategies = [
        BillOfLadingStrategy(
            condition=lambda x: True,
            transform=lambda x: x,
            description="Попытка использовать оригинальный номер коносамента без изменений."
        ),
        BillOfLadingStrategy(
            condition=lambda x: x.endswith("SRV"),
            transform=lambda x: x.removesuffix("SRV"),
            description="Удаление суффикса 'SRV' из номера коносамента, если он присутствует."
        ),
    ]

    # Сохраняем исходный номер коносамента для обработки
    original_bill = document.bill_of_lading

    # Перебираем стратегии обработки
    for strategy in strategies:
        # Проверяем, применима ли текущая стратегия
        if not strategy.condition(original_bill):
            # Если условие не выполнено, переходим к следующей стратегии
            continue

        # Применяем трансформацию к номеру коносамента
        candidate_bill = strategy.transform(original_bill).strip()

        # Если после трансформации получена пустая строка, переходим к следующей стратегии
        if not candidate_bill:
            continue

        # Выполняем HTTP-запрос для получения номеров транзакций
        transaction_numbers = tsup_http_request(
            "TransactionNumberFromBillOfLading", candidate_bill
        )

        # Проверяем, что получен непустой список номеров транзакций
        if transaction_numbers and isinstance(transaction_numbers, list):
            # Обновляем поля документа: номер коносамента и номера транзакций
            document.bill_of_lading = candidate_bill
            document.transaction_numbers = transaction_numbers
            # Прерываем цикл, так как найдены действительные номера транзакций
            break


@dataclass
class ContainerMatch:
    """Результат сопоставления одного OCR-кода с кодом из базы."""
    ocr_code: str
    db_code: str | None = None
    similarity: float | None = None


# def weighted_scorer(s1: str, s2: str, score_cutoff: float | None = 0) -> float:
#     # задаём свои веса для операций
#     return Levenshtein.normalized_similarity(s1, s2, weights=(2, 2, 1), score_cutoff=score_cutoff)

def match_containers(
        ocr_containers: list[str] | tuple[str],
        db_containers: list[str] | set[str] | tuple[str],
        threshold: float = 0.9
) -> list[ContainerMatch]:
    """
    Сопоставляет номера контейнеров, извлечённые OCR, с уникальными кодами из базы.

    Алгоритм:
      1. Для каждого OCR-кода берём до top_k наиболее похожих кодов из db_containers
         с учётом score_cutoff (threshold * 100).
      2. Собираем набор кандидатов (OCR_i, DB_j, similarity).
      3. Используем точный Hungarian algorithm (scipy.optimize.linear_sum_assignment),
       который минимизирует суммарную "стоимость" (1 - similarity) и
       возвращает глобально оптимальное паросочетание;

    Notes:
        - Возвращается список ContainerMatch той же длины и в том же порядке,
          что и ocr_containers: для каждого элемента либо лучший (уникальный)
          db_code, либо None.
        - similarity возвращается в виде float в диапазоне [0.0, 1.0].

    Args:
        ocr_containers: список номеров, распознанных OCR.
        db_containers: эталонные номера из 1С.
        threshold: минимальная схожесть для зачёта совпадения (0-1).

    Returns:
        Список ContainerMatch для всех OCR-кодов, в том же порядке, что и ocr_containers.
    """
    # Валидация входа и простые случаи
    if not (0 < threshold <= 1):
        raise ValueError("threshold должен быть в диапазоне (0, 1].")

    # Быстрый выход для пустых входов
    if ocr_containers == 0:
        return []
    if db_containers == 0:
        # Нет эталонных кодов — все None
        return [ContainerMatch(ocr) for ocr in ocr_containers]

    # Подготовим результирующий массив (по умолчанию None)
    results: list[ContainerMatch] = [ContainerMatch(ocr) for ocr in ocr_containers]

    # --- Шаг 1. Поиск точных совпадений ---
    ocr_remaining: list[tuple[int, str]] = []
    db_remaining_set: set[str] = set(db_containers)

    for idx, ocr_code in enumerate(ocr_containers):
        # Если OCR-код ровно совпадает с кодом из базы, фиксируем это сразу.
        if ocr_code in db_remaining_set:
            results[idx].db_code = ocr_code
            results[idx].similarity = 1.0
            # Убираем код из дальнейшего рассмотрения, чтобы он был уникальным
            db_remaining_set.remove(ocr_code)
        else:
            ocr_remaining.append((idx, ocr_code))

    # Если все совпали — можно завершить
    if not ocr_remaining or not db_remaining_set:
        return results

    # --- Шаг 2. Fuzzy сопоставление для оставшихся ---
    db_remaining = list(db_remaining_set)
    # Переводим threshold в формат rapidfuzz (0-100)
    threshold_percentage = threshold * 100

    n_ocr = len(ocr_remaining)
    n_db = len(db_remaining)

    # Создаем матрицу схожести n_ocr x n_db
    similarity_matrix = np.zeros((n_ocr, n_db), dtype=np.float32)

    # Для каждой оставшейся OCR строки запросим top_k кандидатов с cutoff
    # и заполним соответствующие ячейки матрицы.
    for i_local, (_, ocr_code) in enumerate(ocr_remaining):
        extracted: list[tuple[str, float, int]] = process.extract(
            query=ocr_code,
            choices=db_remaining,
            scorer=fuzz.ratio,
            # scorer=weighted_scorer,
            limit=7,
            score_cutoff=threshold_percentage,
            # score_cutoff=threshold,
        )

        for _, raw_score, j_db in extracted:
            similarity_matrix[i_local, j_db] = raw_score

    # Если нет совпадений вообще - завершаем
    if not np.any(similarity_matrix):
        return results

    # --- Шаг 3. Hungarian algorithm (минимизация стоимости) ---
    # rapidfuzz даёт 0..100, переводим в 0..1
    similarity_matrix /= 100

    # Hungarian algorithm минимизирует суммарную "стоимость".
    # Переведём схожесть в стоимость: cost = 1 - similarity (чем выше sim, тем ниже cost).
    cost_matrix = 1 - similarity_matrix

    # Решаем задачу минимизации (Hungarian algorithm) -
    # найдёт пары (i,j) для min(n_ocr, n_db) элементов
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    # --- Шаг 4. Формирование результатов ---
    # Обрабатываем найденные пары: если similarity >= threshold — фиксируем,
    # иначе — пропускаем (оставляем None).
    for i_local, j_db in zip(row_ind, col_ind):
        sim = float(similarity_matrix[i_local, j_db])
        if sim < threshold:
            continue

        i_global, _ = ocr_remaining[i_local]
        # Присваиваем уникальный db_code
        results[i_global].db_code = db_remaining[j_db]
        results[i_global].similarity = sim

    return results


def correct_container_numbers(
        document: StructuredDocument,
        db_containers: set[str],
) -> None:
    """

    Args:
        document: Объект StructuredDocument с полем bill_of_lading, содержащим номер коносамента.
        db_containers: эталонные номера из 1С.

    Returns:
        None: Функция изменяет входной объект document, добавляя номера транзакций
              в поле transaction_numbers и, при необходимости, обновляя bill_of_lading.
    """

    ocr_containers: list[str] = [cont.container for cont in document.containers]

    matched_containers: list[ContainerMatch] = match_containers(ocr_containers, db_containers)

    for container_doc, container_match in zip(document.containers, matched_containers):
        if container_match.db_code and container_match.db_code != container_doc.container:
            note_message = (
                f"🟡 Распознанный номер контейнера {container_doc.container} "
                f"был заменен на {container_match.db_code}. "
                f"Similarity: {container_match.similarity:.4f}"
            )

            document.notes.add(note_message)
            logger.info(note_message)
            container_doc.container = container_match.db_code


# if __name__ == "__main__":
#     # Пример использования:
#     ocr_list = ["PKSU001486"]
#     db_list = ["PKSU0001486", "MSCU1234567"]
#
#     # ocr_list = ["PKSU00001486"]  # "TEMU7654321"
#     # db_list = ["PKSU0001486", "MSCU1234567", "TEMU7654329", "MSCU123fg67", "sdfdsdfd", "fger4wjeikwejq", "sdfwdefqw",
#     #            "MSCU12ert4567", "MSCU12ert4567", "MSCU12ert4567"]
#
#     matches = match_containers(ocr_list, db_list)
#     try:
#         for match in matches:
#             print(match)
#     except:
#         pass
