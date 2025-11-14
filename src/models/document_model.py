import re
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, Any, Iterable

from pydantic import Field, field_validator
# from ordered_set import OrderedSet

from src.utils import file_to_base64
from src.models.mixin import StorableModel, OrderedSetType
from src.models.enums import DocType

logger = logging.getLogger(__name__)

# Константы оформления
RED_HEX: str = "#FF6666"
GREEN_HEX: str = "#39A739"

# Паттерны
BAN_OPK_PATTERN = re.compile(r"\bзапрет\s+опк\b", re.IGNORECASE)


@dataclass
class FieldConfig:
    """Конфигурация отображения / трансформации поля для отчёта и экспорта.

    Attributes:
        name: Имя поля в модели.
        transform: Опциональная функция для преобразования значения поля перед отображением/экспортом.
        always_display: Всегда показывать поле в отчёте, даже если значение пустое/False.
        html_tag: Функция обёртки значения в HTML (например, сделать жирным). Если None — не применять.
    """
    name: str
    transform: Callable[[Any], Any] | None = None
    always_display: bool = False
    html_tag: Callable[[str], str] | None = lambda x: f"<b>{x}</b>"


def format_sent_status_report(is_sent: bool) -> str:
    """Форматирует булев флаг отправки в HTML-строку с цветом и подписью.

    Args:
        is_sent: Флаг того, что данные отправлены.

    Returns:
        str: HTML-строка, например "<span style='color: #39A739;'>Успешно!</span>"
    """
    color = GREEN_HEX if is_sent else RED_HEX
    text = "Успешно!" if is_sent else "Не отправлено!"
    return f"<span style='color: {color};'>{text}</span>"


# def format_containers_report(containers: list["Container"]) -> str:
#     """Форматирует список контейнеров в многострочный блок для отчёта.
#
#     Args:
#         containers: Список экземпляров Container.
#
#     Returns:
#         str: Пустая строка если контейнеров нет, иначе строка, начинающаяся с новой строки,
#              в которой каждый контейнер представлен в виде собственной строки (с отступом).
#     """
#     if not containers:
#         return ""
#
#     # Формируем единый блок: отдельная строка на каждый контейнер
#     return "\n" + "\n".join(cont.format_report() for cont in containers)


class Container(StorableModel):
    """Pydantic-модель контейнера.

    Содержит номер контейнера, список пломб, дату выгрузки и примечание.
    Валидатор note оставляет только пометку 'Запрет ОПК' при совпадении с шаблоном,
    в противном случае примечание отбрасывается.
    """
    container: str = Field(
        description="Номер контейнера."
    )
    seals: OrderedSetType[str] = Field(
        default_factory=OrderedSetType,
        description="Номера пломб.",
        json_schema_extra={"tsup_title": "ИмпМорскаяПеревозкаНомерПломбы"},
    )
    upload_datetime: str | None = Field(
        default=None,
        description="Дата и время выгрузки / помещения на склад.",
        json_schema_extra={"tsup_title": "ИмпМорскаяПеревозкаДатаВыгрузкиКонтейнера"},
    )
    note: str | None = Field(
        default=None,
        description="Значение из столбца 'примечания' в таблице ДУ."
    )

    @field_validator("note", mode="before")
    @classmethod
    def restrict_note(cls, note: str | None) -> str | None:
        """Валидатор поля note.

        Оставляет значение 'Запрет ОПК' если в тексте найдено совпадение шаблона BAN_OPK_PATTERN.
        Если note пустое или не содержит шаблона, возвращается None (т.е. другие примечания игнорируются).

        Args:
            note: Входное значение.

        Returns:
            str | None: 'Запрет ОПК' или None.
        """
        if not note:
            return None

        return "Запрет ОПК" if BAN_OPK_PATTERN.search(note) else None

    def format_report(self) -> str:
        """Формирует одну строку HTML-отчёта для контейнера.

        Пример результата:
            "<b>LXHU1234567</b>: [SEAL1, SEAL2] - 2025-10-07 12:00 - <b>Запрет ОПК</b>"

        Returns:
            str: Отформатированная строка с информацией о контейнере.
        """
        # Формируем базовую строку с номером контейнера
        result = f"<b>{self.container}</b>"

        # Добавляем пломбы, если они есть
        if self.seals:
            result += f" - [{', '.join(str(seal) for seal in self.seals)}]"

        # Добавляем дату выгрузки, если указана
        if self.upload_datetime:
            result += f" - {self.upload_datetime}"

        # Добавляем примечание, если есть
        if self.note:
            result += f" - <b>{self.note}</b>"

        return result

    @staticmethod
    def format_containers_section(containers: Iterable["Container"] | None) -> str:
        """Форматирует список контейнеров в текстовый формат.

        Args:
            containers: Итерабельный объект с контейнерами или None.

        Returns:
            str: Отформатированная строка с описаниями контейнеров или пустая строка,
                если контейнеры отсутствуют.
        """
        # Проверяем, существует ли итерабельный объект и содержит ли он элементы.
        if not containers:
            return ""

        # Фильтруем только экземпляры класса Container
        containers = [
            cont for cont in containers
            if isinstance(cont, Container)
        ]

        # Формируем строку с отступом в 2 пробела и маркером '•' для каждого контейнера.
        return "\n".join(
            f"  • {cont.format_report()}"
            for cont in containers
        )

    def to_tsup_dict(self) -> dict[str, Any]:
        """Готовит словарь данных контейнера для отправки в ЦУП.

        Использует json_schema_extra.tsup_title, если он указан в поле модели, иначе — имя поля.
        Пропускает пустые поля.

        Returns:
            dict[str, Any]: Словарь, ключи — tsup_title или имя поля, и значения из модели
        """
        # Поля для экспорта в ЦУП
        field_names: tuple[str, ...] = (
            "container",
            "seals",
            "upload_datetime"
        )

        result: dict[str, Any] = {}

        # Перебираем поля и добавляем непустые значения в словарь
        for field_name in field_names:
            value = getattr(self, field_name, None)
            if not value:
                continue

            if isinstance(value, OrderedSetType):
                value = list(value)

            # Получаем schema_extra и tsup_title, используем имя поля как запасной вариант
            schema_extra = self.__class__.model_fields[field_name].json_schema_extra
            tsup_title = schema_extra.get("tsup_title") or field_name if schema_extra else field_name
            result[tsup_title] = value

        return result


class StructuredDocument(StorableModel):
    """Модель структурированного документа (коносамент / ДУ) с контейнерами и метаданными.

    Содержит методы для кодирования файла, формирования текстового HTML-отчёта и подготовки данных
    для отправки в ЦУП (to_tsup_dict).
    """
    bill_of_lading: str | None = Field(
        default=None,
        title="Номер коносамента",
        description="Номер коносамента."
    )
    containers: list[Container] = Field(
        default_factory=list,
        title="Контейнеры",
        description="Список объектов контейнеров."
    )
    document_created_datetime: str | None = Field(
        default=None,
        title="Дата ДО",
        description="Дата и время составления документа (отчета) в формате строки.",
        json_schema_extra={"tsup_title": "ИмпМорскаяПеревозкаДатаПолученияДУ"},
    )
    voyage_number: str | None = Field(
        default=None,
        title="Номер рейса",
        description="Номер рейса.",
        json_schema_extra={"tsup_title": "ИмпМорскаяПеревозкаНомерРейсаФидер"},
    )
    document_type: DocType = Field(
        default=DocType.BILL_OF_LADING,
        title="Тип документа",
        description="Тип документа (значение из DocType).",
        json_schema_extra={"tsup_title": "ЭтоКоносамент"},
    )
    transaction_numbers: list[str] = Field(
        default_factory=list,
        title="Номера сделок",
        description="Список номеров сделок (транзакций)."
    )
    file_path: Path | str | None = Field(
        default=None,
        description="Путь к файлу документа (строка или объект Path)."
    )
    errors: OrderedSetType[str] = Field(
        default_factory=OrderedSetType,
        title="Ошибки",
        description="Список сообщений об ошибках обработки."
    )
    notes: OrderedSetType[str] = Field(
        default_factory=OrderedSetType,
        description="Список системных сообщений."
    )
    is_data_sent_to_tsup: bool = Field(
        default=False,
        title="Отправка в ЦУП",
        description="Флаг отправки данных в ЦУП. По умолчанию: False."
    )
    source_file_name: str | None = Field(
        default=None,
        description="Имя файла для загрузки в ЦУП."
    )
    source_file_base64: str | None = Field(
        default=None,
        description="Содержимое файла, закодированное в base64."
    )

    # trace_folder: Path | None = Field(
    #     default=None,
    #     description="Путь к директории, содержащей данные трассировки."
    # )

    @field_validator("containers", mode="before")
    @classmethod
    def validate_containers(cls, containers: list[Container]) -> list[Container]:
        """Фильтрует контейнеры, убирая записи без номера.

        Args:
            containers: Список словарей с данными контейнеров.

        Returns:
            list[dict]: Отфильтрованный список контейнеров.
        """
        if not containers:
            return []

        return [
            cont for cont in containers
            if isinstance(cont, dict) and cont.get("container", "").strip()
        ]

    def encode_file(self) -> None:
        """Кодирует файл (по атрибуту file_path) в base64 и формирует имя файла для ЦУП.

        Правила:
          - Если file_path не задан или файл не найден — логируем предупреждение и ничего не меняем.
          - Формируем имя в формате: {DOCTYPE_PREFIX}_{bill_of_lading|unknown}_AUTO{suffix}
            где DOCTYPE_PREFIX — часть document_type.value до первого подчёркивания.

        Returns:
            None: Обновляет поля source_file_name и source_file_base64 при успехе.
        """
        # Если не задан путь к файлу, то ничего не делаем
        if not self.file_path:
            logger.warning("❌ Путь к файлу не указан")
            return

        file_path = Path(self.file_path)
        if not file_path.is_file():
            logger.warning("❌ Файл не найден: %s", file_path)
            return

        # Генерация имени файла
        doc_type_prefix = self.document_type.value.split("_")[0]
        bill = self.bill_of_lading or "unknown"
        suffix = file_path.suffix
        self.source_file_name = f"{doc_type_prefix}_{bill}_AUTO{suffix}"

        # Кодирование файла в base64
        self.source_file_base64 = file_to_base64(file_path)

    def format_report(self) -> str:
        """Формирует человекочитаемый HTML-подобный отчёт по документу.

        Логика:
          - Определяем набор полей (FieldConfig) и заголовков (title) из метаданных pydantic.
          - Для каждого поля:
              * проверяем наличие значения (если поле не всегда показывать)
              * применяем transform, если задан
              * приводим коллекции к строке
              * применяем html_tag (если задан)
          - Возвращаем многострочный отчёт

        Args:
            None

        Returns:
            str: Многострочная строка отчёта.
        """
        fields: tuple[FieldConfig, ...] = (
            FieldConfig("is_data_sent_to_tsup", transform=format_sent_status_report, always_display=True),
            FieldConfig("document_type", transform=lambda x: x.split("_")[0]),
            FieldConfig("bill_of_lading"),
            FieldConfig("document_created_datetime"),
            FieldConfig("voyage_number"),
            FieldConfig("transaction_numbers"),
            FieldConfig("containers",
                        transform=lambda containers: Container.format_containers_section(containers),
                        html_tag=lambda x: f"\n{x}"
                        ),
            # FieldConfig("errors",
            #             transform=lambda notes: "\n".join(f"{' ' * 2}• {n}" for n in notes),
            #             html_tag=lambda x: f"\n{x}"
            #             ),
        )

        # Расчёт ширины для выравнивания.
        titles: list[str] = [self.__class__.model_fields[f.name].title or f.name for f in fields]
        # +1 к максимальной длине — место для двоеточия
        max_title_length = max((len(t) for t in titles), default=0) + 1

        # Формируем строки отчёта
        output_lines: list[str] = []

        for field_cfg, title in zip(fields, titles):
            # Текущее значение поля
            value = getattr(self, field_cfg.name, None)

            # Пропускаем пустое значение если поле не обязано всегда отображаться
            if not field_cfg.always_display and not value:
                continue

            # Преобразуем значение через transform, если задан
            if field_cfg.transform:
                try:
                    value = field_cfg.transform(value)
                except Exception as e:
                    logger.exception(
                        "⛔ Ошибка применения transform для поля %s: %s",
                        field_cfg.name, e
                    )

            # Для коллекций приводим к читаемой строке
            if isinstance(value, (list, tuple, set)):
                value = ", ".join(str(i) for i in value)

            # Оборачиваем в HTML-тег, если требуется
            if field_cfg.html_tag:
                try:
                    value = field_cfg.html_tag(value)
                except Exception as e:
                    logger.exception(
                        "⛔ Ошибка применения html_tag для поля %s: %s",
                        field_cfg.name, e
                    )

            padded_title = f"{title}:".ljust(max_title_length)
            output_lines.append(f"{padded_title} {value}")

        return "\n".join(output_lines)

    def format_report_with_errors(self) -> list[str]:
        return (
                [f"<span style='color: {RED_HEX};'><b>{err}</b></span>" for err in self.errors] +
                [self.format_report()]
        )

    def to_tsup_dict(self) -> dict[str, Any]:
        """Готовит словарь данных документа для отправки в ЦУП.

        Логика:
          - Собираем набор полей (FieldConfig) и для каждого непустого поля:
              * применяем transform (если задан)
              * извлекаем tsup_title из json_schema_extra поля модели (если существует)
              * добавляем в результирующий словарь
          - Возвращаем словарь, где значения — скалярные или структурированные (список/словарь) данные,
            в зависимости от поля (например, containers может быть списком dict'ов).

        Returns:
            dict[str, Any]: Словарь данных, готовый к сериализации и отправке.
        """
        # Поля для экспорта в ЦУП
        fields: tuple[FieldConfig, ...] = (
            FieldConfig("bill_of_lading"),
            # FieldConfig("document_type", transform=lambda x: "true" if str(x).startswith("КС") else "false"),
            FieldConfig("transaction_numbers"),
            FieldConfig("document_created_datetime"),
            FieldConfig("voyage_number"),
            FieldConfig("containers", transform=lambda x: [cont.to_tsup_dict() for cont in x]),
            # FieldConfig("source_file_name"),
            # FieldConfig("source_file_base64"),
        )

        result: dict[str, Any] = {}

        # Перебираем поля и добавляем непустые значения в словарь
        for field_cfg in fields:
            field_name = field_cfg.name
            value = getattr(self, field_name, None)
            if not value:
                continue

            if field_cfg.transform:
                try:
                    value = field_cfg.transform(value)
                except Exception as e:
                    logger.exception(
                        "⛔ Ошибка применения transform для поля %s: %s",
                        field_cfg.name, e
                    )

            # Получаем schema_extra и tsup_title, используем имя поля как запасной вариант
            schema_extra = self.__class__.model_fields[field_name].json_schema_extra
            tsup_title = schema_extra.get("tsup_title") or field_name if schema_extra else field_name
            result[tsup_title] = value

        # Добавление файлов
        is_bol: bool = self.document_type.value.startswith("КС")

        result["files"] = [{
            "name": self.source_file_name,
            "base64": self.source_file_base64,
            "ТипДокумента": "Коносамент фидерный" if is_bol else "",
            "ЭтоКоносамент": "true" if is_bol else "false"
        }]

        return result

# if __name__ == "__main__":
#     doc = StructuredDocument.load(
#         r"C:\Users\Cherdantsev\Desktop\Новая папка (2)\MDTRLS2510085.pdf.json"
#     )
#     # doc.file_path = r"C:\Users\Cherdantsev\Desktop\Новая папка (2)\КС_SILMUN25236300.pdf"
#
#     # print(doc.file_path)
#     # print(type(doc.file_path))
#
#     # print(doc.model_dump_json(indent=4))
#     # print(doc)
#     # print("\n\n")
#
#     # doc.errors.append("test")
#     #
#     # doc.errors.append(
#     #     f"Номер транзакции из ЦУП отсутствует. "
#     #     f"Возможно, номер коносамента ({doc.bill_of_lading}) "
#     #     f"распознан неверно."
#     # )
#     #
#     # # doc = StructuredDocument()
#     #
#     # print(doc.format_report())
#     # print("\n\n")
#     # import json
#     #
#     # print(json.dumps(doc.to_tsup_dict(), indent=4, ensure_ascii=False))
#
#     print(doc.errors)
