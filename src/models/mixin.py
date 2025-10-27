from pathlib import Path
from typing import Self, Any, TypeVar, get_origin, get_args, Union
import types

from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
    GetJsonSchemaHandler,
    GetCoreSchemaHandler
)
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema, CoreSchema

from ordered_set import OrderedSet
from src.utils import read_json, write_text

T = TypeVar("T")


class OrderedSetType(OrderedSet[T]):
    """OrderedSet-обёртка, интегрируемая с Pydantic v2.

    - При чтении принимает list, tuple, set, OrderedSet.
    - Пытается применить валидацию элементов, если указан параметр типа (T).
    - При сериализации в JSON выдаёт обычный список.
    """

    @classmethod
    def __get_pydantic_core_schema__(
            cls,
            _source_type: Any,
            _handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Регистрирует схему валидации и сериализации для Pydantic."""
        return core_schema.no_info_after_validator_function(
            cls._validate,
            core_schema.list_schema(),  # ожидаем список на входе
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: list(v),  # сериализация в список
                return_schema=core_schema.list_schema(),
            ),
        )

    @classmethod
    def _validate(cls, value: Any) -> "OrderedSetType":
        """Преобразует list/set/tuple/OrderedSet в OrderedSetType."""
        # Если уже нужный тип — вернуть
        if isinstance(value, cls):
            return value

        # Если list, tuple, set, OrderedSet - поддерживаем
        if isinstance(value, (list, set, tuple, OrderedSet)):
            return cls(value)

        raise TypeError(
            f"Ожидался list, set, tuple или OrderedSet, получен {type(value)}"
        )

    @classmethod
    def __get_pydantic_json_schema__(
            cls, _core_schema: CoreSchema,
            handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Генерация схемы JSON — представляем OrderedSetType как список."""
        return handler(core_schema.list_schema())


class StorableModel(BaseModel):
    """Миксин для сохранения и загрузки Pydantic-моделей из JSON-файлов."""
    model_config = ConfigDict(
        coerce_numbers_to_str=True,
        # arbitrary_types_allowed=True,
    )

    def save(self, file_path: Path | str) -> None:
        """Сохраняет модель в JSON-файл.

        Args:
            file_path (Path | str): Путь к файлу, куда будет сохранена модель.
        """
        model_dump = self.model_dump_json(indent=4)
        write_text(file_path, model_dump)

    @classmethod
    def load(cls, file_path: Path | str) -> Self:
        """Загружает модель из JSON-файла.

        Args:
            file_path (Path | str): Путь к JSON-файлу.

        Returns:
            Self: Экземпляр модели, восстановленный из файла.
        """
        model_dict = read_json(file_path)
        return cls.model_validate(model_dict)

    @field_validator("*", mode="before")
    def empty_str_to_none(cls, v: Any, info) -> Any:
        """Инициализировать пустые строки, как None, если поле допускает значение None."""
        # Пропускаем, если значение не строка или непустая строка
        if not (isinstance(v, str) and v.strip() == ""):
            return v

        field_name = info.field_name
        field = cls.model_fields.get(field_name)

        # Безопасно проверяем, что поле существует
        if not field:
            return v

        # Проверяем, допускает ли аннотация None
        if cls._is_optional_type(field.annotation):
            return None

        # Если поле не допускает None — возвращаем как есть
        return v

    @staticmethod
    def _is_optional_type(tp: Any) -> bool:
        """Проверяет, является ли тип Optional[...] (т.е. допускает None)."""
        if tp is None:
            return True
        origin = get_origin(tp)
        if origin is types.UnionType or origin is Union:
            args = get_args(tp)
            return type(None) in args
        return False
