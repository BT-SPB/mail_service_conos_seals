from pathlib import Path
from typing import DefaultDict, Annotated
from collections import defaultdict

from pydantic import Field
from ordered_set import OrderedSet

from src.models.mixin import StorableModel, OrderedSetType
from src.models.metadata_to_email_report import metadata_to_email_report


class StructuredMetadata(StorableModel):
    """Pydantic-модель метаданных и генерации HTML-отчёта."""
    subject: str | None = Field(
        default=None,
        description="Тема входящего email."
    )
    sender: str | None = Field(
        default=None,
        description="Отправитель email."
    )
    date: str | None = Field(
        default=None,
        description="Дата и время получения сообщения."
    )
    text_content: str | None = Field(
        default=None,
        description="Текст входящего сообщения."
    )
    files: list[str] = Field(
        default_factory=list,
        description="Список имен приложенных файлов."
    )
    errors: DefaultDict[str, Annotated[OrderedSetType[str], Field(default_factory=OrderedSetType)]] = Field(
        default_factory=lambda: defaultdict(OrderedSetType),
        description="Словарь: имя файла → список сообщений об ошибках."
    )
    partial_successes: DefaultDict[str, Annotated[OrderedSetType[str], Field(default_factory=OrderedSetType)]] = Field(
        default_factory=lambda: defaultdict(OrderedSetType),
        description="Словарь: имя файла → список сообщений о частичном успехе."
    )
    successes: DefaultDict[str, Annotated[OrderedSetType[str], Field(default_factory=OrderedSetType)]] = Field(
        default_factory=lambda: defaultdict(OrderedSetType),
        description="Словарь: имя файла → список сообщений об успешных операциях."
    )
    global_errors: OrderedSetType[str] = Field(
        default_factory=OrderedSetType,
        description="Список глобальных ошибок."
    )
    error_dir: Path | None = Field(
        default=None,
        description="Путь до директории для файлов с ошибками."
    )
    success_dir: Path | None = Field(
        default=None,
        description="Путь до директории для успешно обработанных файлов."
    )

    def email_report(self) -> str:
        return metadata_to_email_report(self)


if __name__ == "__main__":
    from src.utils import write_text

    # old_file = read_json(r"C:\Users\Cherdantsev\Desktop\251001_091741_aby@sdrzbt.ru\metadata_old2.json")
    #
    # metadata = StructuredMetadata.model_validate(old_file)
    # # metadata = StructuredMetadata(
    # #     subject="df"
    # # )
    # # metadata.errors["file_name_1"].append("text message")
    # # metadata.errors["file_name_1"].append("text message1")
    # print(metadata)
    #
    # doc_json = metadata.model_dump_json(indent=4)
    # write_text(r"C:\Users\Cherdantsev\Desktop\251001_091741_aby@sdrzbt.ru\metadata.json", doc_json)

    metadata = StructuredMetadata.load(r"C:\Users\Cherdantsev\Desktop\Новая папка (2)\metadata.json")
    metadata.error_dir = Path(
        r"\\192.168.6.9\docs\04 PROJECT MANAGERS\ЛИНЕЙНЫЕ МЕНЕДЖЕРЫ\OCR_CONOS\ERROR\250930_110231_aby@sdrzbt.ru")

    report = metadata.email_report()
    write_text(r"C:\Users\Cherdantsev\Desktop\Новая папка (2)\report.html", report)
    # print("\n\n")
    # print(metadata.model_dump_json(indent=4))
    #
    # from src.utils_email import send_email
    # from src.utils import read_text
    # from config import config
    # import logging
    #
    # logger = logging.basicConfig(level=logging.INFO)
    #
    # # report = read_text(r"C:\Users\Cherdantsev\Desktop\Новая папка (2)\4.html")
    #
    # send_email(
    #     email_text=report,
    #     recipient_emails=config.notification_emails,
    #     subject="test",
    #     email_format="html",
    # )
