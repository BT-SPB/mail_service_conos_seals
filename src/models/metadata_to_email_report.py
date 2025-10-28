import html
from enum import StrEnum
from textwrap import dedent
from typing import Any, Callable, Iterable
from dataclasses import dataclass
from collections import defaultdict

from config import config
from src.utils_email import convert_email_date_to_moscow

CSS_STYLE = f"""
    /* Сброс */
    body, table, td, p {{
      margin: 0; 
      padding: 0;
      mso-line-height-rule: exactly;
    }}
    table {{ 
        border-collapse: separate; 
        mso-table-lspace: 0pt; 
        mso-table-rspace: 0pt; 
    }}
    img {{ 
        border: 0; 
        outline: none; 
        text-decoration: none; 
        -ms-interpolation-mode: bicubic; 
    }}
    a {{
        color:#ffffff;
        text-decoration:underline;
    }}
    body {{
        width: 100% !important;
        background-color: #fafafa;
        font-family: Arial, Helvetica, sans-serif;
        color: #333333;
    }}
    
    /* Общие таблицы */
    .table_wrapper {{ 
        width: 100%; 
        background-color: #fafafa;
        padding: 20px 0; 
    }}
    .table_container {{ 
        width: 700px; 
        background-color: #ffffff; 
        border: 1px solid #dddddd;
    }}
    
    /* Шапка */
    .header_block {{ 
        background-color: #667eea; 
        color: #ffffff; 
        text-align: center; 
        padding: 30px 25px; 
    }}
    .header_title {{ 
        font-size: 28px; 
        font-weight: bold; 
        padding-bottom: 5px; 
        color: #ffffff; 
    }}
    .header_text {{ 
        color: #f0f0f0; 
        font-size: 15px; 
        padding: 5px 0; 
    }}
    
    /* Сводка */
    .summary {{
        background-color: #f8f9fa;
        padding: 25px;
        border-bottom: 1px solid #e0e0e0;
        text-align: center;
    }}
    .stat-cell {{
        width: 25%;
        vertical-align: top;
        padding: 10px;
    }}
    .stat-icon {{
        font-size: 22px;
        padding-bottom: 5px;
    }}
    .stat-number {{
        font-size: 18px;
        padding-bottom: 5px;
    }}
    .stat-label {{
        font-size: 12px;
        text-transform: uppercase;
    }}
    
    /* Секции */
    .section {{ 
        padding: 16px; 
        border-bottom: 1px solid #e0e0e0; 
    }}
    .section_title {{ 
        font-size: 22px; 
        color: #444444;
        font-weight: bold; 
    }}
    
    /* Путь */
    .folder_path {{ 
        background-color: #f8f9fa; 
        border-left: 3px solid #dee2e6; 
        padding: 8px 12px; 
        font-size: 12px; 
    }}
    
    /* Карточка файла */
    .file_card {{ 
        border: 1px solid #e9ecef; 
        width: 100%; 
        background-color: #ffffff;
    }}
    .file_header {{ 
        background-color: #f8f9fa; 
        font-weight: bold; 
        font-size: 14px; 
        color: #555555;
        padding: 8px 12px; 
        border-bottom: 1px solid #e9ecef; 
    }}
    .file_content {{ 
        padding: 14px 10px; 
    }}
    pre {{ 
        padding: 6px; 
        margin: 0; 
        font-family: 'Courier New', Courier, monospace; 
        font-size: 13px; 
        white-space: pre-wrap; 
        word-wrap: break-word; 
    }}
    code {{
        background-color: #e9ecef; 
    }}
    
    /* Подвал */
    .footer_block {{ 
        background-color: #f8f9fa;  
        text-align: center; 
        padding: 16px 25px; 
        font-size: 12px; 
        color: #777777; 
    }}
    .footer_text {{ 
        padding: 8px 0; 
    }}
"""


def render_stat_cell_html(
        label: str,
        color: str,
        icon: str,
        count: str | int,
        background_color: str | None = None,
) -> str:
    """Формирует HTML-блок (ячейку статистики) для вставки в отчёт."""
    style_attr = f'style=" background-color:{background_color};"' if background_color else ""
    number_color = color if count > 0 else "#999999"
    number_font_weight = "bold" if count > 0 else "normal"
    label_color = "#666666" if count > 0 else "#999999"

    return f"""
    <td class="stat-cell" align="center" valign="top"{style_attr}>
        <p class="stat-icon" style="color:{color};">{icon}</p>
        <p class="stat-number" style="color:{number_color};font-weight:{number_font_weight};">{count}</p>
        <p class="stat-label", style="color:{label_color}">{label}</p>
    </td>
    """


@dataclass()
class SectionConfig:
    """Конфигурация секции отчёта.

    Attributes:
        title: Заголовок секции (может содержать emoji).
        color: Базовый HEX-цвет секции (например, '#ff4d4d').
        folder_attr: Имя атрибута StructuredMetadata, где хранится Path для копий файлов.
        condition: Либо булево, либо callable(self) -> bool, определяющее необходимость показа секции.
    """
    attr_name: str
    icon: str
    stat_label: str
    title: str
    color: str
    background_color: str
    folder_attr: str | None = None
    condition: Callable[..., bool] | bool | None = None
    count: int = 0


SECTION_META: list[SectionConfig] = [
    SectionConfig(
        attr_name="errors",
        icon="❌",
        stat_label="Ошибки",
        title="Ошибки обработки",
        color="#ff4d4d",
        background_color="#fff5f5",
        folder_attr="error_dir",
    ),
    SectionConfig(
        attr_name="partial_successes",
        icon="⚠️",
        stat_label="Частично",
        title="Частично обработанные файлы",
        color="#ffaa00",
        background_color="#fffaf0",
        folder_attr="error_dir",
    ),
    SectionConfig(
        attr_name="successes",
        icon="✅",
        stat_label="Успешно",
        title="Успешно обработанные файлы",
        color="#28a745",
        background_color="#f6fff8",
        condition=lambda: config.enable_success_notifications
    ),
]


def _escape(text: Any) -> str:
    """Безопасно экранирует текст для вставки в HTML."""
    if text is None:
        return ""
    return html.escape(str(text))


def _formatted_dict(data: defaultdict[str, Iterable[str]]) -> str:
    """Форматирует словарь filename -> [messages] в валидный HTML <ol> с вложенными <ul>."""
    if not data:
        return ""

    items = []
    for filename, messages in data.items():
        msg_list = "".join(
            f"""<tr><td class="file_content"><pre>{msg}</pre></td></tr>"""
            for msg in messages
        )

        items.append(
            f"""\n
            <!-- Start file -->
            <tr><td>
                <table role="presentation" class="file_card">
                    <tr><td class="file_header">📄&nbsp;&nbsp;{_escape(filename)}</td></tr>
                        {msg_list}
                </table>
            <tr><td>
            <!-- End file -->\n
            """
        )

    return '<tr><td height="8"></td></tr>\n'.join(items)


def metadata_to_email_report(model: "StructuredMetadata") -> str:
    """ФормируетHTML-отчёт для отправки по email.

    Возвращает пустую строку, если нет секций для показа.
    """
    sections: list[str] = []
    summary: list[str] = []

    for sec_cfg in SECTION_META:
        section_data = getattr(model, sec_cfg.attr_name, None)
        sec_cfg.count = len(section_data) if section_data is not None else 0

        summary.append(
            render_stat_cell_html(
                label=sec_cfg.stat_label,
                color=sec_cfg.color,
                icon=sec_cfg.icon,
                count=sec_cfg.count,
            )
        )

        # пропускаем пустые данные
        if not section_data:
            continue

        # вычисляем condition
        condition = sec_cfg.condition
        if condition is not None:
            if callable(condition):
                try:
                    is_show = bool(condition())
                except Exception:
                    # если callable упал — безопасно пропускаем секцию
                    is_show = False
            else:
                is_show = bool(condition)

            if not is_show:
                continue

        # Получаем путь к файлам из другого поля
        folder_path = getattr(model, sec_cfg.folder_attr, None) if sec_cfg.folder_attr else None
        folder_line = (
            f"""
            <tr><td class="folder_path">
                📁 Копии файлов: <code>{_escape(folder_path)}</code>
                <br><small style="color: #666666;">🔗 Откройте папку в проводнике вручную</small>
            </td></tr>
            <tr><td height="12"></td></tr>
            """
        ) if folder_path else ""

        sections.append(
            f"""
            <!-- START SECTION {sec_cfg.attr_name.upper()} -->
            <tr><td class="section" style="background-color: {sec_cfg.background_color}; border-left: 5px solid {sec_cfg.color};">
                <table role="presentation" width="100%">
                    <tr><td class="section_title">
                        <span style="color: {sec_cfg.color};">{sec_cfg.icon}</span>&nbsp;&nbsp;{sec_cfg.title}
                    </td></tr>
                    <tr><td height="20"></td></tr>
                    {folder_line}{_formatted_dict(section_data)}
                </table>
            </td></tr>
            <!-- END SECTION {sec_cfg.attr_name.upper()} -->
            """
        )

    if not sections:
        return ""

    header = f"""
    <!-- START HEADER -->
        <tr><td class="header_block" align="center">
            <p class="header_title">📊 Отчёт об обработке документов</p>
            <p class="header_text">Автоматическое уведомление о статусе обработки файлов</p>
            <p class="header_text">
                <b>Отправитель:</b> <a href="mailto:{model.sender}">{model.sender}</a> | 
                <b>Дата:</b> {convert_email_date_to_moscow(model.date)}
            </p>
        </td></tr>
    <!-- END HEADER -->
    """

    footer = f"""
    <!-- START FOOTER -->
        <tr><td class="footer_block">
            <p class="footer_text">
                С уважением,<br>
                <b>Система автоматической обработки документов</b>
            </p>
            <p class="footer_text" style="color: #999999; font-size: 11px;">
                Это автоматическое сообщение. Пожалуйста, не отвечайте на него.
            </p>
        </td></tr>
    <!-- END FOOTER -->
    """

    summary_total_block: str = render_stat_cell_html(
        label="Всего",
        color="#007bff",
        icon="🔵",
        count=sum(sec_cfg.count for sec_cfg in SECTION_META),
        background_color="#e6f0ff",
    )
    summary_formed = f"""
    <!-- START SUMMARY -->
        <tr><td class="summary" align="center">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                <tr>
                    {"".join(summary[::-1])}{summary_total_block}
                </tr>
            </table>
        </td></tr>
    <!-- END SUMMARY -->
    """

    sections_formed = f"""
    <!-- START BLOCK SECTIONS -->
        {"".join(sections)}
    <!-- END BLOCK SECTIONS -->
    """

    indent = "\n" * 6

    html_document = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Отчёт об обработке документов</title>
    <style type="text/css">{CSS_STYLE}</style>
</head>
<body>
    <table role="presentation" class="table_wrapper" align="center" cellspacing="0" cellpadding="0">
    <tr><td align="center">
        <table role="presentation" class="table_container" cellspacing="0" cellpadding="0" width="700">{indent}
            {header}{indent}
            {summary_formed}{indent}
            {sections_formed}{indent}
            {footer}{indent}
        </table>
    </td></tr>
    </table>
</body>
</html>
    """.strip()

    return dedent(html_document).strip()
