from enum import StrEnum


class Environment(StrEnum):
    """Режим работы проекта"""
    # Только мониторинг почты
    TEST_EMAIL = "test_email"
    # Только мониторинг директории OUTPUT_DIR
    TEST_DIR = "test_dir"
    # Тест всего функционала
    TEST = "test"
    # полный функционал
    PROD = "prod"


class DocType(StrEnum):
    BILL_OF_LADING = "КС"
    DU = "ДУ_base"
    DU_NLE = "ДУ_Новорослесэкспорт"
    DU_NUTEP = "ДУ_НУТЭП"
    DU_NMTP = "ДУ_НМТП"

    @classmethod
    def _missing_(cls, value: object) -> "DocType":
        """Возвращает UNKNOWN при неизвестном значении."""
        return cls.BILL_OF_LADING
