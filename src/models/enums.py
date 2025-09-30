from enum import StrEnum

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
