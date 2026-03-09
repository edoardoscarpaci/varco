from enum import StrEnum


class UpperStrEnum(StrEnum):
    @staticmethod
    def _generate_next_value_(name, *args):  # type: ignore
        return name.upper()
