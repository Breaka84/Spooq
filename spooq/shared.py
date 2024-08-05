from enum import Enum


class EnumMode(Enum):
    @classmethod
    def to_string(cls) -> str:
        return "\n".join([f"- {e.name}: {e.value}" for e in cls])
