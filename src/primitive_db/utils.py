from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .constants import FALSE_STRINGS, TRUE_STRINGS, VALID_TYPES
from .errors import ParseError, SchemaError, ValidationError

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def ensure_identifier(name: str, *, what: str = "identifier") -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Некорректное имя {what}: {name!r}")
    return name

def validate_identifier(name: str, **kwargs) -> str:
    context = kwargs.get("context") or kwargs.get("what") or "identifier"
    return ensure_identifier(name, what=str(context))
    """Проверяет корректность идентификатора (имя таблицы/поля) и выбрасывает ValidationError.
"""
 #   if not _IDENTIFIER_RE.match(name):
#      raise ValidationError(f"Некорректное имя {what}: {name!r}")
  #  return name


def parse_column_spec(spec: str) -> tuple[str, str]:
    """Парсит спецификацию поля вида field:type и возвращает (field, type).
"""
    if ":" not in spec:
        raise ParseError(f"Ожидается <поле:тип>, получено: {spec!r}")
    field, type_name = spec.split(":", 1)
    field = field.strip()
    type_name = type_name.strip()
    validate_identifier(field, what="поля")
    if type_name not in VALID_TYPES:
        raise SchemaError(f"Неподдерживаемый тип поля {field!r}: {type_name!r}")
    return field, type_name


def parse_assignment(expr: str) -> tuple[str, str]:
    """Парсит присваивание field=value и возвращает (field, raw_value).
"""
    if "=" not in expr:
        raise ParseError(f"Ожидается <поле=значение>, получено: {expr!r}")
    field, raw = expr.split("=", 1)
    field = field.strip()
    raw = raw.strip()
    validate_identifier(field, what="поля")
    return field, raw


def strip_quotes(value: str) -> str:
    """Удаляет внешние одинарные/двойные кавычки у строки, если они присутствуют.
"""
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def split_outside_quotes(text: str, *, sep: str = ",") -> list[str]:
    """Split by separator, but ignore separators inside single/double quotes."""
    parts: list[str] = []
    buf: list[str] = []
    quote: str | None = None

    for ch in text:
        if ch in {"'", '"'}:
            if quote is None:
                quote = ch
            elif quote == ch:
                quote = None

        if ch == sep and quote is None:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            continue

        buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def cast_to_type(raw: str, type_name: str) -> Any:
    """Преобразует строковое значение в тип из схемы (int/float/str/bool/None).
"""
    raw = raw.strip()
    if raw.lower() in {"null", "none"}:
        return None

    if type_name == "str":
        return strip_quotes(raw)

    if type_name == "bool":
        lowered = strip_quotes(raw).strip().lower()
        if lowered in TRUE_STRINGS:
            return True
        if lowered in FALSE_STRINGS:
            return False
        raise ValidationError(f"Некорректное значение bool: {raw!r}")

    if type_name == "int":
        try:
            return int(strip_quotes(raw))
        except ValueError as exc:
            raise ValidationError(f"Некорректное значение int: {raw!r}") from exc

    if type_name == "float":
        try:
            return float(strip_quotes(raw))
        except ValueError as exc:
            raise ValidationError(f"Некорректное значение float: {raw!r}") from exc

    raise SchemaError(f"Неподдерживаемый тип: {type_name!r}")


@dataclass(frozen=True)
class Condition:
    """Условие фильтрации: поле, оператор сравнения и «сырое» значение.
"""
    field: str
    op: str
    raw_value: str


def parse_comparison(expr: str) -> Condition:
    """Parse one comparison like 'age>=30' or 'name="Alice"'."""
    expr = expr.strip()
    # Order matters: check longest operators first.
    for op in (">=", "<=", "!=", "=", ">", "<"):
        if op in expr:
            field, raw = expr.split(op, 1)
            field = field.strip()
            raw = raw.strip()
            validate_identifier(field, what="поля")
            if raw == "":
                raise ParseError(f"Пустое значение в условии: {expr!r}")
            return Condition(field=field, op=op, raw_value=raw)
    raise ParseError(f"Не удалось распознать условие: {expr!r}")


def normalize_spaces(text: str) -> str:
    """Нормализует пробелы (удаляет повторяющиеся и обрезает по краям).
"""
    return " ".join(text.strip().split())