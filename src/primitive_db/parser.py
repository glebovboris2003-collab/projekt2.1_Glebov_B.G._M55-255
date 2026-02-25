"""Парсер пользовательских команд. Разбирает текст в структурированные объекты Command."""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from typing import Optional

from primitive_db import constants as const
from primitive_db.errors import ParseError
from primitive_db.utils import (Condition, parse_assignment, parse_column_spec, parse_comparison, split_outside_quotes)


@dataclass(frozen=True)
class Command:
    """Структура, описывающая распарсенную команду и её аргументы."""

    name: str
    table: Optional[str] = None
    columns: Optional[list[tuple[str, str]]] = (
        None  # Список кортежей (имя_поля, тип_данных)
    )
    values: Optional[dict[str, str]] = None  # Для INSERT: поле -> сырое_значение
    set_values: Optional[dict[str, str]] = None  # Для UPDATE: поле -> сырое_значение
    where: Optional[list[Condition]] = None  # Для фильтрации (SELECT, UPDATE, DELETE)


# Предкомпилированные регулярные выражения для сложных команд с условиями WHERE
PATTERN_UPDATE = re.compile(
    r'^update\s+(?P<table>\w+)\s+set\s+(?P<set>.+?)(?:\s+where\s+(?P<where>.+))?$',
    flags=re.IGNORECASE | re.DOTALL,
)
PATTERN_SELECT = re.compile(
    r'^select\s+(?P<table>\w+)(?:\s+where\s+(?P<where>.+))?$',
    flags=re.IGNORECASE | re.DOTALL,
)
PATTERN_DELETE = re.compile(
    r'^delete\s+(?P<table>\w+)(?:\s+where\s+(?P<where>.+))?$',
    flags=re.IGNORECASE | re.DOTALL,
)


def _parse_where_clause(raw_where: str) -> list[Condition]:
    """Разбирает блок WHERE на отдельные условия (поддерживает только оператор AND)."""
    raw_where = raw_where.strip()
    if not raw_where:
        return []

    tokens = shlex.split(raw_where, posix=True)
    conditions: list[str] = []
    buffer: list[str] = []

    for token in tokens:
        token_lower = token.lower()
        if token_lower == 'and':
            if not buffer:
                raise ParseError('Синтаксическая ошибка: пустое условие перед AND.')
            conditions.append(' '.join(buffer))
            buffer.clear()
        elif token_lower == 'or':
            raise ParseError(
                'Оператор OR временно не поддерживается. Используйте только AND.'
            )
        else:
            buffer.append(token)

    if buffer:
        conditions.append(' '.join(buffer))

    return [parse_comparison(cond) for cond in conditions]


def _parse_create_table(line: str) -> Command:
    """Обработчик для команды create_table."""
    tokens = shlex.split(line, posix=True)
    if len(tokens) < 3:
        raise ParseError(const.ERR_CREATE_SYNTAX)

    table_name = tokens[1]
    raw_columns = tokens[2:]
    columns = [parse_column_spec(col) for col in raw_columns]

    return Command(name='create_table', table=table_name, columns=columns)


def _parse_drop_table(line: str) -> Command:
    """Обработчик для команды drop_table."""
    tokens = shlex.split(line, posix=True)
    if len(tokens) != 2:
        raise ParseError(const.ERR_DROP_SYNTAX)

    return Command(name='drop_table', table=tokens[1])


def _parse_insert(line: str) -> Command:
    """Обработчик для команды insert."""
    tokens = shlex.split(line, posix=True)
    if len(tokens) < 3:
        raise ParseError(const.ERR_INSERT_SYNTAX)

    table_name = tokens[1]
    raw_assignments = tokens[2:]

    values_dict: dict[str, str] = {}
    for assignment in raw_assignments:
        field, raw_val = parse_assignment(assignment)
        values_dict[field] = raw_val

    return Command(name='insert', table=table_name, values=values_dict)


def _parse_select(line: str) -> Command:
    """Обработчик для команды select (использует регулярное выражение)."""
    match = PATTERN_SELECT.match(line)
    if not match:
        raise ParseError(const.ERR_SELECT_SYNTAX)

    table_name = match.group('table')
    where_raw = match.group('where')
    where_conditions = _parse_where_clause(where_raw) if where_raw else None

    return Command(name='select', table=table_name, where=where_conditions)


def _parse_update(line: str) -> Command:
    """Обработчик для команды update (использует регулярное выражение)."""
    match = PATTERN_UPDATE.match(line)
    if not match:
        raise ParseError(const.ERR_UPDATE_SYNTAX)

    table_name = match.group('table')
    set_raw = match.group('set')
    where_raw = match.group('where')

    set_dict: dict[str, str] = {}
    for assignment in split_outside_quotes(set_raw, sep=','):
        field, raw_val = parse_assignment(assignment)
        set_dict[field] = raw_val

    where_conditions = _parse_where_clause(where_raw) if where_raw else None
    return Command(
        name='update', table=table_name, set_values=set_dict, where=where_conditions
    )


def _parse_delete(line: str) -> Command:
    """Обработчик для команды delete (использует регулярное выражение)."""
    match = PATTERN_DELETE.match(line)
    if not match:
        raise ParseError(const.ERR_DELETE_SYNTAX)

    table_name = match.group('table')
    where_raw = match.group('where')
    where_conditions = _parse_where_clause(where_raw) if where_raw else None

    return Command(name='delete', table=table_name, where=where_conditions)


def parse_command(line: str) -> Command:
    """
    Главная функция-маршрутизатор.
    Определяет тип команды по первому слову и передает строку в нужный обработчик.
    """
    line = line.strip()
    if not line:
        raise ParseError('Получена пустая команда.')

    # Получаем первое слово (имя команды)
    first_word = line.split(maxsplit=1)[0].lower()

    match first_word:
        case 'exit' | 'quit':
            return Command(name='exit')
        case 'help':
            return Command(name='help')
        case 'list_tables':
            return Command(name='list_tables')
        case 'create_table':
            return _parse_create_table(line)
        case 'drop_table':
            return _parse_drop_table(line)
        case 'insert':
            return _parse_insert(line)
        case 'select':
            return _parse_select(line)
        case 'update':
            return _parse_update(line)
        case 'delete':
            return _parse_delete(line)
        case _:
            raise ParseError(f"{const.ERR_UNKNOWN_CMD} '{first_word}'")
