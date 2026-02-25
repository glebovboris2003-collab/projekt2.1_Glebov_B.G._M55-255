"""Ядро базы данных: управление таблицами, CRUD-операции и кэширование."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

# Используем относительные импорты
from . import constants as const
from .decorators import (require_confirmation, catch_database_errors, audit_log_command, measure_execution_time, ask_user_confirmation)
from .errors import SchemaError, TableExistsError, TableNotFoundError, ValidationError
from .storage import ensure_data_dir, load_meta, load_table, save_meta, save_table, table_path
from .utils import Condition, validate_identifier, cast_to_type


@dataclass(frozen=True)
class QueryResult:
    """Структура для хранения результата SELECT-запроса и флага кэширования."""
    rows: list[dict[str, Any]]
    from_cache: bool


def create_query_cache() -> Callable:
    """
    Продвинутая механика: Замыкание для кэширования результатов SELECT.
    Сохраняет результаты запросов, инвалидируя их при изменении версии таблицы.
    """
    _cache_storage: dict[tuple[str, str, int], list[dict[str, Any]]] = {}

    def fetch_with_cache(
        table_name: str,
        condition_key: str,
        table_version: int,
        compute_func: Callable[[], list[dict[str, Any]]],
    ) -> QueryResult:
        cache_key = (table_name, condition_key, table_version)
        
        if cache_key in _cache_storage:
            # Возвращаем копию данных, чтобы пользователь случайно не изменил кэш
            return QueryResult(rows=[dict(row) for row in _cache_storage[cache_key]], from_cache=True)
            
        fresh_data = compute_func()
        _cache_storage[cache_key] = [dict(row) for row in fresh_data]
        return QueryResult(rows=fresh_data, from_cache=False)

    return fetch_with_cache


class JsonDatabaseCore:
    """Главный класс базы данных. Реализует CRUD-операции над JSON-файлами."""

    def __init__(self, db_root: Optional[Path] = None):
        self.db_root = db_root or const.get_root_directory()
        ensure_data_dir(self.db_root)
        self._meta = load_meta(self.db_root)
        
        # Инициализируем версии таблиц для работы кэша
        tables_dict = self._meta.get('tables', {})
        self._table_versions: dict[str, int] = {t_name: 0 for t_name in tables_dict.keys()}
        self._execute_cached_select = create_query_cache()

    def _increment_version(self, table: str) -> None:
        self._table_versions[table] = self._table_versions.get(table, 0) + 1

    def _get_table_schema_info(self, table: str) -> dict[str, Any]:
        tables = self._meta.get('tables', {})
        if table not in tables:
            raise TableNotFoundError(f"Таблица не найдена: '{table}'")
        return tables[table]

    def _sync_metadata(self) -> None:
        save_meta(self.db_root, self._meta)

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    def list_tables(self) -> list[dict[str, Any]]:
        tables = self._meta.get('tables', {})
        result = []
        for t_name, t_info in sorted(tables.items()):
            result.append({
                'table': t_name,
                'schema': dict(t_info.get('schema', {})),
                'rows_file': str(table_path(self.db_root, t_name)),
            })
        return result

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    def create_table(self, table: str, columns: list[tuple[str, str]]) -> None:
        validate_identifier(table, context="таблицы")
        
        if table in self._meta.get('tables', {}):
            raise TableExistsError(f"Таблица уже существует: '{table}'")
        if not columns:
            raise SchemaError("Невозможно создать таблицу без указания полей.")

        schema: dict[str, str] = {}
        for field, data_type in columns:
            if field == const.ID_FIELD:
                raise SchemaError(f"Поле '{const.ID_FIELD}' генерируется автоматически.")
            if field in schema:
                raise SchemaError(f"Обнаружено дублирующееся поле: '{field}'")
            if data_type not in const.VALID_TYPES:
                raise SchemaError(f"Неподдерживаемый тип данных: '{data_type}'")
            schema[field] = data_type

        self._meta.setdefault('tables', {})[table] = {'schema': schema, 'last_id': 0}
        self._sync_metadata()
        save_table(self.db_root, table, [])
        self._table_versions[table] = 0
        
        print(f"Успех: таблица '{table}' успешно создана.")

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    @require_confirmation("Вы уверены, что хотите полностью удалить таблицу?")
    def drop_table(self, table: str) -> None:
        validate_identifier(table, context="таблицы")
        tables = self._meta.get('tables', {})
        
        if table not in tables:
            raise TableNotFoundError(f"Таблица не найдена: '{table}'")
            
        file_path = table_path(self.db_root, table)
        if file_path.exists():
            file_path.unlink()
            
        del tables[table]
        self._sync_metadata()
        self._table_versions.pop(table, None)
        print(f"Успех: таблица '{table}' удалена.")

    def _prepare_conditions(self, table: str, where_clauses: Optional[list[Condition]]) -> list[tuple[str, str, Any]]:
        if not where_clauses:
            return []

        table_info = self._get_table_schema_info(table)
        schema: dict[str, str] = table_info.get('schema', {})
        prepared_filters = []
        
        for cond in where_clauses:
            field, operator, raw_val = cond.field, cond.op, cond.raw_value
            
            if field == const.ID_FIELD:
                type_name = "int"
            elif field not in schema:
                raise ValidationError(f"Неизвестное поле в условии WHERE: '{field}'")
            else:
                type_name = schema[field]
                
            parsed_value = cast_to_type(raw_val, type_name)
            prepared_filters.append((field, operator, parsed_value))
            
        return prepared_filters

    def _row_matches(self, row: dict[str, Any], conditions: list[tuple[str, str, Any]]) -> bool:
        for field, operator, target_value in conditions:
            row_value = row.get(field)
            
            match operator:
                case '=' | '==':
                    if row_value != target_value: return False
                case '!=':
                    if row_value == target_value: return False
                case '>':
                    if row_value is None or target_value is None or not (row_value > target_value): return False
                case '<':
                    if row_value is None or target_value is None or not (row_value < target_value): return False
                case '>=':
                    if row_value is None or target_value is None or not (row_value >= target_value): return False
                case '<=':
                    if row_value is None or target_value is None or not (row_value <= target_value): return False
                case _:
                    raise ValidationError(f"Неподдерживаемый оператор сравнения: '{operator}'")
        return True

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    def insert(self, table: str, values: dict[str, str]) -> dict[str, Any]:
        validate_identifier(table, context="таблицы")
        table_info = self._get_table_schema_info(table)
        schema: dict[str, str] = table_info.get('schema', {})

        missing_fields = [f for f in schema.keys() if f not in values]
        extra_fields = [f for f in values.keys() if f not in schema]
        
        if missing_fields:
            raise ValidationError(f"Не заполнены обязательные поля: {', '.join(missing_fields)}")
        if extra_fields:
            raise ValidationError(f"Переданы лишние поля, отсутствующие в схеме: {', '.join(extra_fields)}")

        new_row: dict[str, Any] = {}
        for field, type_name in schema.items():
            new_row[field] = cast_to_type(values[field], type_name)

        table_info['last_id'] = int(table_info.get('last_id', 0)) + 1
        new_row[const.ID_FIELD] = table_info['last_id']

        rows = load_table(self.db_root, table)
        rows.append(new_row)
        save_table(self.db_root, table, rows)
        
        self._sync_metadata()
        self._increment_version(table)
        
        print(f"Успех: добавлена запись ({const.ID_FIELD}={new_row[const.ID_FIELD]}).")
        return dict(new_row)

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    def select(self, table: str, where: Optional[list[Condition]] = None) -> QueryResult:
        validate_identifier(table, context="таблицы")
        self._get_table_schema_info(table)

        prepared_conditions = self._prepare_conditions(table, where)
        condition_hash = str([(f, op, repr(v)) for f, op, v in prepared_conditions])

        def _compute_rows() -> list[dict[str, Any]]:
            rows = load_table(self.db_root, table)
            if not prepared_conditions:
                return [dict(r) for r in rows]
            return [dict(r) for r in rows if self._row_matches(r, prepared_conditions)]

        current_version = self._table_versions.get(table, 0)
        return self._execute_cached_select(table, condition_hash, current_version, _compute_rows)

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    def update(self, table: str, set_values: dict[str, str], where: Optional[list[Condition]] = None) -> int:
        validate_identifier(table, context="таблицы")
        table_info = self._get_table_schema_info(table)
        schema: dict[str, str] = table_info.get('schema', {})

        if not set_values:
            raise ValidationError("Для команды UPDATE необходимо указать обновляемые поля (SET).")

        prepared_set_data: dict[str, Any] = {}
        for field, raw_val in set_values.items():
            if field == const.ID_FIELD:
                raise ValidationError(f"Поле '{const.ID_FIELD}' защищено от изменений.")
            if field not in schema:
                raise ValidationError(f"Неизвестное поле для обновления: '{field}'")
            prepared_set_data[field] = cast_to_type(raw_val, schema[field])

        prepared_conditions = self._prepare_conditions(table, where)
        rows = load_table(self.db_root, table)
        
        update_count = 0
        for row in rows:
            if not prepared_conditions or self._row_matches(row, prepared_conditions):
                for target_field, new_value in prepared_set_data.items():
                    row[target_field] = new_value
                update_count += 1

        save_table(self.db_root, table, rows)
        self._increment_version(table)
        print(f"Успех: обновлено записей — {update_count}.")
        return update_count

    @catch_database_errors
    @measure_execution_time
    @audit_log_command
    def delete(self, table: str, where: Optional[list[Condition]] = None) -> int:
        validate_identifier(table, context="таблицы")
        self._get_table_schema_info(table)

        prepared_conditions = self._prepare_conditions(table, where)

        if not prepared_conditions:
            if not ask_user_confirmation("Внимание! Вы собираетесь удалить ВСЕ записи из таблицы. Продолжить?"):
                print("Операция отменена пользователем.")
                return 0

        rows = load_table(self.db_root, table)
        initial_count = len(rows)
        
        if not prepared_conditions:
            rows = []
        else:
            rows = [r for r in rows if not self._row_matches(r, prepared_conditions)]
            
        deleted_count = initial_count - len(rows)
        save_table(self.db_root, table, rows)
        self._increment_version(table)
        print(f"Успех: удалено записей — {deleted_count}.")
        return deleted_count
