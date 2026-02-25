"""
Иерархия кастомных исключений для управления ошибками базы данных.
Реализует доменные ошибки, которые перехватываются на уровне декораторов.
"""

from __future__ import annotations


class DBError(Exception):
    """
    Базовое исключение проекта.
    Все специфические ошибки наследуются от этого класса для удобного перехвата
    в декораторе catch_database_errors.
    """

    def __init__(
        self, message: str = 'Произошла непредвиденная ошибка базы данных'
    ) -> None:
        self.message = message
        super().__init__(self.message)


class ParseError(DBError):
    """Исключение для синтаксических ошибок при разборе текстовых команд."""

    def __init__(self, message: str = 'Ошибка синтаксиса команды') -> None:
        super().__init__(message)


class TableExistsError(DBError):
    """Выбрасывается, когда происходит попытка создать уже существующую таблицу."""

    def __init__(
        self, message: str = 'Конфликт: таблица с таким именем уже существует'
    ) -> None:
        super().__init__(message)


class TableNotFoundError(DBError):
    """Выбрасывается при попытке выполнить операции над несуществующей таблицей."""

    def __init__(self, message: str = 'Запрашиваемая таблица не найдена') -> None:
        super().__init__(message)


class SchemaError(DBError):
    """Ошибки проектирования схемы (например, использование зарезервированного поля id)."""

    def __init__(
        self, message: str = 'Нарушение целостности или правил схемы таблицы'
    ) -> None:
        super().__init__(message)


class ValidationError(DBError):
    """Ошибки несоответствия типов данных или отсутствия обязательных полей."""

    def __init__(self, message: str = 'Данные не прошли валидацию') -> None:
        super().__init__(message)


class StorageError(DBError):
    """Исключения слоя файловой системы (проблемы с доступом к JSON или метаданным)."""

    def __init__(
        self, message: str = 'Критическая ошибка доступа к файловому хранилищу'
    ) -> None:
        super().__init__(message)
