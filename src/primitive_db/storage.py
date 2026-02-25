"""
Слой файлового хранилища (Storage Layer).
Обеспечивает безопасное чтение и атомарную запись JSON-файлов на диск.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from primitive_db import constants as const
from primitive_db.errors import StorageError


def _fetch_json_data(file_path: Path, default_fallback: Any) -> Any:
    """
    Читает JSON с диска. Если файла нет, возвращает переданное значение по умолчанию.
    Перехватывает ошибки файловой системы и битого JSON.
    """
    if not file_path.exists():
        return default_fallback

    try:
        content = file_path.read_text(encoding='utf-8')
        return json.loads(content)
    except (OSError, json.JSONDecodeError) as exc:
        raise StorageError(
            f'Ошибка чтения или декодирования JSON: {file_path}'
        ) from exc


def _atomic_json_write(target_path: Path, payload: Any) -> None:
    """
    Продвинутая механика: Атомарная запись.
    Сначала пишет данные во временный файл (.tmp), а затем мгновенно переименовывает его.
    Это гарантирует, что файл базы данных не повредится при внезапном сбое программы.
    """
    try:
        # Убеждаемся, что директория для файла существует
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Создаем временный файл рядом с целевым
        temp_file = target_path.with_name(f'{target_path.name}.tmp')

        with temp_file.open('w', encoding='utf-8') as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=4, sort_keys=True)

        # Атомарная подмена файла (в POSIX системах работает идеально)
        temp_file.replace(target_path)

    except OSError as exc:
        raise StorageError(f'Критическая ошибка записи на диск: {target_path}') from exc


def load_meta(db_root: Path) -> dict[str, Any]:
    """Загружает главный файл метаданных (db_meta.json)."""
    meta_file = const.get_meta_filepath(db_root)
    return _fetch_json_data(meta_file, default_fallback={'tables': {}})


def save_meta(db_root: Path, metadata: dict[str, Any]) -> None:
    """Атомарно сохраняет структуру базы данных в db_meta.json."""
    meta_file = const.get_meta_filepath(db_root)
    _atomic_json_write(meta_file, metadata)


def table_path(db_root: Path, table_name: str) -> Path:
    """Генерирует абсолютный путь к файлу конкретной таблицы."""
    data_directory = const.get_data_directory(db_root)
    return data_directory / f'{table_name}.json'


def load_table(db_root: Path, table_name: str) -> list[dict[str, Any]]:
    """Считывает все записи конкретной таблицы. Возвращает пустой список, если таблица пуста."""
    file_path = table_path(db_root, table_name)
    return _fetch_json_data(file_path, default_fallback=[])


def save_table(db_root: Path, table_name: str, rows: list[dict[str, Any]]) -> None:
    """Атомарно перезаписывает данные таблицы (весь массив строк)."""
    file_path = table_path(db_root, table_name)
    _atomic_json_write(file_path, rows)


def ensure_data_dir(db_root: Path) -> None:
    """Проверяет наличие системной папки data/ и создает её при необходимости."""
    target_dir = const.get_data_directory(db_root)
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise StorageError(
            f'Невозможно создать директорию хранилища: {target_dir}'
        ) from exc
