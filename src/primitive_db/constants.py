"""Системные константы, пути и текстовые сообщения для базы данных."""

from pathlib import Path

# --- ОСНОВНЫЕ НАСТРОЙКИ ---
APP_TITLE = 'Primitive DB Project'

# Названия системных файлов и директорий
METADATA_FILE = 'db_meta.json'
TABLES_DIR = 'data'
LOGS_DIR = 'logs'
ACTIONS_LOG_FILE = 'commands.log'

# Системные поля таблиц
ID_FIELD = 'id'

# --- ТИПЫ ДАННЫХ ---
# Поддерживаемые базовые типы в нашей БД
VALID_TYPES: dict[str, type] = {
    'int': int,
    'float': float,
    'str': str,
    'bool': bool,
}

# Строковые представления логических значений (frozenset защищает от изменения)
TRUE_STRINGS = frozenset({'true', '1', 'yes', 'y', 'да', 'д'})
FALSE_STRINGS = frozenset({'false', '0', 'no', 'n', 'нет', 'н'})


# --- ФУНКЦИИ ГЕНЕРАЦИИ ПУТЕЙ ---
def get_root_directory() -> Path:
    """Возвращает корневую директорию проекта (откуда запущен скрипт)."""
    return Path.cwd()


def get_meta_filepath(root_dir: Path) -> Path:
    """Генерирует полный путь к файлу метаданных."""
    return root_dir / METADATA_FILE


def get_data_directory(root_dir: Path) -> Path:
    """Генерирует путь к папке, где будут лежать JSON-файлы таблиц."""
    return root_dir / TABLES_DIR


def get_logs_directory(root_dir: Path) -> Path:
    """Генерирует путь к папке с логами."""
    return root_dir / LOGS_DIR


def get_log_filepath(root_dir: Path) -> Path:
    """Генерирует путь к конкретному файлу логирования команд."""
    return get_logs_directory(root_dir) / ACTIONS_LOG_FILE


# --- ТЕКСТОВЫЕ СООБЩЕНИЯ И ОШИБКИ (из предыдущего шага) ---
MSG_NO_TABLES = 'В базе данных пока нет таблиц.'
MSG_EMPTY_RESULT = 'Пустой результат'
MSG_FROM_CACHE = '[Взято из кэша]'

ERR_CREATE_SYNTAX = (
    'Ошибка синтаксиса. Используйте: create_table <table> <field:type> ...'
)
ERR_DROP_SYNTAX = 'Ошибка синтаксиса. Используйте: drop_table <table>'
ERR_INSERT_SYNTAX = 'Ошибка синтаксиса. Используйте: insert <table> <field=value> ...'
ERR_SELECT_SYNTAX = 'Ошибка синтаксиса. Используйте: select <table> [where <условие>]'
ERR_UPDATE_SYNTAX = (
    'Ошибка синтаксиса. Используйте: update <table> set <field=value> [where <условие>]'
)
ERR_DELETE_SYNTAX = 'Ошибка синтаксиса. Используйте: delete <table> [where <условие>]'
ERR_UNKNOWN_CMD = 'Неизвестная команда:'

