"""Точка входа: интерактивный REPL (Read-Eval-Print Loop) для работы с базой данных."""

from __future__ import annotations

import sys

# Используем относительные импорты.
# Если Pylance ругается на .engine, убедитесь, что файл называется именно engine.py, а не commands.py.
from . import constants as const
from .core import JsonDatabaseCore
from .engine import DatabaseEngine  
from .errors import DBError, ParseError
from .parser import parse_command


def _get_user_input() -> str:
    """
    Считывает строку ввода пользователя.
    Использует библиотеку prompt, при её отсутствии откатывается к input().
    """
    prompt_prefix = 'db> '
    try:
        # Добавляем # type: ignore, чтобы линтер не жаловался на отсутствие типов в сторонней библиотеке
        import prompt as prompt_lib  # type: ignore

        try:
            match = prompt_lib.regex(r'.*', prompt=prompt_prefix)
            return match.group(0) if match else ''
        except TypeError:
            # Резервный вариант для специфических версий библиотеки
            match = prompt_lib.regex(r'.*')
            return match.group(0) if match else ''
    except (ImportError, Exception):
        return input(prompt_prefix)


def main() -> None:
    """
    Главный цикл приложения.
    Инициализирует ядро БД, диспетчер команд и запускает бесконечный цикл обработки ввода.
    """
    print(f'Добро пожаловать в {const.APP_TITLE} (JSON, без SQL).')
    print("Для просмотра списка доступных команд введите 'help'. Для выхода — 'exit'.")

    # Инициализируем компоненты
    database_core = JsonDatabaseCore()
    engine = DatabaseEngine(database_core)

    while True:
        try:
            raw_line = _get_user_input().strip()
        except (EOFError, KeyboardInterrupt):
            # Изящная обработка прерывания (Ctrl+D или Ctrl+C)
            print('\nЗавершение работы базы данных. До свидания!')
            break

        if not raw_line:
            continue

        try:
            # 1. Парсинг строки в объект команды
            command = parse_command(raw_line)

            # 2. Выполнение команды через логику движка
            is_running = engine.process_command(command)

            # 3. Условие выхода из цикла
            if not is_running:
                print('Завершение работы базы данных. До свидания!')
                break

        except ParseError as exc:
            print(f'Ошибка парсинга: {exc}')
        except ValueError as exc:
            # Перехватываем ошибки синтаксиса из DatabaseEngine
            print(f'Ошибка синтаксиса: {exc}')
        except DBError as exc:
            # Перехватываем доменные ошибки базы данных
            print(f'Ошибка выполнения: {exc}')


if __name__ == '__main__':
    main()
