"""Декораторы и утилиты для CLI-приложения (обработка ошибок, профилирование, логирование)."""

from __future__ import annotations

import functools
import time
import traceback
from datetime import datetime
from typing import Any, Callable, TypeVar, cast

from . import constants as const
from .errors import DBError

F = TypeVar('F', bound=Callable[..., Any])


def _safe_input(prompt_text: str) -> str:
    """
    Безопасно запрашивает ввод у пользователя.
    Пытается использовать библиотеку prompt, при её отсутствии откатывается на стандартный input().
    """
    try:
        import prompt as prompt_lib  # type: ignore
        try:
            return str(prompt_lib.regex(r".*", prompt=prompt_text))
        except TypeError:
            return str(prompt_lib.regex(r".*"))
    except ImportError:
        return input(prompt_text)


def ask_user_confirmation(message: str) -> bool:
    """Запрашивает у пользователя подтверждение действия (Y/N)."""
    raw_response = _safe_input(f"{message} (Y/N): ").strip().lower()
    true_strings = getattr(const, "TRUE_STRINGS", ('y', 'yes', 'да', 'д'))
    return raw_response in true_strings


def require_confirmation(warning_message: str) -> Callable[[F], F]:
    """Декоратор: запрашивает подтверждение перед выполнением опасного действия."""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not ask_user_confirmation(warning_message):
                print("Действие отменено пользователем.")
                return None
            return func(*args, **kwargs)
        return cast(F, wrapper)
    return decorator


def catch_database_errors(func: F) -> F:
    """Декоратор: перехватывает ошибки базы данных и выводит понятные пользователю сообщения."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except DBError as exc:
            print(f"Ошибка БД: {exc}")
            return None
        except KeyboardInterrupt:
            print("\nОперация прервана пользователем (Ctrl+C).")
            return None
        except Exception:
            print("Критическая системная ошибка!")
            traceback.print_exc()
            return None
    return cast(F, wrapper)


def audit_log_command(func: F) -> F:
    """Декоратор: записывает факт вызова метода в лог-файл (commands.log)."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        instance = args[0]
        timestamp = datetime.now().isoformat(timespec='seconds')
        
        log_args = args[1:] if len(args) > 1 else ()
        log_entry = f"[{timestamp}] FUNC: {func.__name__} | ARGS: {log_args} | KWARGS: {kwargs}\n"
        
        try:
            log_dir = instance.db_root / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "commands.log"
            
            with log_file.open("a", encoding="utf-8") as file:
                file.write(log_entry)
        except OSError:
            pass
            
        return func(*args, **kwargs)
    return cast(F, wrapper)


def measure_execution_time(func: F) -> F:
    """
    Декоратор: замеряет и выводит в консоль время выполнения функции в миллисекундах.
    Использует time.perf_counter() для максимальной точности.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            print(f"[⏱ Замер времени] {func.__name__}: {elapsed_ms:.2f} ms")
    return cast(F, wrapper)
