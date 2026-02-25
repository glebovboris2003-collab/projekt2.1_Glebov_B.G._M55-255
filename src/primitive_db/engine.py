"""Выполнение распарсенных команд и вывод результатов пользователю (PrettyTable, сообщения)."""

from __future__ import annotations

from typing import Any, Iterable

from .core import JsonDatabaseCore, QueryResult
from .errors import ParseError
from .parser import Command


def _try_pretty_table(headers: list[str], rows: Iterable[dict[str, Any]]) -> str:
    try:
        from prettytable import PrettyTable  
    except Exception:
        lines = ['\t'.join(headers)]
        for r in rows:
            lines.append('\t'.join(str(r.get(h, '')) for h in headers))
        return '\n'.join(lines)

    t = PrettyTable()
    t.field_names = headers
    for r in rows:
        t.add_row([r.get(h, '') for h in headers])
    return t.get_string()


_HELP = """Команды:
  create_table <table> <field:type> <field:type> ...
  list_tables
  drop_table <table>

  insert <table> <field=value> <field=value> ...
  select <table> [where <условие>]
  update <table> set <field=value>, <field=value> [where <условие>]
  delete <table> [where <условие>]

  help
  exit

Примеры:
  create_table users name:str age:int is_active:bool
  insert users name="Pop" age=23 is_active=true
  select users where age>=23 and is_active=true
  update users set age=25 where name="Pop"
  delete users where id=1
"""


class DatabaseEngine:
    """Диспетчер команд: вызывает методы PrimitiveDB и форматирует вывод."""

    def __init__(self, db: PrimitiveDB):
        self._db = db

    def process_command(self, cmd: Command) -> bool:
        """Execute one command. Returns False if should exit."""
        if cmd.name == 'exit':
            return False

        if cmd.name == 'help':
            print(_HELP)
            return True

        if cmd.name == 'list_tables':
            info = self._db.list_tables()
            if info is None:
                return True
            if not info:
                print('Таблиц нет.')
                return True
            rows = []
            for t in info:
                schema = t['schema']
                cols = ', '.join(f'{k}:{v}' for k, v in schema.items())
                rows.append(
                    {'table': t['table'], 'schema': cols, 'file': t['rows_file']}
                )
            print(_try_pretty_table(['table', 'schema', 'file'], rows))
            return True

        if cmd.name == 'create_table':
            if not cmd.table or not cmd.columns:
                raise ParseError('Синтаксис: create_table <table> <field:type> ...')
            self._db.create_table(cmd.table, cmd.columns)
            return True

        if cmd.name == 'drop_table':
            if not cmd.table:
                raise ParseError('Синтаксис: drop_table <table>')
            self._db.drop_table(cmd.table)
            return True

        if cmd.name == 'insert':
            if not cmd.table or not cmd.values:
                raise ParseError('Синтаксис: insert <table> <field=value> ...')
            self._db.insert(cmd.table, cmd.values)
            return True

        if cmd.name == 'select':
            if not cmd.table:
                raise ParseError('Синтаксис: select <table> [where ...]')
            res = self._db.select(cmd.table, cmd.where)
            if res is None:
                return True
            self._print_select(res)
            return True

        if cmd.name == 'update':
            if not cmd.table or not cmd.set_values:
                raise ParseError(
                    'Синтаксис: update <table> set <field=value>,... [where ...]'
                )
            self._db.update(cmd.table, cmd.set_values, cmd.where)
            return True

        if cmd.name == 'delete':
            if not cmd.table:
                raise ParseError('Синтаксис: delete <table> [where ...]')
            self._db.delete(cmd.table, cmd.where)
            return True

        raise ParseError(f'Неизвестная команда: {cmd.name!r}')

    def _print_select(self, res: SelectResult) -> None:
        rows = res.rows
        if not rows:
            msg = 'Пустой результат'
            if res.from_cache:
                msg += ' (cache)'
            print(msg + '.')
            return

        # Stable header order: id first, then rest alphabetical
        headers = sorted({k for r in rows for k in r.keys()})
        if 'id' in headers:
            headers.remove('id')
            headers = ['id', *headers]

        out = _try_pretty_table(headers, rows)
        if res.from_cache:
            out += '\n[cache]'
        print(out)
