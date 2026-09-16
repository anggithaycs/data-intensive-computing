"""Read Week 2 SQL relative to this module, independently of the working directory."""

from pathlib import Path

SQL_ROOT = Path(__file__).resolve().parent / "sql"


def read_sql(relative_path):
    return (SQL_ROOT / relative_path).read_text(encoding="utf-8")
