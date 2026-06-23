"""
Миграция данных из SQLite → PostgreSQL.

Использование:
    DATABASE_URL=postgresql+psycopg2://user:pass@host/db python migrate_sqlite_to_postgres.py

Если DATABASE_URL не задан — скрипт попросит ввести его интерактивно.

Скрипт идемпотентен: использует ON CONFLICT DO NOTHING, безопасно запускать повторно.
"""

import os
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Определяем DATABASE_URL
# ---------------------------------------------------------------------------

DATABASE_URL: str = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    DATABASE_URL = input(
        "Введите DATABASE_URL для PostgreSQL "
        "(например postgresql+psycopg2://user:pass@host/db): "
    ).strip()

if not DATABASE_URL:
    print("DATABASE_URL не задан — выход.")
    sys.exit(1)

# Railway-совместимость
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

if not DATABASE_URL.startswith("postgresql"):
    print(f"Ожидается PostgreSQL URL, получено: {DATABASE_URL!r}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# SQLite-источник
# ---------------------------------------------------------------------------

_SQLITE_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent / "prices.db")))

if not _SQLITE_PATH.exists():
    print(f"SQLite файл не найден: {_SQLITE_PATH}")
    sys.exit(1)

print(f"Источник (SQLite): {_SQLITE_PATH}")
print(f"Назначение (PG):   {DATABASE_URL}")
print()

# ---------------------------------------------------------------------------
# Инициализируем PostgreSQL через database.py (уже переписанный)
# ---------------------------------------------------------------------------

# Временно подставляем PG URL чтобы init_db() создал схему в Postgres
os.environ["DATABASE_URL"] = DATABASE_URL

# Импортируем после установки DATABASE_URL — модуль читает env при импорте
import importlib
import database as db_module

# Пересоздаём движок с PG URL (на случай если модуль уже был инициализирован ранее)
importlib.reload(db_module)

print("Создаём схему в PostgreSQL...")
db_module.init_db()
print("Схема создана.")
print()

from sqlalchemy import create_engine, text

pg_engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# ---------------------------------------------------------------------------
# Читаем данные из SQLite
# ---------------------------------------------------------------------------

def _sqlite_rows(sqlite_conn, query: str) -> list[dict]:
    sqlite_conn.row_factory = sqlite3.Row
    cur = sqlite_conn.execute(query)
    return [dict(r) for r in cur.fetchall()]


print(f"Читаем данные из {_SQLITE_PATH}...")
sqlite_conn = sqlite3.connect(str(_SQLITE_PATH))
sqlite_conn.row_factory = sqlite3.Row

products     = _sqlite_rows(sqlite_conn, "SELECT * FROM products")
users        = _sqlite_rows(sqlite_conn, "SELECT * FROM users")
offers       = _sqlite_rows(sqlite_conn, "SELECT * FROM offers")
price_history = _sqlite_rows(sqlite_conn, "SELECT * FROM price_history")
parse_runs   = _sqlite_rows(sqlite_conn, "SELECT * FROM parse_runs")
sqlite_conn.close()

print(f"  products:      {len(products)}")
print(f"  users:         {len(users)}")
print(f"  offers:        {len(offers)}")
print(f"  price_history: {len(price_history)}")
print(f"  parse_runs:    {len(parse_runs)}")
print()

# ---------------------------------------------------------------------------
# Вставка данных в PostgreSQL
# ---------------------------------------------------------------------------

BATCH = 500  # размер батча для executemany


def _insert_batch(conn, table: str, rows: list[dict], conflict_col: str = "id") -> int:
    """Вставляет строки батчами, пропуская конфликты по conflict_col (обычно PK)."""
    if not rows:
        return 0

    cols = list(rows[0].keys())
    col_names = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    sql = (
        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    
    )

    inserted = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i: i + BATCH]
        result = conn.execute(text(sql), batch)
        inserted += result.rowcount
        print(f"  {table}: вставлено {min(i + BATCH, len(rows))}/{len(rows)} строк...", end="\r")

    print()
    return inserted


def _reset_sequence(conn, table: str, id_col: str = "id"):
    """Сбрасывает SERIAL-последовательность PostgreSQL до MAX(id)+1."""
    max_id = conn.execute(text(f"SELECT COALESCE(MAX({id_col}), 0) FROM {table}")).scalar()
    seq_name = f"{table}_{id_col}_seq"
    conn.execute(text(f"SELECT setval('{seq_name}', :v, true)"), {"v": max(max_id, 1)})


print("Вставляем данные в PostgreSQL...")

with pg_engine.connect() as conn:
    # Порядок важен из-за внешних ключей:
    # products → users → offers → price_history → parse_runs

    print("  → products")
    # Добавляем fts_tokens если колонка есть в SQLite — если нет, заполняем из name
    if products and "fts_tokens" not in products[0]:
        from database import _fts_tokens
        for p in products:
            p["fts_tokens"] = _fts_tokens(p["name"])
    # Добавляем slug если нет
    if products and "slug" not in products[0]:
        from database import _make_slug
        seen_slugs: dict = {}
        for p in products:
            base = _make_slug(p["name"]) or "product"
            slug = base
            counter = 2
            while slug in seen_slugs:
                slug = f"{base}-{counter}"
                counter += 1
            seen_slugs[slug] = True
            p["slug"] = slug
    n_products = _insert_batch(conn, "products", products)

    print("  → users")
    # Убеждаемся что есть нужные колонки
    if users and "username" not in users[0]:
        for u in users:
            u["username"] = None
    if users and "google_id" not in users[0]:
        for u in users:
            u["google_id"] = None
    n_users = _insert_batch(conn, "users", users)

    print("  → offers")
    n_offers = _insert_batch(conn, "offers", offers)

    print("  → price_history")
    n_history = _insert_batch(conn, "price_history", price_history)

    print("  → parse_runs")
    n_runs = _insert_batch(conn, "parse_runs", parse_runs)

    conn.commit()
    print()

    # --- Сбрасываем SERIAL-последовательности ---
    print("Сбрасываем SERIAL-последовательности...")
    for tbl in ("products", "users", "offers", "price_history", "parse_runs"):
        _reset_sequence(conn, tbl)
        print(f"  {tbl}_id_seq сброшена")
    conn.commit()

print()
print("=" * 50)
print("Миграция завершена.")
print(f"  products:      {n_products} новых")
print(f"  users:         {n_users} новых")
print(f"  offers:        {n_offers} новых")
print(f"  price_history: {n_history} новых")
print(f"  parse_runs:    {n_runs} новых")
print("=" * 50)
