"""
Модуль базы данных — SQLAlchemy хранилище товаров и цен.
Поддерживает PostgreSQL (продакшн) и SQLite (локально) через DATABASE_URL.

Таблицы:
- products      — уникальные товары (после нормализации)
- offers        — предложения с каждого магазина (цена, ссылка, наличие)
- price_history — история изменения цен
"""

import os
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, event, text, bindparam
from sqlalchemy.exc import IntegrityError

# ---------------------------------------------------------------------------
# Конфигурация подключения
# ---------------------------------------------------------------------------

DB_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent / "prices.db")))

_DEFAULT_DB_URL = f"sqlite:///{DB_PATH}"

DATABASE_URL: str = os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)

# Railway отдаёт postgres://, psycopg2 требует postgresql+psycopg2://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

_IS_POSTGRES = DATABASE_URL.startswith("postgresql")

_connect_args = {} if _IS_POSTGRES else {"check_same_thread": False}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=_connect_args,
)

# SQLite PRAGMAs через event hook (не работают через обычный execute после commit)
if not _IS_POSTGRES:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=10000")
        cursor.close()


# ---------------------------------------------------------------------------
# Вспомогательные утилиты
# ---------------------------------------------------------------------------

def get_connection():
    """Возвращает SQLAlchemy Connection (контекстный менеджер для транзакций)."""
    return engine.connect()


@contextmanager
def _conn_ctx():
    """Вспомогательный контекстный менеджер: открывает, возвращает и закрывает соединение."""
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def _get_columns(conn, table: str) -> set:
    """Возвращает множество имён колонок таблицы (dialect-aware)."""
    if _IS_POSTGRES:
        rows = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = :t"
        ), {"t": table}).fetchall()
        return {r[0] for r in rows}
    else:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {r[1] for r in rows}


def _get_tables(conn) -> set:
    """Возвращает множество имён таблиц в текущей БД (dialect-aware)."""
    if _IS_POSTGRES:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )).fetchall()
        return {r[0] for r in rows}
    else:
        rows = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )).fetchall()
        return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Токенизация и FTS
# ---------------------------------------------------------------------------

def _name_tokens(name: str) -> list:
    """Разбивает название на токены для поиска.
    'RTX5060' → ['rtx', '5060', 'rtx5060'] — ищем и слипшееся и раздельное."""
    s = name.lower()
    raw = re.findall(r"[a-zа-яё0-9]+", s)
    split = re.sub(r"([a-zа-яё])(\d)", r"\1 \2", s)
    split = re.sub(r"(\d)([a-zа-яё])", r"\1 \2", split)
    expanded = re.findall(r"[a-zа-яё0-9]+", split)
    return list(set(raw + expanded))


def _query_word_matches(query_word: str, name_tokens: list) -> bool:
    for token in name_tokens:
        if token.startswith(query_word):
            return True
        if (len(query_word) >= 6 and query_word.isalpha()
                and len(token) >= 6 and token.isalpha()):
            if token[:-2] == query_word[:-2]:
                return True
    return False


def _fts_tokens(name: str) -> str:
    """Возвращает строку токенов для FTS-индекса."""
    return " ".join(_name_tokens(name))


def _build_fts_query(query: str) -> str | None:
    """Переводит поисковый запрос в FTS5 MATCH-выражение (для SQLite)."""
    terms = []
    for word in re.findall(r"[а-яёa-z0-9]+", query.lower()):
        if len(word) >= 6 and word.isalpha():
            terms.append(word[:-2] + "*")
        else:
            terms.append(word + "*")
    return " ".join(terms) if terms else None


def _build_pg_fts_query(query: str) -> str | None:
    """Переводит поисковый запрос в to_tsquery-выражение (для PostgreSQL).
    'rtx 5060' → 'rtx:* & 5060:*'
    """
    terms = []
    for word in re.findall(r"[а-яёa-z0-9]+", query.lower()):
        if len(word) >= 6 and word.isalpha():
            terms.append(word[:-2] + ":*")
        else:
            terms.append(word + ":*")
    return " & ".join(terms) if terms else None


# ---------------------------------------------------------------------------
# Транслитерация и slug
# ---------------------------------------------------------------------------

_TRANSLIT_MAP = {
    'а': 'a',  'б': 'b',  'в': 'v',  'г': 'g',  'д': 'd',
    'е': 'e',  'ё': 'yo', 'ж': 'zh', 'з': 'z',  'и': 'i',
    'й': 'y',  'к': 'k',  'л': 'l',  'м': 'm',  'н': 'n',
    'о': 'o',  'п': 'p',  'р': 'r',  'с': 's',  'т': 't',
    'у': 'u',  'ф': 'f',  'х': 'kh', 'ц': 'ts', 'ч': 'ch',
    'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y',  'ь': '',
    'э': 'e',  'ю': 'yu', 'я': 'ya',
}


def _make_slug(name: str) -> str:
    """Генерирует URL-slug из названия товара (с транслитерацией кириллицы)."""
    s = name.lower()
    s = ''.join(_TRANSLIT_MAP.get(c, c) for c in s)
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', '-', s.strip())
    s = re.sub(r'-+', '-', s)
    return s.strip('-')[:80]


def _unique_slug(conn, name: str, exclude_id: int = None) -> str:
    """Возвращает уникальный slug, добавляя -2, -3 при конфликтах."""
    base = _make_slug(name) or "product"
    slug = base
    counter = 2
    while True:
        row = conn.execute(
            text("SELECT id FROM products WHERE slug = :slug"), {"slug": slug}
        ).mappings().fetchone()
        if not row or (exclude_id is not None and row["id"] == exclude_id):
            return slug
        slug = f"{base}-{counter}"
        counter += 1


# ---------------------------------------------------------------------------
# DDL — создание схемы
# ---------------------------------------------------------------------------

def _ddl_products() -> str:
    if _IS_POSTGRES:
        return """
        CREATE TABLE IF NOT EXISTS products (
            id           SERIAL PRIMARY KEY,
            name         TEXT NOT NULL,
            category     TEXT NOT NULL DEFAULT 'GPU',
            brand        TEXT,
            model        TEXT,
            slug         TEXT,
            canonical_id INTEGER REFERENCES products(id),
            fts_tokens   TEXT,
            created_at   TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    return """
        CREATE TABLE IF NOT EXISTS products (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            category     TEXT NOT NULL DEFAULT 'GPU',
            brand        TEXT,
            model        TEXT,
            slug         TEXT,
            canonical_id INTEGER REFERENCES products(id),
            created_at   TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """


def _ddl_offers() -> str:
    if _IS_POSTGRES:
        return """
        CREATE TABLE IF NOT EXISTS offers (
            id         SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id),
            source     TEXT NOT NULL,
            source_id  TEXT,
            price      INTEGER NOT NULL,
            url        TEXT,
            in_stock   INTEGER NOT NULL DEFAULT 1,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    return """
        CREATE TABLE IF NOT EXISTS offers (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL REFERENCES products(id),
            source     TEXT NOT NULL,
            source_id  TEXT,
            price      INTEGER NOT NULL,
            url        TEXT,
            in_stock   INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """


def _ddl_price_history() -> str:
    if _IS_POSTGRES:
        return """
        CREATE TABLE IF NOT EXISTS price_history (
            id          SERIAL PRIMARY KEY,
            offer_id    INTEGER NOT NULL REFERENCES offers(id),
            price       INTEGER NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    return """
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            offer_id    INTEGER NOT NULL REFERENCES offers(id),
            price       INTEGER NOT NULL,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """


def _ddl_users() -> str:
    if _IS_POSTGRES:
        return """
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            email      TEXT NOT NULL UNIQUE,
            password   TEXT NOT NULL,
            username   TEXT,
            google_id  TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        """
    return """
        CREATE TABLE IF NOT EXISTS users (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT NOT NULL UNIQUE,
            password   TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """


def _ddl_parse_runs() -> str:
    if _IS_POSTGRES:
        return """
        CREATE TABLE IF NOT EXISTS parse_runs (
            id             SERIAL PRIMARY KEY,
            parser_key     TEXT NOT NULL,
            source         TEXT NOT NULL,
            category       TEXT,
            started_at     TIMESTAMP NOT NULL DEFAULT NOW(),
            finished_at    TIMESTAMP,
            status         TEXT NOT NULL DEFAULT 'running',
            error_msg      TEXT,
            expected_total INTEGER,
            items_found    INTEGER NOT NULL DEFAULT 0,
            saved_count    INTEGER NOT NULL DEFAULT 0,
            updated_count  INTEGER NOT NULL DEFAULT 0
        )
        """
    return """
        CREATE TABLE IF NOT EXISTS parse_runs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            parser_key     TEXT NOT NULL,
            source         TEXT NOT NULL,
            category       TEXT,
            started_at     TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at    TEXT,
            status         TEXT NOT NULL DEFAULT 'running',
            error_msg      TEXT,
            expected_total INTEGER,
            items_found    INTEGER NOT NULL DEFAULT 0,
            saved_count    INTEGER NOT NULL DEFAULT 0,
            updated_count  INTEGER NOT NULL DEFAULT 0
        )
    """


def init_db():
    """Создаёт таблицы если их ещё нет + запускает миграции."""
    with engine.connect() as conn:
        conn.execute(text(_ddl_products()))
        conn.execute(text(_ddl_offers()))
        conn.execute(text(_ddl_price_history()))
        conn.execute(text(_ddl_users()))
        conn.execute(text(_ddl_parse_runs()))

        # Индексы
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uix_offer_source ON offers(source, source_id)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_parse_runs_started ON parse_runs(started_at DESC)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_parse_runs_source ON parse_runs(source, category)"
        ))

        if _IS_POSTGRES:
            # GIN-индекс для полнотекстового поиска по fts_tokens
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_products_fts "
                "ON products USING GIN (to_tsvector('simple', COALESCE(fts_tokens, '')))"
            ))
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uix_products_slug ON products(slug)"
            ))
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uix_users_google_id ON users(google_id)"
            ))
        else:
            # SQLite FTS5 виртуальная таблица
            conn.execute(text(
                "CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(tokens)"
            ))

        conn.commit()

    migrate_db()


# ---------------------------------------------------------------------------
# mark_missing_as_out_of_stock
# ---------------------------------------------------------------------------

def mark_missing_as_out_of_stock(source: str, present_ids: list, category: str = None):
    """
    Помечает как in_stock=0 все офферы этого source,
    которых нет в списке present_ids.
    Если передана category — затрагивает только офферы товаров этой категории.
    """
    if not present_ids:
        return
    with engine.connect() as conn:
        if category:
            stmt = text(
                "UPDATE offers SET in_stock=0 "
                "WHERE source=:source AND in_stock=1 "
                "AND source_id NOT IN :ids "
                "AND product_id IN (SELECT id FROM products WHERE category=:category)"
            ).bindparams(bindparam("ids", expanding=True))
            conn.execute(stmt, {"source": source, "ids": list(present_ids), "category": category})
        else:
            stmt = text(
                "UPDATE offers SET in_stock=0 "
                "WHERE source=:source AND in_stock=1 "
                "AND source_id NOT IN :ids"
            ).bindparams(bindparam("ids", expanding=True))
            conn.execute(stmt, {"source": source, "ids": list(present_ids)})
        conn.commit()


# ---------------------------------------------------------------------------
# find_product_by_name
# ---------------------------------------------------------------------------

def find_product_by_name(conn, name):
    """Ищет товар по точному названию."""
    row = conn.execute(
        text("SELECT * FROM products WHERE name = :name"), {"name": name}
    ).mappings().fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# save_products
# ---------------------------------------------------------------------------

def save_products(products, source):
    """
    Сохраняет список товаров в базу.

    Каждый товар — словарь с ключами: id, name, price, url, in_stock.
    source — название магазина.

    Логика:
    - Если оффер (по source + source_id) уже есть — обновляем цену
    - Если цена изменилась — пишем в price_history
    - Если оффера нет — создаём product (если нужно) + offer
    """
    now = datetime.now().isoformat()
    saved = 0
    updated = 0

    with engine.connect() as conn:
        for item in products:
            source_id = str(item["id"])

            existing = conn.execute(
                text(
                    "SELECT o.id, o.price, o.product_id FROM offers o "
                    "WHERE o.source = :source AND o.source_id = :source_id"
                ),
                {"source": source, "source_id": source_id},
            ).mappings().fetchone()

            if existing:
                old_price = existing["price"]
                conn.execute(
                    text(
                        "UPDATE offers SET price=:price, in_stock=:in_stock, "
                        "url=:url, updated_at=:updated_at WHERE id=:id"
                    ),
                    {
                        "price": item["price"],
                        "in_stock": int(item["in_stock"]),
                        "url": item["url"],
                        "updated_at": now,
                        "id": existing["id"],
                    },
                )
                if old_price != item["price"]:
                    conn.execute(
                        text(
                            "INSERT INTO price_history (offer_id, price, recorded_at) "
                            "VALUES (:offer_id, :price, :recorded_at)"
                        ),
                        {"offer_id": existing["id"], "price": item["price"], "recorded_at": now},
                    )
                updated += 1
            else:
                product = find_product_by_name(conn, item["name"])
                if product:
                    product_id = product["id"]
                    new_cat = item.get("category", "GPU")
                    if product["category"] != new_cat:
                        conn.execute(
                            text("UPDATE products SET category = :cat WHERE id = :id"),
                            {"cat": new_cat, "id": product_id},
                        )
                else:
                    slug = _unique_slug(conn, item["name"])
                    fts_tok = _fts_tokens(item["name"])
                    if _IS_POSTGRES:
                        product_id = conn.execute(
                            text(
                                "INSERT INTO products (name, category, slug, fts_tokens) "
                                "VALUES (:name, :category, :slug, :fts_tokens) RETURNING id"
                            ),
                            {
                                "name": item["name"],
                                "category": item.get("category", "GPU"),
                                "slug": slug,
                                "fts_tokens": fts_tok,
                            },
                        ).scalar()
                    else:
                        product_id = conn.execute(
                            text(
                                "INSERT INTO products (name, category, slug) "
                                "VALUES (:name, :category, :slug) RETURNING id"
                            ),
                            {
                                "name": item["name"],
                                "category": item.get("category", "GPU"),
                                "slug": slug,
                            },
                        ).scalar()
                        conn.execute(
                            text(
                                "INSERT INTO products_fts(rowid, tokens) VALUES (:rowid, :tokens)"
                            ),
                            {"rowid": product_id, "tokens": fts_tok},
                        )

                offer_id = conn.execute(
                    text(
                        "INSERT INTO offers "
                        "(product_id, source, source_id, price, url, in_stock, updated_at) "
                        "VALUES (:product_id, :source, :source_id, :price, :url, :in_stock, :updated_at) "
                        "RETURNING id"
                    ),
                    {
                        "product_id": product_id,
                        "source": source,
                        "source_id": source_id,
                        "price": item["price"],
                        "url": item["url"],
                        "in_stock": int(item["in_stock"]),
                        "updated_at": now,
                    },
                ).scalar()
                conn.execute(
                    text(
                        "INSERT INTO price_history (offer_id, price, recorded_at) "
                        "VALUES (:offer_id, :price, :recorded_at)"
                    ),
                    {"offer_id": offer_id, "price": item["price"], "recorded_at": now},
                )
                saved += 1

        conn.commit()

    return saved, updated


# ---------------------------------------------------------------------------
# get_all_offers
# ---------------------------------------------------------------------------

def get_all_offers():
    """Возвращает все предложения с названиями товаров."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT p.name, o.source, o.price, o.url, o.in_stock, o.updated_at
            FROM offers o
            JOIN products p ON p.id = o.product_id
            ORDER BY p.name, o.price
        """)).mappings().fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# migrate_db
# ---------------------------------------------------------------------------

def migrate_db():
    """Добавляет новые колонки и выполняет очистку данных."""
    with engine.connect() as conn:

        if not _IS_POSTGRES:
            # --- SQLite-only: добавление колонок ---
            product_cols = _get_columns(conn, "products")

            if "canonical_id" not in product_cols:
                conn.execute(text(
                    "ALTER TABLE products ADD COLUMN canonical_id INTEGER REFERENCES products(id)"
                ))
                conn.commit()

            user_cols = _get_columns(conn, "users")
            if "username" not in user_cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN username TEXT"))
                conn.commit()
            if "google_id" not in user_cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN google_id TEXT"))
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uix_users_google_id ON users(google_id)"
                ))
                conn.commit()

            product_cols = _get_columns(conn, "products")
            if "slug" not in product_cols:
                conn.execute(text("ALTER TABLE products ADD COLUMN slug TEXT"))
                conn.commit()
                rows = conn.execute(text(
                    "SELECT id, name FROM products WHERE slug IS NULL"
                )).mappings().fetchall()
                for row in rows:
                    slug = _unique_slug(conn, row["name"], exclude_id=row["id"])
                    conn.execute(
                        text("UPDATE products SET slug = :slug WHERE id = :id"),
                        {"slug": slug, "id": row["id"]},
                    )
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uix_products_slug ON products(slug)"
                ))
                conn.commit()

            # parse_runs — добавляем таблицу если нет
            tables = _get_tables(conn)
            if "parse_runs" not in tables:
                conn.execute(text(_ddl_parse_runs()))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_parse_runs_started "
                    "ON parse_runs(started_at DESC)"
                ))
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_parse_runs_source "
                    "ON parse_runs(source, category)"
                ))
                conn.commit()

            # FTS5 — добавляем виртуальную таблицу если нет
            tables = _get_tables(conn)
            if "products_fts" not in tables:
                conn.execute(text(
                    "CREATE VIRTUAL TABLE products_fts USING fts5(tokens)"
                ))
                conn.commit()

            fts_count = conn.execute(
                text("SELECT COUNT(*) FROM products_fts")
            ).scalar()
            if fts_count == 0:
                rows = conn.execute(
                    text("SELECT id, name FROM products")
                ).mappings().fetchall()
                for r in rows:
                    conn.execute(
                        text(
                            "INSERT INTO products_fts(rowid, tokens) "
                            "VALUES (:rowid, :tokens)"
                        ),
                        {"rowid": r["id"], "tokens": _fts_tokens(r["name"])},
                    )
                conn.commit()

        else:
            # --- PostgreSQL-only: добавление колонок ---
            product_cols = _get_columns(conn, "products")
            if "canonical_id" not in product_cols:
                conn.execute(text(
                    "ALTER TABLE products ADD COLUMN IF NOT EXISTS "
                    "canonical_id INTEGER REFERENCES products(id)"
                ))
                conn.commit()
            if "fts_tokens" not in product_cols:
                conn.execute(text(
                    "ALTER TABLE products ADD COLUMN IF NOT EXISTS fts_tokens TEXT"
                ))
                conn.commit()
            if "slug" not in product_cols:
                conn.execute(text(
                    "ALTER TABLE products ADD COLUMN IF NOT EXISTS slug TEXT"
                ))
                conn.commit()

            user_cols = _get_columns(conn, "users")
            if "username" not in user_cols:
                conn.execute(text(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT"
                ))
                conn.commit()
            if "google_id" not in user_cols:
                conn.execute(text(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS google_id TEXT"
                ))
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uix_users_google_id ON users(google_id)"
                ))
                conn.commit()

            # Заполняем slug для товаров у которых его нет
            rows = conn.execute(text(
                "SELECT id, name FROM products WHERE slug IS NULL"
            )).mappings().fetchall()
            for row in rows:
                slug = _unique_slug(conn, row["name"], exclude_id=row["id"])
                conn.execute(
                    text("UPDATE products SET slug = :slug WHERE id = :id"),
                    {"slug": slug, "id": row["id"]},
                )
            if rows:
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uix_products_slug ON products(slug)"
                ))
                conn.commit()

            # Заполняем fts_tokens для PostgreSQL если нет
            fts_null_count = conn.execute(
                text("SELECT COUNT(*) FROM products WHERE fts_tokens IS NULL")
            ).scalar()
            if fts_null_count > 0:
                rows = conn.execute(text(
                    "SELECT id, name FROM products WHERE fts_tokens IS NULL"
                )).mappings().fetchall()
                for r in rows:
                    conn.execute(
                        text("UPDATE products SET fts_tokens = :tok WHERE id = :id"),
                        {"tok": _fts_tokens(r["name"]), "id": r["id"]},
                    )
                conn.commit()

        # --- Общие шаги для обоих диалектов ---

        # Нормализуем source: 'citilink-gpu' → 'citilink', 'regard-cpu' → 'regard'
        for prefix in ("citilink", "regard"):
            has_stale = conn.execute(
                text("SELECT 1 FROM offers WHERE source LIKE :pat LIMIT 1"),
                {"pat": f"{prefix}-%"},
            ).fetchone()
            if not has_stale:
                continue
            conn.execute(
                text(
                    "DELETE FROM price_history WHERE offer_id IN ("
                    "  SELECT id FROM offers WHERE source LIKE :pat "
                    "  AND source_id IN (SELECT source_id FROM offers WHERE source = :src)"
                    ")"
                ),
                {"pat": f"{prefix}-%", "src": prefix},
            )
            conn.execute(
                text(
                    "DELETE FROM offers WHERE source LIKE :pat "
                    "AND source_id IN (SELECT source_id FROM offers WHERE source = :src)"
                ),
                {"pat": f"{prefix}-%", "src": prefix},
            )
            conn.execute(
                text("UPDATE offers SET source = :src WHERE source LIKE :pat"),
                {"src": prefix, "pat": f"{prefix}-%"},
            )
        conn.commit()

        # Зависшие runs — помечаем как failed
        conn.execute(text(
            "UPDATE parse_runs SET status='failed', error_msg='Process was killed' "
            "WHERE status='running'"
        ))
        conn.commit()

        # Исправляем Mvideo URLs: /product/{id} → /products/{id}
        conn.execute(text(
            "UPDATE offers "
            "SET url = REPLACE(url, 'mvideo.ru/product/', 'mvideo.ru/products/') "
            "WHERE source = 'mvideo' AND url LIKE '%mvideo.ru/product/%'"
        ))
        conn.commit()


# ---------------------------------------------------------------------------
# parse_runs
# ---------------------------------------------------------------------------

def start_parse_run(parser_key: str, source: str, category: str = None) -> int:
    """Создаёт запись о начале запуска, возвращает run_id."""
    with engine.connect() as conn:
        run_id = conn.execute(
            text(
                "INSERT INTO parse_runs (parser_key, source, category, status) "
                "VALUES (:parser_key, :source, :category, 'running') RETURNING id"
            ),
            {"parser_key": parser_key, "source": source, "category": category},
        ).scalar()
        conn.commit()
        return run_id


def finish_parse_run(run_id: int, status: str, items_found: int,
                     saved: int, updated: int,
                     expected_total: int = None, error_msg: str = None):
    """Завершает запись о запуске."""
    now_expr = "NOW()" if _IS_POSTGRES else "datetime('now')"
    with engine.connect() as conn:
        conn.execute(
            text(
                f"UPDATE parse_runs SET finished_at={now_expr}, status=:status, "
                "items_found=:items_found, saved_count=:saved_count, "
                "updated_count=:updated_count, expected_total=:expected_total, "
                "error_msg=:error_msg WHERE id=:id"
            ),
            {
                "status": status,
                "items_found": items_found,
                "saved_count": saved,
                "updated_count": updated,
                "expected_total": expected_total,
                "error_msg": error_msg,
                "id": run_id,
            },
        )
        conn.commit()


def get_parse_runs(source: str = None, limit: int = 100) -> list:
    """История запусков для страницы аудита."""
    with engine.connect() as conn:
        if source:
            rows = conn.execute(
                text(
                    "SELECT * FROM parse_runs WHERE source=:source "
                    "ORDER BY started_at DESC LIMIT :limit"
                ),
                {"source": source, "limit": limit},
            ).mappings().fetchall()
        else:
            rows = conn.execute(
                text("SELECT * FROM parse_runs ORDER BY started_at DESC LIMIT :limit"),
                {"limit": limit},
            ).mappings().fetchall()
        return [dict(r) for r in rows]


def get_audit_summary() -> list:
    """
    Агрегированный отчёт: последний завершённый run по каждой (source, category)
    с количеством офферов в БД.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH last_runs AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY source, category
                           ORDER BY started_at DESC
                       ) AS rn
                FROM parse_runs
                WHERE status != 'running'
            ),
            db_counts AS (
                SELECT o.source, p.category, COUNT(*) AS db_count
                FROM offers o
                JOIN products p ON p.id = o.product_id
                GROUP BY o.source, p.category
            ),
            parsed_sources AS (
                SELECT DISTINCT source FROM parse_runs
            )
            SELECT lr.source, lr.category, lr.started_at AS last_run,
                   lr.status, lr.items_found, lr.expected_total,
                   COALESCE(dc.db_count, 0) AS db_count
            FROM last_runs lr
            LEFT JOIN db_counts dc
                ON dc.source = lr.source
               AND dc.category = lr.category
            WHERE lr.rn = 1

            UNION ALL

            SELECT dc.source, dc.category,
                   NULL AS last_run, 'never' AS status,
                   NULL AS items_found, NULL AS expected_total,
                   dc.db_count
            FROM db_counts dc
            WHERE dc.source NOT IN (SELECT source FROM parsed_sources)

            ORDER BY source, category
        """)).mappings().fetchall()
        result = []
        for r in rows:
            row = dict(r)
            if row["expected_total"] and row["items_found"] is not None:
                row["coverage_pct"] = round(
                    row["items_found"] / row["expected_total"] * 100, 1
                )
            else:
                row["coverage_pct"] = None
            result.append(row)
        return result


# ---------------------------------------------------------------------------
# search_products
# ---------------------------------------------------------------------------

def search_products(query=None, brand=None, chip=None, sources=None, category=None):
    """
    Поиск товаров с группировкой по canonical_id.
    Возвращает список товаров, каждый с вложенным списком offers.
    """
    with engine.connect() as conn:
        sql = (
            "SELECT p.id, p.name, p.brand, p.model, p.category, p.slug, "
            "p.canonical_id, COALESCE(p.canonical_id, p.id) AS group_id "
            "FROM products p WHERE 1=1"
        )
        params: dict = {}

        if category:
            sql += " AND p.category = :category"
            params["category"] = category
        if brand:
            sql += " AND p.brand = :brand"
            params["brand"] = brand
        if chip:
            sql += " AND (p.model LIKE :chip1 OR p.name LIKE :chip2)"
            params["chip1"] = f"%{chip}%"
            params["chip2"] = f"%{chip}%"
        if query:
            if _IS_POSTGRES:
                fts_expr = _build_pg_fts_query(query)
                if fts_expr:
                    sql += (
                        " AND to_tsvector('simple', COALESCE(p.fts_tokens, '')) "
                        "@@ to_tsquery('simple', :fts_expr)"
                    )
                    params["fts_expr"] = fts_expr
            else:
                fts_expr = _build_fts_query(query)
                if fts_expr:
                    sql += " AND p.id IN (SELECT rowid FROM products_fts WHERE tokens MATCH :fts_expr)"
                    params["fts_expr"] = fts_expr

        sql += " ORDER BY p.name"
        products_list = [
            dict(r) for r in conn.execute(text(sql), params).mappings().fetchall()
        ]

        groups = {}
        for p in products_list:
            gid = p["group_id"]
            if gid not in groups:
                groups[gid] = {"product": p, "offers": []}

        if not groups:
            return []

        group_ids = list(groups.keys())
        related = conn.execute(
            text(
                "SELECT id, COALESCE(canonical_id, id) AS group_id "
                "FROM products WHERE COALESCE(canonical_id, id) IN :gids"
            ).bindparams(bindparam("gids", expanding=True)),
            {"gids": group_ids},
        ).mappings().fetchall()
        all_product_ids = [r["id"] for r in related]

        offers = conn.execute(
            text(
                "SELECT o.*, p.name as product_name, "
                "COALESCE(p.canonical_id, p.id) AS group_id "
                "FROM offers o JOIN products p ON p.id = o.product_id "
                "WHERE o.product_id IN :pids AND o.in_stock = 1 "
                "ORDER BY o.price"
            ).bindparams(bindparam("pids", expanding=True)),
            {"pids": all_product_ids},
        ).mappings().fetchall()

        for o in offers:
            o = dict(o)
            if sources and o["source"] not in sources:
                continue
            gid = o["group_id"]
            if gid in groups:
                groups[gid]["offers"].append(o)

        groups = {gid: g for gid, g in groups.items() if g["offers"]}
        return sorted(
            groups.values(),
            key=lambda g: g["offers"][0]["price"] if g["offers"] else float("inf"),
        )


# ---------------------------------------------------------------------------
# _attach_offers  (internal helper)
# ---------------------------------------------------------------------------

def _attach_offers(conn, product: dict) -> dict:
    """Добавляет список offers к словарю товара (включая canonical-пары)."""
    canonical_id = product.get("canonical_id") or product["id"]
    related = conn.execute(
        text(
            "SELECT id FROM products WHERE id = :cid OR canonical_id = :cid2"
        ),
        {"cid": canonical_id, "cid2": canonical_id},
    ).mappings().fetchall()
    related_ids = [r["id"] for r in related]

    offers = conn.execute(
        text(
            "SELECT o.*, p.name as product_name "
            "FROM offers o JOIN products p ON p.id = o.product_id "
            "WHERE o.product_id IN :pids AND o.in_stock = 1 "
            "ORDER BY o.price"
        ).bindparams(bindparam("pids", expanding=True)),
        {"pids": related_ids},
    ).mappings().fetchall()
    product["offers"] = [dict(o) for o in offers]
    return product


def get_product_by_slug_with_offers(slug: str):
    """Возвращает товар по slug со всеми предложениями."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM products WHERE slug = :slug"), {"slug": slug}
        ).mappings().fetchone()
        if not row:
            return None
        return _attach_offers(conn, dict(row))


def get_product_with_offers(product_id):
    """Возвращает товар со всеми предложениями (включая связанные через canonical_id)."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM products WHERE id = :id"), {"id": product_id}
        ).mappings().fetchone()
        if not row:
            return None
        return _attach_offers(conn, dict(row))


def get_products_with_offers_bulk(product_ids: list) -> dict:
    """Возвращает dict {product_id: product_with_offers} тремя запросами (без N+1)."""
    if not product_ids:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM products WHERE id IN :ids").bindparams(
                bindparam("ids", expanding=True)
            ),
            {"ids": product_ids},
        ).mappings().fetchall()
        products = {r["id"]: dict(r) for r in rows}

        canonical_ids = list({p.get("canonical_id") or p["id"] for p in products.values()})
        related = conn.execute(
            text(
                "SELECT id, COALESCE(canonical_id, id) AS canonical "
                "FROM products WHERE COALESCE(canonical_id, id) IN :cids"
            ).bindparams(bindparam("cids", expanding=True)),
            {"cids": canonical_ids},
        ).mappings().fetchall()
        all_related_ids = [r["id"] for r in related]
        pid_to_canonical = {r["id"]: r["canonical"] for r in related}

        offers = conn.execute(
            text(
                "SELECT o.*, p.name as product_name, "
                "COALESCE(p.canonical_id, p.id) AS canonical "
                "FROM offers o JOIN products p ON p.id = o.product_id "
                "WHERE o.product_id IN :oids AND o.in_stock = 1 "
                "ORDER BY o.price"
            ).bindparams(bindparam("oids", expanding=True)),
            {"oids": all_related_ids},
        ).mappings().fetchall()

        offers_by_canonical: dict = {}
        for o in offers:
            offers_by_canonical.setdefault(o["canonical"], []).append(dict(o))

        result = {}
        for pid, product in products.items():
            canonical = pid_to_canonical.get(pid) or product.get("canonical_id") or pid
            product["offers"] = offers_by_canonical.get(canonical, [])
            result[pid] = product
        return result


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

def get_price_history_for_product(product_id):
    """Возвращает историю цен для всех offers связанных с товаром."""
    with engine.connect() as conn:
        product = conn.execute(
            text("SELECT * FROM products WHERE id = :id"), {"id": product_id}
        ).mappings().fetchone()
        if not product:
            return []

        canonical_id = product["canonical_id"] or product["id"]
        related = conn.execute(
            text(
                "SELECT id FROM products WHERE id = :cid OR canonical_id = :cid2"
            ),
            {"cid": canonical_id, "cid2": canonical_id},
        ).mappings().fetchall()
        related_ids = [r["id"] for r in related]

        rows = conn.execute(
            text(
                "SELECT ph.price, ph.recorded_at, o.source "
                "FROM price_history ph "
                "JOIN offers o ON o.id = ph.offer_id "
                "WHERE o.product_id IN :pids "
                "ORDER BY ph.recorded_at"
            ).bindparams(bindparam("pids", expanding=True)),
            {"pids": related_ids},
        ).mappings().fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Filter options / Stats
# ---------------------------------------------------------------------------

def get_filter_options(category=None):
    """Возвращает бренды, модели и источники для фильтров."""
    with engine.connect() as conn:
        if category:
            brands = [
                r["brand"] for r in conn.execute(
                    text(
                        "SELECT DISTINCT brand FROM products "
                        "WHERE brand IS NOT NULL AND category = :cat ORDER BY brand"
                    ),
                    {"cat": category},
                ).mappings().fetchall()
            ]
            models = [
                r["model"] for r in conn.execute(
                    text(
                        "SELECT DISTINCT model FROM products "
                        "WHERE model IS NOT NULL AND category = :cat ORDER BY model"
                    ),
                    {"cat": category},
                ).mappings().fetchall()
            ]
        else:
            brands = [
                r["brand"] for r in conn.execute(
                    text("SELECT DISTINCT brand FROM products WHERE brand IS NOT NULL ORDER BY brand")
                ).mappings().fetchall()
            ]
            models = [
                r["model"] for r in conn.execute(
                    text("SELECT DISTINCT model FROM products WHERE model IS NOT NULL ORDER BY model")
                ).mappings().fetchall()
            ]
        sources = [
            r["source"] for r in conn.execute(
                text("SELECT DISTINCT source FROM offers ORDER BY source")
            ).mappings().fetchall()
        ]
        return {"brands": brands, "models": models, "sources": sources}


def get_stats():
    """Статистика для главной страницы."""
    with engine.connect() as conn:
        total_products = conn.execute(
            text("SELECT COUNT(*) c FROM products")
        ).mappings().fetchone()["c"]
        total_offers = conn.execute(
            text("SELECT COUNT(*) c FROM offers")
        ).mappings().fetchone()["c"]
        sources = conn.execute(
            text("SELECT COUNT(DISTINCT source) c FROM offers")
        ).mappings().fetchone()["c"]
        matched = conn.execute(
            text("SELECT COUNT(*) c FROM products WHERE canonical_id IS NOT NULL")
        ).mappings().fetchone()["c"]
        min_price = conn.execute(
            text("SELECT MIN(price) c FROM offers")
        ).mappings().fetchone()["c"] or 0
        max_price = conn.execute(
            text("SELECT MAX(price) c FROM offers")
        ).mappings().fetchone()["c"] or 0
        return {
            "total_products": total_products,
            "total_offers": total_offers,
            "sources": sources,
            "matched": matched,
            "min_price": min_price,
            "max_price": max_price,
        }


def get_store_offer_counts() -> dict:
    """Возвращает {source: offer_count} для магазинов с данными."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source, COUNT(*) as cnt FROM offers "
                "WHERE in_stock=1 GROUP BY source ORDER BY cnt DESC"
            )
        ).mappings().fetchall()
        return {r["source"]: r["cnt"] for r in rows}


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------

def create_user(email, password_hash):
    with engine.connect() as conn:
        try:
            user_id = conn.execute(
                text(
                    "INSERT INTO users (email, password) "
                    "VALUES (:email, :password) RETURNING id"
                ),
                {"email": email, "password": password_hash},
            ).scalar()
            conn.commit()
            return user_id
        except IntegrityError:
            conn.rollback()
            return None


def get_user_by_email(email):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM users WHERE email = :email"), {"email": email}
        ).mappings().fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM users WHERE id = :id"), {"id": user_id}
        ).mappings().fetchone()
        return dict(row) if row else None


def update_user_profile(user_id, username=None):
    """Обновляет профиль пользователя."""
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE users SET username = :username WHERE id = :id"),
            {"username": username, "id": user_id},
        )
        conn.commit()


def delete_user(user_id):
    """Удаляет пользователя."""
    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM users WHERE id = :id"), {"id": user_id}
        )
        conn.commit()


def get_user_by_google_id(google_id):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM users WHERE google_id = :google_id"),
            {"google_id": google_id},
        ).mappings().fetchone()
        return dict(row) if row else None


def link_google_account(user_id, google_id):
    with engine.connect() as conn:
        try:
            conn.execute(
                text("UPDATE users SET google_id = :google_id WHERE id = :id"),
                {"google_id": google_id, "id": user_id},
            )
            conn.commit()
            return True
        except IntegrityError:
            conn.rollback()
            return False


def create_user_google(email, google_id):
    """Создаёт пользователя через Google OAuth (без пароля)."""
    with engine.connect() as conn:
        try:
            user_id = conn.execute(
                text(
                    "INSERT INTO users (email, password, google_id) "
                    "VALUES (:email, '', :google_id) RETURNING id"
                ),
                {"email": email, "google_id": google_id},
            ).scalar()
            conn.commit()
            return user_id
        except IntegrityError:
            conn.rollback()
            return None
