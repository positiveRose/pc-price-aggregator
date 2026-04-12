"""
Модуль базы данных — SQLite хранилище товаров и цен.

Таблицы:
- products     — уникальные товары (после нормализации)
- offers       — предложения с каждого магазина (цена, ссылка, наличие)
- price_history — история изменения цен
"""

import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "prices.db"


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Создаёт таблицы если их ещё нет."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            category    TEXT NOT NULL DEFAULT 'GPU',
            brand       TEXT,
            model       TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS offers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  INTEGER NOT NULL REFERENCES products(id),
            source      TEXT NOT NULL,          -- 'citilink', 'regard', 'dns'
            source_id   TEXT,                   -- ID товара на площадке
            price       INTEGER NOT NULL,
            url         TEXT,
            in_stock    INTEGER NOT NULL DEFAULT 1,
            updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            offer_id    INTEGER NOT NULL REFERENCES offers(id),
            price       INTEGER NOT NULL,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS uix_offer_source
            ON offers(source, source_id);
    """)
    conn.commit()
    conn.close()


def find_product_by_name(conn, name):
    """Ищет товар по точному названию."""
    row = conn.execute(
        "SELECT * FROM products WHERE name = ?", (name,)
    ).fetchone()
    return row


def save_products(products, source):
    """
    Сохраняет список товаров в базу.

    Каждый товар — словарь с ключами: id, name, price, url, in_stock.
    source — название магазина ('citilink', 'dns').

    Логика:
    - Если товар (по source + source_id) уже есть — обновляем цену
    - Если цена изменилась — пишем в price_history
    - Если товара нет — создаём product + offer
    """
    conn = get_connection()
    now = datetime.now().isoformat()
    saved = 0
    updated = 0

    try:
        for item in products:
            source_id = str(item["id"])

            # Проверяем есть ли уже такое предложение
            existing = conn.execute(
                "SELECT o.id, o.price, o.product_id FROM offers o "
                "WHERE o.source = ? AND o.source_id = ?",
                (source, source_id),
            ).fetchone()

            if existing:
                old_price = existing["price"]
                # Обновляем предложение
                conn.execute(
                    "UPDATE offers SET price=?, in_stock=?, updated_at=? "
                    "WHERE id=?",
                    (item["price"], int(item["in_stock"]), now, existing["id"]),
                )
                # Если цена изменилась — записываем в историю
                if old_price != item["price"]:
                    conn.execute(
                        "INSERT INTO price_history (offer_id, price, recorded_at) "
                        "VALUES (?, ?, ?)",
                        (existing["id"], item["price"], now),
                    )
                updated += 1
            else:
                # Ищем или создаём продукт
                product = find_product_by_name(conn, item["name"])
                if product:
                    product_id = product["id"]
                else:
                    cur = conn.execute(
                        "INSERT INTO products (name) VALUES (?)",
                        (item["name"],),
                    )
                    product_id = cur.lastrowid

                # Создаём предложение
                cur = conn.execute(
                    "INSERT INTO offers (product_id, source, source_id, price, url, in_stock, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (product_id, source, source_id, item["price"], item["url"],
                     int(item["in_stock"]), now),
                )
                # Первая запись в историю цен
                conn.execute(
                    "INSERT INTO price_history (offer_id, price, recorded_at) "
                    "VALUES (?, ?, ?)",
                    (cur.lastrowid, item["price"], now),
                )
                saved += 1

        conn.commit()
    finally:
        conn.close()

    return saved, updated


def get_all_offers():
    """Возвращает все предложения с названиями товаров."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT p.name, o.source, o.price, o.url, o.in_stock, o.updated_at
            FROM offers o
            JOIN products p ON p.id = o.product_id
            ORDER BY p.name, o.price
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# Создаём таблицы при первом импорте
init_db()
