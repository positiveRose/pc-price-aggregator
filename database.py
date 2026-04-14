"""
Модуль базы данных — SQLite хранилище товаров и цен.

Таблицы:
- products     — уникальные товары (после нормализации)
- offers       — предложения с каждого магазина (цена, ссылка, наличие)
- price_history — история изменения цен
"""

import re
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "prices.db"


def _name_tokens(name: str) -> list:
    """Разбивает название на токены для поиска.
    'RTX5060' → ['rtx', '5060', 'rtx5060'] — ищем и слипшееся и раздельное."""
    s = name.lower()
    # Исходные слова
    raw = re.findall(r"[a-zа-яё0-9]+", s)
    # Разбиваем слипшиеся буква+цифра: 'rtx5060' → 'rtx 5060'
    split = re.sub(r"([a-zа-яё])(\d)", r"\1 \2", s)
    split = re.sub(r"(\d)([a-zа-яё])", r"\1 \2", split)
    expanded = re.findall(r"[a-zа-яё0-9]+", split)
    return list(set(raw + expanded))


def _make_slug(name: str) -> str:
    """Генерирует URL-slug из названия товара."""
    s = name.lower()
    s = re.sub(r'[^\x00-\x7f]', ' ', s)   # кириллицу → пробел
    s = re.sub(r'[^a-z0-9\s]', ' ', s)     # спецсимволы → пробел
    s = re.sub(r'\s+', '-', s.strip())
    s = re.sub(r'-+', '-', s)
    return s.strip('-')[:80]


def _unique_slug(conn, name: str, exclude_id: int = None) -> str:
    """Возвращает уникальный slug, добавляя -2, -3 при конфликтах."""
    base = _make_slug(name) or "product"
    slug = base
    counter = 2
    while True:
        row = conn.execute("SELECT id FROM products WHERE slug = ?", (slug,)).fetchone()
        if not row or (exclude_id is not None and row["id"] == exclude_id):
            return slug
        slug = f"{base}-{counter}"
        counter += 1


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Создаёт таблицы если их ещё нет + миграции."""
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
            source      TEXT NOT NULL,          -- 'citilink', 'regard', 'oldi', 'e2e4', 'mvideo', 'eldorado', 'dns'
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

        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            email       TEXT NOT NULL UNIQUE,
            password    TEXT NOT NULL,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()
    migrate_db()


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
                    # Обновляем категорию если она поменялась
                    new_cat = item.get("category", "GPU")
                    if product["category"] != new_cat:
                        conn.execute(
                            "UPDATE products SET category = ? WHERE id = ?",
                            (new_cat, product_id),
                        )
                else:
                    slug = _unique_slug(conn, item["name"])
                    cur = conn.execute(
                        "INSERT INTO products (name, category, slug) VALUES (?, ?, ?)",
                        (item["name"], item.get("category", "GPU"), slug),
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


def migrate_db():
    """Добавляет новые колонки если их ещё нет."""
    conn = get_connection()
    product_cols = {r["name"] for r in conn.execute("PRAGMA table_info(products)")}
    if "canonical_id" not in product_cols:
        conn.execute("ALTER TABLE products ADD COLUMN canonical_id INTEGER REFERENCES products(id)")
        conn.commit()
    user_cols = {r["name"] for r in conn.execute("PRAGMA table_info(users)")}
    if "username" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
        conn.commit()
    if "google_id" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN google_id TEXT")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uix_users_google_id ON users(google_id)")
        conn.commit()

    # Добавляем slug если нет
    product_cols = {r["name"] for r in conn.execute("PRAGMA table_info(products)")}
    if "slug" not in product_cols:
        conn.execute("ALTER TABLE products ADD COLUMN slug TEXT")
        conn.commit()
        # Заполняем slugs для существующих товаров
        rows = conn.execute("SELECT id, name FROM products WHERE slug IS NULL").fetchall()
        for row in rows:
            slug = _unique_slug(conn, row["name"], exclude_id=row["id"])
            conn.execute("UPDATE products SET slug = ? WHERE id = ?", (slug, row["id"]))
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uix_products_slug ON products(slug)"
        )
        conn.commit()

    # Нормализуем source: 'citilink-gpu' → 'citilink', 'regard-cpu' → 'regard'
    for prefix in ("citilink", "regard"):
        has_stale = conn.execute(
            "SELECT 1 FROM offers WHERE source LIKE ? LIMIT 1", (f"{prefix}-%",)
        ).fetchone()
        if not has_stale:
            continue
        conn.execute(f"""
            DELETE FROM price_history
            WHERE offer_id IN (
                SELECT id FROM offers
                WHERE source LIKE '{prefix}-%'
                  AND source_id IN (
                    SELECT source_id FROM offers WHERE source = '{prefix}'
                  )
            )
        """)
        conn.execute(f"""
            DELETE FROM offers
            WHERE source LIKE '{prefix}-%'
              AND source_id IN (
                SELECT source_id FROM offers WHERE source = '{prefix}'
              )
        """)
        conn.execute(f"UPDATE offers SET source = '{prefix}' WHERE source LIKE '{prefix}-%'")
    conn.commit()
    conn.close()


def search_products(query=None, brand=None, chip=None, sources=None):
    """
    Поиск товаров с группировкой по canonical_id.
    Возвращает список товаров, каждый с вложенным списком offers.
    Подтягивает offers всех связанных через canonical_id товаров.
    """
    conn = get_connection()
    try:
        # Загружаем все товары с фильтрами
        sql = """
            SELECT p.id, p.name, p.brand, p.model, p.category, p.slug,
                   p.canonical_id, COALESCE(p.canonical_id, p.id) AS group_id
            FROM products p
            WHERE 1=1
        """
        params = []

        if brand:
            sql += " AND p.brand = ?"
            params.append(brand)
        if chip:
            sql += " AND p.model LIKE ?"
            params.append(f"%{chip}%")

        sql += " ORDER BY p.name"
        products = [dict(r) for r in conn.execute(sql, params).fetchall()]

        if query:
            query_words = query.lower().split()
            products = [
                p for p in products
                if all(
                    any(w.startswith(qw) for w in _name_tokens(p["name"]))
                    for qw in query_words
                )
            ]

        # Группируем по canonical_id
        groups = {}
        for p in products:
            gid = p["group_id"]
            if gid not in groups:
                groups[gid] = {"product": p, "offers": []}

        if not groups:
            return []

        # Собираем ВСЕ связанные product_id (включая canonical-пары)
        group_ids = list(groups.keys())
        placeholders = ",".join("?" * len(group_ids))
        related = conn.execute(f"""
            SELECT id, COALESCE(canonical_id, id) AS group_id
            FROM products
            WHERE COALESCE(canonical_id, id) IN ({placeholders})
        """, group_ids).fetchall()
        all_product_ids = [r["id"] for r in related]

        # Загружаем offers для ВСЕХ связанных товаров
        placeholders = ",".join("?" * len(all_product_ids))
        offers = conn.execute(f"""
            SELECT o.*, p.name as product_name, COALESCE(p.canonical_id, p.id) AS group_id
            FROM offers o
            JOIN products p ON p.id = o.product_id
            WHERE o.product_id IN ({placeholders})
            ORDER BY o.price
        """, all_product_ids).fetchall()

        for o in offers:
            o = dict(o)
            if sources and o["source"] not in sources:
                continue
            gid = o["group_id"]
            if gid in groups:
                groups[gid]["offers"].append(o)

        # Убираем товары без офферов после фильтрации по источникам
        if sources:
            groups = {gid: g for gid, g in groups.items() if g["offers"]}

        return sorted(
            groups.values(),
            key=lambda g: g["offers"][0]["price"] if g["offers"] else float("inf"),
        )
    finally:
        conn.close()


def _attach_offers(conn, product: dict) -> dict:
    """Добавляет список offers к словарю товара (включая canonical-пары)."""
    canonical_id = product.get("canonical_id") or product["id"]
    related = conn.execute(
        "SELECT id FROM products WHERE id = ? OR canonical_id = ?",
        (canonical_id, canonical_id),
    ).fetchall()
    related_ids = [r["id"] for r in related]
    placeholders = ",".join("?" * len(related_ids))
    offers = conn.execute(f"""
        SELECT o.*, p.name as product_name
        FROM offers o
        JOIN products p ON p.id = o.product_id
        WHERE o.product_id IN ({placeholders})
        ORDER BY o.price
    """, related_ids).fetchall()
    product["offers"] = [dict(o) for o in offers]
    return product


def get_product_by_slug_with_offers(slug: str):
    """Возвращает товар по slug со всеми предложениями."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM products WHERE slug = ?", (slug,)).fetchone()
        if not row:
            return None
        return _attach_offers(conn, dict(row))
    finally:
        conn.close()


def get_product_with_offers(product_id):
    """Возвращает товар со всеми предложениями (включая связанные через canonical_id)."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        if not row:
            return None
        return _attach_offers(conn, dict(row))
    finally:
        conn.close()


def get_price_history_for_product(product_id):
    """Возвращает историю цен для всех offers связанных с товаром."""
    conn = get_connection()
    try:
        product = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        if not product:
            return []

        canonical_id = product["canonical_id"] or product["id"]
        related = conn.execute(
            "SELECT id FROM products WHERE id = ? OR canonical_id = ?",
            (canonical_id, canonical_id),
        ).fetchall()
        related_ids = [r["id"] for r in related]

        placeholders = ",".join("?" * len(related_ids))
        rows = conn.execute(f"""
            SELECT ph.price, ph.recorded_at, o.source
            FROM price_history ph
            JOIN offers o ON o.id = ph.offer_id
            WHERE o.product_id IN ({placeholders})
            ORDER BY ph.recorded_at
        """, related_ids).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_filter_options():
    """Возвращает уникальные бренды, GPU модели и источники для фильтров."""
    conn = get_connection()
    try:
        brands = [r["brand"] for r in conn.execute(
            "SELECT DISTINCT brand FROM products WHERE brand IS NOT NULL ORDER BY brand"
        ).fetchall()]
        models = [r["model"] for r in conn.execute(
            "SELECT DISTINCT model FROM products WHERE model IS NOT NULL ORDER BY model"
        ).fetchall()]
        sources = [r["source"] for r in conn.execute(
            "SELECT DISTINCT source FROM offers ORDER BY source"
        ).fetchall()]
        return {"brands": brands, "models": models, "sources": sources}
    finally:
        conn.close()


def get_stats():
    """Статистика для главной страницы."""
    conn = get_connection()
    try:
        total_products = conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
        total_offers = conn.execute("SELECT COUNT(*) c FROM offers").fetchone()["c"]
        sources = conn.execute("SELECT COUNT(DISTINCT source) c FROM offers").fetchone()["c"]
        matched = conn.execute(
            "SELECT COUNT(*) c FROM products WHERE canonical_id IS NOT NULL"
        ).fetchone()["c"]
        min_price = conn.execute("SELECT MIN(price) c FROM offers").fetchone()["c"] or 0
        max_price = conn.execute("SELECT MAX(price) c FROM offers").fetchone()["c"] or 0
        return {
            "total_products": total_products,
            "total_offers": total_offers,
            "sources": sources,
            "matched": matched,
            "min_price": min_price,
            "max_price": max_price,
        }
    finally:
        conn.close()


def create_user(email, password_hash):
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO users (email, password) VALUES (?, ?)",
            (email, password_hash),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()


def get_user_by_email(email):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_id(user_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_user_profile(user_id, username=None):
    """Обновляет профиль пользователя."""
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET username = ? WHERE id = ?", (username, user_id))
        conn.commit()
    finally:
        conn.close()


def delete_user(user_id):
    """Удаляет пользователя."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


def get_user_by_google_id(google_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM users WHERE google_id = ?", (google_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def link_google_account(user_id, google_id):
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET google_id = ? WHERE id = ?", (google_id, user_id))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def create_user_google(email, google_id):
    """Создаёт пользователя через Google OAuth (без пароля)."""
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO users (email, password, google_id) VALUES (?, '', ?)",
            (email, google_id),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()


