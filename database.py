"""
Модуль базы данных — SQLite хранилище товаров и цен.

Таблицы:
- products     — уникальные товары (после нормализации)
- offers       — предложения с каждого магазина (цена, ссылка, наличие)
- price_history — история изменения цен
"""

import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

# Путь к базе. На Railway задаётся через DB_PATH=/data/prices.db (Volume).
# Локально — рядом с проектом.
DB_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent / "prices.db")))


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


def _query_word_matches(query_word: str, name_tokens: list) -> bool:
    """Проверяет, подходит ли слово запроса к любому токену названия.
    Поддерживает русское склонение: 'видеокарту' найдёт 'видеокарта' (сравнение стемов).
    """
    for token in name_tokens:
        if token.startswith(query_word):
            return True
        # Для чисто буквенных слов ≥6 символов — сравниваем стемы (первые N-2 букв)
        # 'видеокарту' (10 букв) и 'видеокарта' (10 букв) → stem 'видекарт' == 'видекарт'
        if (len(query_word) >= 6 and query_word.isalpha()
                and len(token) >= 6 and token.isalpha()):
            if token[:-2] == query_word[:-2]:
                return True
    return False


def _fts_tokens(name: str) -> str:
    """Возвращает строку токенов для FTS5-индекса."""
    return " ".join(_name_tokens(name))


def _build_fts_query(query: str) -> str | None:
    """Переводит поисковый запрос в FTS5 MATCH-выражение.

    Для длинных буквенных слов (≥6 символов) использует стем [:-2]
    чтобы находить русские склонения: 'видеокарту' → 'видеокар*'.
    """
    terms = []
    for word in re.findall(r"[а-яёa-z0-9]+", query.lower()):
        if len(word) >= 6 and word.isalpha():
            terms.append(word[:-2] + "*")
        else:
            terms.append(word + "*")
    return " ".join(terms) if terms else None


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
        row = conn.execute("SELECT id FROM products WHERE slug = ?", (slug,)).fetchone()
        if not row or (exclude_id is not None and row["id"] == exclude_id):
            return slug
        slug = f"{base}-{counter}"
        counter += 1


def get_connection():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
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

        CREATE TABLE IF NOT EXISTS parse_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            parser_key    TEXT NOT NULL,
            source        TEXT NOT NULL,
            category      TEXT,
            started_at    TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at   TEXT,
            status        TEXT NOT NULL DEFAULT 'running',
            error_msg     TEXT,
            expected_total INTEGER,
            items_found   INTEGER NOT NULL DEFAULT 0,
            saved_count   INTEGER NOT NULL DEFAULT 0,
            updated_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_parse_runs_started
            ON parse_runs(started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_parse_runs_source
            ON parse_runs(source, category);
    """)
    conn.commit()
    conn.close()
    migrate_db()


def mark_missing_as_out_of_stock(source: str, present_ids: list, category: str = None):
    """
    Помечает как in_stock=0 все офферы этого source,
    которых нет в списке present_ids (товары исчезли со страницы каталога).
    Если передана category — затрагивает только офферы товаров этой категории,
    что важно для магазинов с раздельным парсингом по категориям (WB и др.).
    """
    if not present_ids:
        return
    conn = get_connection()
    try:
        placeholders = ",".join("?" * len(present_ids))
        if category:
            conn.execute(
                f"UPDATE offers SET in_stock=0 "
                f"WHERE source=? AND in_stock=1 AND source_id NOT IN ({placeholders})"
                f" AND product_id IN (SELECT id FROM products WHERE category=?)",
                [source] + list(present_ids) + [category],
            )
        else:
            conn.execute(
                f"UPDATE offers SET in_stock=0 "
                f"WHERE source=? AND in_stock=1 AND source_id NOT IN ({placeholders})",
                [source] + list(present_ids),
            )
        conn.commit()
    finally:
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
                # Обновляем предложение (включая URL — формат ссылок может меняться)
                conn.execute(
                    "UPDATE offers SET price=?, in_stock=?, url=?, updated_at=? "
                    "WHERE id=?",
                    (item["price"], int(item["in_stock"]), item["url"], now, existing["id"]),
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
                    conn.execute(
                        "INSERT INTO products_fts(rowid, tokens) VALUES (?, ?)",
                        (product_id, _fts_tokens(item["name"])),
                    )

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
        conn.execute(
            "DELETE FROM price_history WHERE offer_id IN ("
            "  SELECT id FROM offers WHERE source LIKE ?"
            "  AND source_id IN (SELECT source_id FROM offers WHERE source = ?)"
            ")",
            (f"{prefix}-%", prefix),
        )
        conn.execute(
            "DELETE FROM offers WHERE source LIKE ?"
            "  AND source_id IN (SELECT source_id FROM offers WHERE source = ?)",
            (f"{prefix}-%", prefix),
        )
        conn.execute(
            "UPDATE offers SET source = ? WHERE source LIKE ?",
            (prefix, f"{prefix}-%"),
        )
    conn.commit()

    # parse_runs — итерация 8
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "parse_runs" not in tables:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS parse_runs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                parser_key    TEXT NOT NULL,
                source        TEXT NOT NULL,
                category      TEXT,
                started_at    TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at   TEXT,
                status        TEXT NOT NULL DEFAULT 'running',
                error_msg     TEXT,
                expected_total INTEGER,
                items_found   INTEGER NOT NULL DEFAULT 0,
                saved_count   INTEGER NOT NULL DEFAULT 0,
                updated_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_parse_runs_started
                ON parse_runs(started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_parse_runs_source
                ON parse_runs(source, category);
        """)
        conn.commit()

    # Зависшие runs (процесс был убит) — помечаем как failed
    conn.execute(
        "UPDATE parse_runs SET status='failed', error_msg='Process was killed' "
        "WHERE status='running'"
    )
    conn.commit()

    # FTS5 индекс для полнотекстового поиска
    if "products_fts" not in tables:
        conn.execute("CREATE VIRTUAL TABLE products_fts USING fts5(tokens)")
        conn.commit()
    fts_count = conn.execute("SELECT COUNT(*) FROM products_fts").fetchone()[0]
    if fts_count == 0:
        rows = conn.execute("SELECT id, name FROM products").fetchall()
        conn.executemany(
            "INSERT INTO products_fts(rowid, tokens) VALUES (?, ?)",
            [(r["id"], _fts_tokens(r["name"])) for r in rows],
        )
        conn.commit()

    # Исправляем Mvideo URLs: /product/{id} → /products/{id} (singular → plural)
    conn.execute(
        "UPDATE offers SET url = REPLACE(url, 'mvideo.ru/product/', 'mvideo.ru/products/') "
        "WHERE source = 'mvideo' AND url LIKE '%mvideo.ru/product/%'"
    )
    conn.commit()

    conn.close()


# ------------------------------------------------------------------ #
# Логирование запусков парсеров                                        #
# ------------------------------------------------------------------ #

def start_parse_run(parser_key: str, source: str, category: str = None) -> int:
    """Создаёт запись о начале запуска, возвращает run_id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO parse_runs (parser_key, source, category, status) "
            "VALUES (?, ?, ?, 'running')",
            (parser_key, source, category),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def finish_parse_run(run_id: int, status: str, items_found: int,
                     saved: int, updated: int,
                     expected_total: int = None, error_msg: str = None):
    """Завершает запись о запуске."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE parse_runs SET finished_at=datetime('now'), status=?, "
            "items_found=?, saved_count=?, updated_count=?, "
            "expected_total=?, error_msg=? WHERE id=?",
            (status, items_found, saved, updated, expected_total, error_msg, run_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_parse_runs(source: str = None, limit: int = 100) -> list:
    """История запусков для страницы аудита."""
    conn = get_connection()
    try:
        if source:
            rows = conn.execute(
                "SELECT * FROM parse_runs WHERE source=? "
                "ORDER BY started_at DESC LIMIT ?",
                (source, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM parse_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_audit_summary() -> list:
    """
    Агрегированный отчёт: последний завершённый run по каждой (source, category)
    с количеством офферов в БД.
    Включает источники которые есть в offers но ещё не имеют parse_runs.
    """
    conn = get_connection()
    try:
        rows = conn.execute("""
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
        """).fetchall()
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
    finally:
        conn.close()


def search_products(query=None, brand=None, chip=None, sources=None, category=None):
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

        if category:
            sql += " AND p.category = ?"
            params.append(category)
        if brand:
            sql += " AND p.brand = ?"
            params.append(brand)
        if chip:
            sql += " AND (p.model LIKE ? OR p.name LIKE ?)"
            params.extend([f"%{chip}%", f"%{chip}%"])
        if query:
            fts_expr = _build_fts_query(query)
            if fts_expr:
                sql += " AND p.id IN (SELECT rowid FROM products_fts WHERE tokens MATCH ?)"
                params.append(fts_expr)

        sql += " ORDER BY p.name"
        products = [dict(r) for r in conn.execute(sql, params).fetchall()]

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

        # Загружаем offers для ВСЕХ связанных товаров (только в наличии)
        placeholders = ",".join("?" * len(all_product_ids))
        offers = conn.execute(f"""
            SELECT o.*, p.name as product_name, COALESCE(p.canonical_id, p.id) AS group_id
            FROM offers o
            JOIN products p ON p.id = o.product_id
            WHERE o.product_id IN ({placeholders}) AND o.in_stock = 1
            ORDER BY o.price
        """, all_product_ids).fetchall()

        for o in offers:
            o = dict(o)
            if sources and o["source"] not in sources:
                continue
            gid = o["group_id"]
            if gid in groups:
                groups[gid]["offers"].append(o)

        # Убираем товары без офферов в наличии
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
        WHERE o.product_id IN ({placeholders}) AND o.in_stock = 1
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


def get_products_with_offers_bulk(product_ids: list) -> dict:
    """Возвращает dict {product_id: product_with_offers} тремя запросами (без N+1)."""
    if not product_ids:
        return {}
    conn = get_connection()
    try:
        ph = ",".join("?" * len(product_ids))
        # 1. Загружаем продукты
        rows = conn.execute(
            f"SELECT * FROM products WHERE id IN ({ph})", product_ids
        ).fetchall()
        products = {r["id"]: dict(r) for r in rows}

        # 2. Находим все связанные product_id (через canonical_id)
        canonical_ids = list({p.get("canonical_id") or p["id"] for p in products.values()})
        cph = ",".join("?" * len(canonical_ids))
        related = conn.execute(
            f"SELECT id, COALESCE(canonical_id, id) AS canonical FROM products "
            f"WHERE COALESCE(canonical_id, id) IN ({cph})",
            canonical_ids,
        ).fetchall()
        all_related_ids = [r["id"] for r in related]
        pid_to_canonical = {r["id"]: r["canonical"] for r in related}

        # 3. Загружаем все офферы за один запрос
        oph = ",".join("?" * len(all_related_ids))
        offers = conn.execute(f"""
            SELECT o.*, p.name as product_name, COALESCE(p.canonical_id, p.id) AS canonical
            FROM offers o
            JOIN products p ON p.id = o.product_id
            WHERE o.product_id IN ({oph}) AND o.in_stock = 1
            ORDER BY o.price
        """, all_related_ids).fetchall()

        # Группируем офферы по canonical_id
        offers_by_canonical: dict = {}
        for o in offers:
            offers_by_canonical.setdefault(o["canonical"], []).append(dict(o))

        # Собираем результат
        result = {}
        for pid, product in products.items():
            canonical = pid_to_canonical.get(pid) or product.get("canonical_id") or pid
            product["offers"] = offers_by_canonical.get(canonical, [])
            result[pid] = product
        return result
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


def get_filter_options(category=None):
    """Возвращает бренды, модели и источники для фильтров.
    Если передана category — бренды и модели только для неё."""
    conn = get_connection()
    try:
        if category:
            brands = [r["brand"] for r in conn.execute(
                "SELECT DISTINCT brand FROM products WHERE brand IS NOT NULL AND category = ? ORDER BY brand",
                (category,),
            ).fetchall()]
            models = [r["model"] for r in conn.execute(
                "SELECT DISTINCT model FROM products WHERE model IS NOT NULL AND category = ? ORDER BY model",
                (category,),
            ).fetchall()]
        else:
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


def get_store_offer_counts() -> dict:
    """Возвращает {source: offer_count} для магазинов с данными."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT source, COUNT(*) as cnt FROM offers WHERE in_stock=1 GROUP BY source ORDER BY cnt DESC"
        ).fetchall()
        return {r["source"]: r["cnt"] for r in rows}
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


