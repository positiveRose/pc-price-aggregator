"""
Парсер Wildberries (wildberries.ru) — комплектующие ПК.

Использует поисковый API search.wb.ru напрямую (без Playwright).
Цена берётся из sizes[0].price.product (копейки → рубли).
"""

import time

import requests

from base_parser import BaseParser, get_requests_proxies

# Минимальный интервал между запросами к WB API (любыми категориями в процессе)
_LAST_RUN_TIME: float = 0.0
_MIN_INTERVAL: float = 65.0  # секунд

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Origin": "https://www.wildberries.ru",
    "Referer": "https://www.wildberries.ru/",
}

_SEARCH_URL = (
    "https://search.wb.ru/exactmatch/ru/common/v18/search"
    "?query={query}&resultset=catalog&limit=100&page={page}"
    "&appType=1&curr=rub&dest=12358283&sort=popular&spp=30"
)

_CATALOG_URL = (
    "https://catalog.wb.ru/catalog/{shard}/v4/catalog"
    "?{query}&appType=1&curr=rub&dest=12358283&page={page}&sort=popular&spp=30"
)


class WbParser(BaseParser):
    SOURCE_NAME = "wb"
    SEARCH_QUERY = "видеокарта"   # используется только если CATALOG_SHARD не задан
    CATALOG_SHARD = None          # shard для catalog.wb.ru, например "electronic73"
    CATALOG_QUERY = None          # параметр фильтра, например "subject=3274"
    CATALOG_URL = ""              # не используется, только для совместимости
    BASE_URL = "https://www.wildberries.ru"
    CARD_SELECTOR = ""
    WAIT_TIMEOUT = 30000
    DELAY_BETWEEN_PAGES = 5
    MAX_PAGES = 10           # 10 стр × 100 товаров = 1000 макс на категорию
    MIN_FEEDBACKS = 5        # Минимум отзывов (прокси продаж: 5 отзывов ≈ 50+ продаж)

    def run(self):
        global _LAST_RUN_TIME
        elapsed = time.time() - _LAST_RUN_TIME
        if _LAST_RUN_TIME > 0 and elapsed < _MIN_INTERVAL:
            wait = _MIN_INTERVAL - elapsed
            print(f"[{self.SOURCE_NAME}] Пауза {wait:.0f}с между категориями...")
            time.sleep(wait)
        _LAST_RUN_TIME = time.time()

        session = requests.Session()
        session.headers.update(_HEADERS)
        proxies = get_requests_proxies()
        if proxies:
            session.proxies.update(proxies)

        all_products = []
        seen_ids = set()

        for page_num in range(1, self.MAX_PAGES + 1):
            if self.CATALOG_SHARD and self.CATALOG_QUERY:
                url = _CATALOG_URL.format(
                    shard=self.CATALOG_SHARD,
                    query=self.CATALOG_QUERY,
                    page=page_num,
                )
                print(f"[{self.SOURCE_NAME}] Страница {page_num}: {self.CATALOG_QUERY}")
            else:
                url = _SEARCH_URL.format(
                    query=requests.utils.quote(self.SEARCH_QUERY),
                    page=page_num,
                )
                print(f"[{self.SOURCE_NAME}] Страница {page_num}: {self.SEARCH_QUERY!r}")

            try:
                r = session.get(url, timeout=(10, 40))
                # Retry на 429 с нарастающей задержкой
                for wait in (15, 30):
                    if r.status_code != 429:
                        break
                    print(f"[{self.SOURCE_NAME}] Rate limit — ждём {wait} сек")
                    time.sleep(wait)
                    r = session.get(url, timeout=(10, 40))
                if r.status_code != 200 or not r.text:
                    print(f"[{self.SOURCE_NAME}] Стр. {page_num}: статус {r.status_code} — стоп")
                    break

                data = r.json()
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {e}")
                break

            # catalog API: data.products; search API: products
            products_raw = (
                data.get("data", {}).get("products")
                or data.get("products")
                or []
            )
            if not products_raw:
                print(f"[{self.SOURCE_NAME}] Стр. {page_num}: товаров нет — стоп")
                break

            parsed = self._parse_items(products_raw, seen_ids)
            print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {len(parsed)} товаров")
            all_products.extend(parsed)

            time.sleep(self.DELAY_BETWEEN_PAGES)

        print(f"[{self.SOURCE_NAME}] Итого: {len(all_products)}")
        return all_products

    def _parse_items(self, items, seen_ids):
        products = []
        for item in items:
            try:
                pid = str(item.get("id", ""))
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)

                # Фильтр по отзывам: убираем товары без реальных продаж
                feedbacks = item.get("feedbacks") or 0
                if feedbacks < self.MIN_FEEDBACKS:
                    continue

                brand = item.get("brand", "") or ""
                name = item.get("name", "") or ""
                if brand and not name.lower().startswith(brand.lower()):
                    name = f"{brand} {name}".strip()
                if not name or len(name) < 3:
                    continue

                # Цена в копейках → рубли
                price = None
                sizes = item.get("sizes") or []
                if sizes:
                    price_obj = sizes[0].get("price") or {}
                    for key in ("product", "basic", "total"):
                        raw = price_obj.get(key)
                        if raw and int(raw) > 0:
                            price = int(raw) // 100
                            break

                if not price or not (300 < price < 10_000_000):
                    continue

                products.append({
                    "id":       pid,
                    "name":     name,
                    "price":    price,
                    "url":      f"https://www.wildberries.ru/catalog/{pid}/detail.aspx",
                    "in_stock": True,
                })
            except Exception:
                continue
        return products

    # ABC stubs
    def parse_products(self, html):
        return []

    def get_total_pages(self, html):
        return 1

    def get_page_url(self, page_num):
        return self.CATALOG_URL
