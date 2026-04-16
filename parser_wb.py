"""
Парсер Wildberries (wildberries.ru) — комплектующие ПК.

Использует поисковый API search.wb.ru напрямую (без Playwright).
Цена берётся из sizes[0].price.product (копейки → рубли).
"""

import time

import requests

from base_parser import BaseParser

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
    "https://search.wb.ru/exactmatch/ru/common/v5/search"
    "?query={query}&resultset=catalog&limit=100&page={page}"
    "&appType=1&curr=rub&dest=-1257786"
)


class WbParser(BaseParser):
    SOURCE_NAME = "wb"
    SEARCH_QUERY = "видеокарта"   # переопределяется в категорийных парсерах
    CATALOG_URL = ""              # не используется, только для совместимости
    BASE_URL = "https://www.wildberries.ru"
    CARD_SELECTOR = ""
    WAIT_TIMEOUT = 30000
    DELAY_BETWEEN_PAGES = 5
    MAX_PAGES = 50

    def run(self):
        session = requests.Session()
        session.headers.update(_HEADERS)

        all_products = []
        seen_ids = set()

        for page_num in range(1, self.MAX_PAGES + 1):
            url = _SEARCH_URL.format(
                query=requests.utils.quote(self.SEARCH_QUERY),
                page=page_num,
            )
            print(f"[{self.SOURCE_NAME}] Страница {page_num}: {self.SEARCH_QUERY!r}")

            try:
                r = session.get(url, timeout=20)
                # Retry на 429 с нарастающей задержкой
                for wait in (15, 30):
                    if r.status_code != 429:
                        break
                    print(f"[{self.SOURCE_NAME}] Rate limit — ждём {wait} сек")
                    time.sleep(wait)
                    r = session.get(url, timeout=20)
                if r.status_code != 200 or not r.text:
                    print(f"[{self.SOURCE_NAME}] Стр. {page_num}: статус {r.status_code} — стоп")
                    break

                data = r.json()
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {e}")
                break

            products_raw = data.get("products") or []
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
