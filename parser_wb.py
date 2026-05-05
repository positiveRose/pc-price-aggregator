"""
Парсер Wildberries (wildberries.ru) — комплектующие ПК.

Все запросы к catalog.wb.ru выполняются через Playwright (page.evaluate → fetch),
чтобы браузер подставлял куки и заголовки, которых достаточно для обхода 403
с датацентровых IP (Railway).
"""

import time

from base_parser import BaseParser

# Минимальный интервал между запросами к WB API (любыми категориями в процессе)
_LAST_RUN_TIME: float = 0.0
_MIN_INTERVAL: float = 65.0  # секунд

_CATALOG_URL_TPL = (
    "https://catalog.wb.ru/catalog/{shard}/v4/catalog"
    "?{query}&appType=1&curr=rub&dest=12358283&page={page}&sort=popular&spp=30"
)

_SEARCH_URL_TPL = (
    "https://search.wb.ru/exactmatch/ru/common/v18/search"
    "?query={query}&resultset=catalog&limit=100&page={page}"
    "&appType=1&curr=rub&dest=12358283&sort=popular&spp=30"
)


class WbParser(BaseParser):
    SOURCE_NAME = "wb"
    SEARCH_QUERY = "видеокарта"   # используется только если CATALOG_SHARD не задан
    CATALOG_SHARD = None          # shard для catalog.wb.ru, например "electronic73"
    CATALOG_QUERY = None          # параметр фильтра, например "subject=3274"
    CATALOG_URL = "https://www.wildberries.ru"
    BASE_URL = "https://www.wildberries.ru"
    CARD_SELECTOR = ""
    WAIT_TIMEOUT = 30000
    DELAY_BETWEEN_PAGES = 5
    MAX_PAGES = 10           # 10 стр × 100 товаров = 1000 макс на категорию
    MIN_FEEDBACKS = 5        # Минимум отзывов

    def run(self):
        global _LAST_RUN_TIME
        elapsed = time.time() - _LAST_RUN_TIME
        if _LAST_RUN_TIME > 0 and elapsed < _MIN_INTERVAL:
            wait = _MIN_INTERVAL - elapsed
            print(f"[{self.SOURCE_NAME}] Пауза {wait:.0f}с между категориями...")
            time.sleep(wait)
        _LAST_RUN_TIME = time.time()

        return self._fetch_via_playwright()

    def _fetch_via_playwright(self):
        from playwright.sync_api import sync_playwright

        all_products = []
        seen_ids = set()

        with sync_playwright() as p:
            browser, page = self._create_browser(p)
            try:
                print(f"[{self.SOURCE_NAME}] Загружаю главную WB для получения куки...")
                try:
                    page.goto("https://www.wildberries.ru", wait_until="domcontentloaded", timeout=60000)
                except Exception as e:
                    print(f"[{self.SOURCE_NAME}] goto WB: {e}")
                time.sleep(4)

                for page_num in range(1, self.MAX_PAGES + 1):
                    if self.CATALOG_SHARD and self.CATALOG_QUERY:
                        api_url = _CATALOG_URL_TPL.format(
                            shard=self.CATALOG_SHARD,
                            query=self.CATALOG_QUERY,
                            page=page_num,
                        )
                        label = self.CATALOG_QUERY
                    else:
                        import urllib.parse
                        api_url = _SEARCH_URL_TPL.format(
                            query=urllib.parse.quote(self.SEARCH_QUERY),
                            page=page_num,
                        )
                        label = self.SEARCH_QUERY

                    print(f"[{self.SOURCE_NAME}] Страница {page_num}: {label}")

                    try:
                        result = page.evaluate(
                            """
                            async (url) => {
                                try {
                                    const resp = await fetch(url, {
                                        headers: {
                                            'Accept': 'application/json, text/plain, */*',
                                            'Accept-Language': 'ru-RU,ru;q=0.9',
                                        }
                                    });
                                    if (!resp.ok) return {error: resp.status};
                                    return await resp.json();
                                } catch(e) {
                                    return {error: String(e)};
                                }
                            }
                            """,
                            api_url,
                        )
                    except Exception as e:
                        print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {e}")
                        break

                    if isinstance(result, dict) and result.get("error"):
                        print(f"[{self.SOURCE_NAME}] Стр. {page_num}: ошибка {result['error']} — стоп")
                        break

                    # catalog API: data.products; search API: products
                    products_raw = (
                        (result.get("data") or {}).get("products")
                        or result.get("products")
                        or []
                    )
                    if not products_raw:
                        print(f"[{self.SOURCE_NAME}] Стр. {page_num}: товаров нет — стоп")
                        break

                    parsed = self._parse_items(products_raw, seen_ids)
                    print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {len(parsed)} товаров")
                    all_products.extend(parsed)

                    time.sleep(self.DELAY_BETWEEN_PAGES)

            finally:
                browser.close()

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
