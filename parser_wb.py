"""
Парсер Wildberries (wildberries.ru) — комплектующие ПК.

Playwright + перехват ответов catalog.wb.ru/catalog/.../v2/catalog.
Страница 1 перехватывается при первой загрузке.
Страницы 2+ — навигация на {url}?page=N, каждый раз перехватываем тот же callback.

Структура JSON:
  data.products[].{id, name, brand, salePriceU / priceU (копейки), sizes[].price.product}
"""

import time

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from base_parser import BaseParser


class WbParser(BaseParser):
    SOURCE_NAME = "wb"
    CATALOG_URL = "https://www.wildberries.ru/catalog/elektronika/noutbuki-i-kompyutery/komplektuyushchie-dlya-pk/videokarty"
    BASE_URL = "https://www.wildberries.ru"
    CARD_SELECTOR = ""
    WAIT_TIMEOUT = 30000
    DELAY_BETWEEN_PAGES = 6
    MAX_PAGES = 15

    # ------------------------------------------------------------------ #
    # Точка входа                                                         #
    # ------------------------------------------------------------------ #

    def run(self):
        all_products = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            buffer = []

            def on_response(response):
                if (
                    "catalog.wb.ru" in response.url
                    and "catalog" in response.url
                    and response.status == 200
                ):
                    try:
                        data = response.json()
                        items = data.get("data", {}).get("products", [])
                        buffer.extend(items)
                    except Exception:
                        pass

            page.on("response", on_response)

            for page_num in range(1, self.MAX_PAGES + 1):
                buffer.clear()
                url = self.CATALOG_URL if page_num == 1 else f"{self.CATALOG_URL}?page={page_num}"
                print(f"[{self.SOURCE_NAME}] Страница {page_num}: {url}")

                try:
                    page.goto(url, wait_until="networkidle", timeout=60000)
                    time.sleep(3)
                except Exception as e:
                    print(f"[{self.SOURCE_NAME}] Ошибка загрузки стр. {page_num}: {e}")
                    break

                snapshot = list(buffer)
                if not snapshot:
                    print(f"[{self.SOURCE_NAME}] Стр. {page_num}: API не ответил — стоп")
                    break

                parsed = self._parse_items(snapshot)
                print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {len(parsed)} товаров")
                all_products.extend(parsed)

                time.sleep(self.DELAY_BETWEEN_PAGES - 3)

            browser.close()

        # Дедупликация
        seen = set()
        unique = []
        for prod in all_products:
            if prod["id"] not in seen:
                seen.add(prod["id"])
                unique.append(prod)

        print(f"[{self.SOURCE_NAME}] Итого: {len(unique)}")
        return unique

    # ------------------------------------------------------------------ #
    # Разбор элементов JSON                                               #
    # ------------------------------------------------------------------ #

    def _parse_items(self, items):
        products = []
        for item in items:
            try:
                pid = str(item.get("id", ""))
                if not pid:
                    continue

                brand = item.get("brand", "") or ""
                name  = item.get("name", "") or item.get("title", "") or ""
                if brand and not name.lower().startswith(brand.lower()):
                    name = f"{brand} {name}".strip()
                if not name or len(name) < 3:
                    continue

                # salePriceU / priceU — в копейках (делим на 100)
                price = None
                for key in ("salePriceU", "priceU"):
                    raw = item.get(key)
                    if raw:
                        price = int(raw) // 100
                        break

                # Fallback: sizes[0].price.product (тоже копейки)
                if not price:
                    sizes = item.get("sizes") or []
                    if sizes:
                        p = sizes[0].get("price") or {}
                        raw = p.get("product") or p.get("total") or p.get("basic")
                        if raw:
                            price = int(raw) // 100

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

    # Методы ABC — не используются, но требуются
    def parse_products(self, html):
        return []

    def get_total_pages(self, html):
        return 1

    def get_page_url(self, page_num):
        return self.CATALOG_URL
