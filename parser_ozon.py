"""
Парсер Ozon (ozon.ru) — комплектующие ПК.

Playwright + перехват XHR-ответов от api/composer-api.bx.
Ozon — React SPA, товары приходят в JSON виде widgetStates с ключами searchResultsV2*.

Структура ответа:
  widgetStates["searchResultsV2-{hash}"] — stringified JSON:
    { "items": [{ "sku": 123, "title": "...", "finalPrice": 12345, ... }] }

Пагинация: ?page=N (начиная со страницы 2).
"""

import json
import re
import time

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from base_parser import BaseParser


class OzonParser(BaseParser):
    SOURCE_NAME = "ozon"
    CATALOG_URL = "https://www.ozon.ru/category/videokarty-i-karty-videozahvata-15720/"
    BASE_URL = "https://www.ozon.ru"
    CARD_SELECTOR = ""
    WAIT_TIMEOUT = 30000
    DELAY_BETWEEN_PAGES = 6
    MAX_PAGES = 10

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
            pg = context.new_page()
            Stealth().apply_stealth_sync(pg)

            buffer = []

            def on_response(response):
                url = response.url
                if response.status != 200:
                    return
                # Перехватываем только JSON-ответы Ozon API с товарами
                if not any(x in url for x in ("composer-api", "entrypoint-api", "api.ozon")):
                    return
                try:
                    data = response.json()
                    items = self._extract_from_widget_states(data)
                    if items:
                        buffer.extend(items)
                except Exception:
                    pass

            pg.on("response", on_response)

            for page_num in range(1, self.MAX_PAGES + 1):
                buffer.clear()
                base = self.CATALOG_URL.rstrip("/")
                url = base + "/" if page_num == 1 else f"{base}/?page={page_num}"
                print(f"[{self.SOURCE_NAME}] Страница {page_num}: {url}")

                try:
                    pg.goto(url, wait_until="domcontentloaded", timeout=60000)
                    # Ozon — React SPA, ждём завершения XHR-запросов
                    time.sleep(7)
                except Exception as e:
                    print(f"[{self.SOURCE_NAME}] Ошибка загрузки стр. {page_num}: {e}")
                    break

                snapshot = list(buffer)
                if not snapshot:
                    print(f"[{self.SOURCE_NAME}] Стр. {page_num}: нет данных — стоп")
                    break

                print(f"[{self.SOURCE_NAME}] Стр. {page_num}: {len(snapshot)} товаров")
                all_products.extend(snapshot)
                time.sleep(max(1, self.DELAY_BETWEEN_PAGES - 7))

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
    # Разбор widgetStates                                                 #
    # ------------------------------------------------------------------ #

    def _extract_from_widget_states(self, data):
        """Ищет товары в widgetStates JSON от Ozon API."""
        products = []
        widget_states = data.get("widgetStates") or {}

        for key, raw in widget_states.items():
            # Интересны только ключи с товарами (searchResults, catalog, tiles)
            key_lower = key.lower()
            if not any(x in key_lower for x in ("search", "catalog", "tile", "product", "items")):
                continue

            try:
                obj = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue

            # Ищем список товаров
            items = (
                obj.get("items")
                or obj.get("products")
                or obj.get("results")
                or []
            )
            if not isinstance(items, list):
                continue

            for item in items:
                product = self._parse_item(item)
                if product:
                    products.append(product)

        # Fallback: рекурсивный поиск в любом месте JSON
        if not products:
            products = self._deep_search_products(data)

        return products

    def _parse_item(self, item):
        """Разбирает один товар из Ozon API."""
        if not isinstance(item, dict):
            return None
        try:
            # SKU / ID
            pid = str(
                item.get("sku")
                or item.get("id")
                or item.get("itemId")
                or ""
            )
            if not pid:
                return None

            # Название
            name = (
                item.get("title")
                or item.get("name")
                or item.get("displayName")
                or ""
            )
            if not name or len(name) < 5:
                return None

            # Цена (у Ozon в рублях, не в копейках)
            price = None
            for key in ("finalPrice", "salePrice", "price", "minPrice", "cardPrice"):
                raw = item.get(key)
                if raw is not None:
                    try:
                        val = int(float(str(raw).replace("\xa0", "").replace(" ", "").replace(",", ".")))
                        if 300 < val < 10_000_000:
                            price = val
                            break
                    except (ValueError, TypeError):
                        pass

            if not price:
                # Попробовать через action/price/mainPrice
                price_obj = item.get("price") or item.get("priceV2") or {}
                if isinstance(price_obj, dict):
                    raw = price_obj.get("cardPrice") or price_obj.get("price") or price_obj.get("originalPrice")
                    if raw:
                        digits = re.sub(r"[^\d]", "", str(raw))
                        if digits:
                            val = int(digits)
                            if 300 < val < 10_000_000:
                                price = val

            if not price:
                return None

            # URL товара
            url_path = (
                item.get("link")
                or item.get("url")
                or item.get("action", {}).get("link", "")
                or f"/product/{pid}/"
            )
            if not url_path.startswith("http"):
                url_path = self.BASE_URL + url_path

            return {
                "id":       pid,
                "name":     name,
                "price":    price,
                "url":      url_path,
                "in_stock": True,
            }
        except Exception:
            return None

    def _deep_search_products(self, obj, depth=0):
        """Рекурсивно ищет массивы товаров в JSON."""
        if depth > 8:
            return []
        products = []

        if isinstance(obj, dict):
            # Проверяем: словарь сам является товаром
            if obj.get("sku") and obj.get("title"):
                p = self._parse_item(obj)
                if p:
                    return [p]
            for v in obj.values():
                products.extend(self._deep_search_products(v, depth + 1))
        elif isinstance(obj, list) and obj:
            # Если список содержит товары
            if isinstance(obj[0], dict) and (obj[0].get("sku") or obj[0].get("finalPrice")):
                for item in obj:
                    p = self._parse_item(item)
                    if p:
                        products.append(p)
            else:
                for v in obj:
                    products.extend(self._deep_search_products(v, depth + 1))
        elif isinstance(obj, str) and obj.startswith("{"):
            try:
                parsed = json.loads(obj)
                products.extend(self._deep_search_products(parsed, depth + 1))
            except Exception:
                pass

        return products

    # ABC stubs
    def parse_products(self, html):
        return []

    def get_total_pages(self, html):
        return 1

    def get_page_url(self, page_num):
        return self.CATALOG_URL
