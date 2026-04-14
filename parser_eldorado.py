"""
Парсер Эльдорадо (eldorado.ru) — видеокарты.

Сайт Next.js с SSR. Товары встроены в __NEXT_DATA__ как Redux state
или dehydratedState (React Query). Структура может меняться.

Стратегии (по порядку):
  1. Глубокий поиск: обходим всё JSON-дерево, собираем любой объект
     у которого есть (productId|id) + (name|title) + числовая price.
  2. Поиск словаря {digit_key: product_dict} (старый Redux-формат).
  3. Fallback: HTML-парсинг карточек.

Playwright необходим для рендеринга страницы.
Пагинация: ?page=N
"""

import json
import math
import re
import time

from bs4 import BeautifulSoup

from base_parser import BaseParser


class EldoradoParser(BaseParser):
    SOURCE_NAME = "eldorado"
    CATALOG_URL = "https://www.eldorado.ru/c/videokarty/"
    BASE_URL = "https://www.eldorado.ru"
    CARD_SELECTOR = ""  # данные в __NEXT_DATA__ (SSR), ждать карточки не нужно
    WAIT_TIMEOUT = 25000
    DELAY_BETWEEN_PAGES = 5

    def parse_products(self, html):
        products = self._parse_next_data(html)
        if not products:
            products = self._parse_html_cards(html)
        return products

    # ------------------------------------------------------------------ #
    # Стратегия 1+2: __NEXT_DATA__                                        #
    # ------------------------------------------------------------------ #

    def _parse_next_data(self, html):
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if not m:
            return []

        try:
            data = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            return []

        # Стратегия А: глубокий поиск любых объектов похожих на товар
        found = {}
        self._deep_collect(data, found, depth=0)

        # Стратегия Б: старый Redux-формат {digit_key: product_dict}
        if not found:
            items_map = self._find_products_map(data)
            if items_map:
                for pid_key, item in items_map.items():
                    if not isinstance(item, dict):
                        continue
                    product = self._item_to_product(item, pid_key)
                    if product and product["id"] not in found:
                        found[product["id"]] = product

        return list(found.values())

    def _deep_collect(self, obj, found, depth):
        """Обходит всё JSON-дерево и собирает любые объекты-товары."""
        if depth > 15:
            return

        if isinstance(obj, dict):
            product = self._try_parse_item(obj)
            if product:
                # Нашли товар — не заходим внутрь (избегаем дублей из вложенных полей)
                if product["id"] not in found:
                    found[product["id"]] = product
                return

            for v in obj.values():
                if isinstance(v, (dict, list)):
                    self._deep_collect(v, found, depth + 1)

        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._deep_collect(item, found, depth + 1)

    def _try_parse_item(self, obj):
        """Проверяет: является ли словарь товаром? Если да — возвращает product dict."""
        pid = obj.get("productId") or obj.get("id")
        if not pid or not str(pid).isdigit():
            return None

        name = obj.get("name") or obj.get("title") or ""
        if not isinstance(name, str) or len(name) < 5:
            return None

        price = self._extract_price(obj)
        if not price:
            return None

        pid_str = str(pid)
        code = obj.get("code") or obj.get("slug") or pid_str
        return {
            "id":       pid_str,
            "name":     name,
            "price":    price,
            "url":      f"{self.BASE_URL}/cat/{code}/",
            "in_stock": True,
        }

    def _extract_price(self, item):
        """Извлекает числовую цену из объекта товара (поддерживает вложенные dict)."""
        for key in ("price", "salePrice", "finalPrice", "basePrice", "priceValue"):
            raw = item.get(key)
            if raw is None:
                continue
            if isinstance(raw, (int, float)) and raw > 0:
                val = int(raw)
                if 300 < val < 10_000_000:
                    return val
            elif isinstance(raw, str):
                digits = re.sub(r"[^\d]", "", raw)
                if digits:
                    val = int(digits)
                    if 300 < val < 10_000_000:
                        return val
            elif isinstance(raw, dict):
                # Вложенный объект цены: {sale: X, base: Y} или {value: X}
                for sub_key in ("sale", "salePrice", "base", "basePrice",
                                "value", "amount", "current"):
                    sub = raw.get(sub_key)
                    if isinstance(sub, (int, float)) and sub > 0:
                        val = int(sub)
                        if 300 < val < 10_000_000:
                            return val
        return None

    def _item_to_product(self, item, pid_key=""):
        """Конвертирует dict товара в product dict (для старого Redux-формата)."""
        pid = str(item.get("productId") or item.get("id") or pid_key)
        name = item.get("name") or item.get("title") or ""
        if not pid or not name or len(name) < 5:
            return None
        price = self._extract_price(item)
        if not price:
            return None
        code = item.get("code") or item.get("slug") or pid
        return {
            "id":       pid,
            "name":     name,
            "price":    price,
            "url":      f"{self.BASE_URL}/cat/{code}/",
            "in_stock": True,
        }

    # ------------------------------------------------------------------ #
    # Старый Redux-формат {digit_key: product_dict}                       #
    # ------------------------------------------------------------------ #

    def _find_products_map(self, obj, depth=0):
        candidates = []
        self._collect_map_candidates(obj, depth, candidates)
        if not candidates:
            return None
        return max(candidates, key=lambda d: len(d))

    def _collect_map_candidates(self, obj, depth, candidates):
        if depth > 12:
            return
        if isinstance(obj, dict):
            if obj:
                sample_keys = list(obj.keys())[:10]
                sample_vals = [obj[k] for k in sample_keys]
                if (all(str(k).isdigit() for k in sample_keys) and
                        all(isinstance(v, dict) for v in sample_vals) and
                        any(("productId" in v or "id" in v or
                             "name" in v or "title" in v)
                            for v in sample_vals)):
                    candidates.append(obj)
                    return
            for v in obj.values():
                self._collect_map_candidates(v, depth + 1, candidates)
        elif isinstance(obj, list):
            for item in obj:
                self._collect_map_candidates(item, depth + 1, candidates)

    # ------------------------------------------------------------------ #
    # Стратегия 3: HTML-парсинг карточек (fallback)                       #
    # ------------------------------------------------------------------ #

    def _parse_html_cards(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen_ids = set()

        cards = (
            soup.select("[data-testid='product-card']") or
            soup.select(".product-card") or
            soup.select(".catalog-product") or
            soup.select("li[class*='product']")
        )

        for card in cards:
            try:
                link = card.find("a", href=re.compile(r"/cat/"))
                if not link:
                    continue

                href = link.get("href", "")
                product_id = self._extract_id_from_href(href)
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                name_el = (
                    card.select_one("[class*='name']") or
                    card.select_one("[class*='title']") or
                    card.select_one("h3") or link
                )
                name = name_el.get_text(strip=True) if name_el else ""
                if not name or len(name) < 5:
                    continue

                price = self._find_price_in_card(card)
                if not price:
                    continue

                url = href if href.startswith("http") else self.BASE_URL + href
                products.append({
                    "id":       product_id,
                    "name":     name,
                    "price":    price,
                    "url":      url,
                    "in_stock": True,
                })
            except Exception:
                continue

        return products

    def _find_price_in_card(self, card):
        for sel in ["[class*='price']", "strong", "b"]:
            el = card.select_one(sel)
            if el:
                price = self._parse_price_text(el.get_text())
                if price:
                    return price
        return None

    def _parse_price_text(self, text):
        digits = re.sub(r"[^\d]", "", text)
        if digits:
            val = int(digits)
            if 300 < val < 10_000_000:
                return val
        return None

    def _extract_id_from_href(self, href):
        m = re.search(r"-(\d{5,})/?$", href.rstrip("/"))
        if m:
            return m.group(1)
        m = re.search(r"/cat/([^/?#]+)", href)
        return m.group(1) if m else None

    # ------------------------------------------------------------------ #
    # Пагинация                                                           #
    # ------------------------------------------------------------------ #

    def get_total_pages(self, html):
        soup = BeautifulSoup(html, "lxml")
        max_page = 1

        # Ищем в __NEXT_DATA__: total и pageSize → вычисляем кол-во страниц
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if m:
            try:
                data = json.loads(m.group(1))
                result = [None, None]
                self._find_pagination_info(data, result)
                total_items, page_size = result
                if total_items and page_size and page_size > 0:
                    max_page = max(max_page, math.ceil(total_items / page_size))
            except Exception:
                pass

        # Ссылки ?page=N в HTML
        for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
            m2 = re.search(r"[?&]page=(\d+)", a.get("href", ""))
            if m2:
                max_page = max(max_page, int(m2.group(1)))

        # Числа в блоке пагинации
        for el in soup.select("[class*='pagination'] a, [class*='pager'] a"):
            text = el.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))

        return max_page

    def _find_pagination_info(self, obj, result, depth=0):
        """Ищет total (кол-во товаров) и pageSize в JSON."""
        if depth > 10 or (result[0] and result[1]):
            return
        if isinstance(obj, dict):
            total = obj.get("total") or obj.get("totalCount") or obj.get("totalItems")
            size  = obj.get("pageSize") or obj.get("limit") or obj.get("perPage")
            if total and isinstance(total, int) and total > 0:
                result[0] = total
            if size and isinstance(size, int) and size > 0:
                result[1] = size
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    self._find_pagination_info(v, result, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._find_pagination_info(item, result, depth + 1)

    def get_page_url(self, page_num):
        base = self.CATALOG_URL.rstrip("/")
        return f"{base}/?page={page_num}"
