"""
Парсер Ситилинк — видеокарты.
Основан на citilink_simple.py из Итерации 1, переработан под общий интерфейс.
Пагинация: ?p=N, всего ~8 страниц.
"""

import json
import math
import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class CitilinkParser(BaseParser):
    SOURCE_NAME = "citilink"
    CATALOG_URL = "https://www.citilink.ru/catalog/videokarty/"
    BASE_URL = "https://www.citilink.ru"
    CARD_SELECTOR = '[data-meta-name="ProductVerticalSnippet"]'
    _CATEGORY = "GPU"

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(self.CARD_SELECTOR)
        products = []

        for card in cards:
            try:
                product = self._parse_card(card)
                if product:
                    products.append(product)
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Ошибка карточки: {e}")
                continue

        return products

    def _parse_card(self, card):
        # Название и ссылка
        link = card.select_one("a[title]")
        if not link:
            return None

        name = link["title"].strip()
        href = link["href"]
        url = href if href.startswith("http") else self.BASE_URL + href

        # Цена
        price_el = card.select_one("[data-meta-price]")
        if not price_el:
            return None

        try:
            price = int(float(price_el["data-meta-price"]))
            if not (300 < price < 10_000_000):
                return None
        except (ValueError, TypeError):
            return None

        # ID товара
        product_id = card.get("data-meta-product-id", "")
        if not product_id:
            parts = href.rstrip("/").split("-")
            product_id = parts[-1] if parts[-1].isdigit() else href.rstrip("/").rsplit("/", 1)[-1]

        return {
            "id": product_id,
            "name": name,
            "price": price,
            "url": url,
            "in_stock": True,
        }

    @staticmethod
    def _find_json_values(obj, key):
        """Рекурсивно ищет все значения по ключу в JSON-структуре."""
        results = []
        if isinstance(obj, dict):
            if key in obj:
                results.append(obj[key])
            for v in obj.values():
                results.extend(CitilinkParser._find_json_values(v, key))
        elif isinstance(obj, list):
            for item in obj:
                results.extend(CitilinkParser._find_json_values(item, key))
        return results

    def _get_subcategory_pagination(self, data):
        """Ищет объект pagination в subcategory через оба SSR-пути."""
        for init_path in (
            ("props", "initialState"),
            ("props", "pageProps", "initialState"),
        ):
            node = data
            for k in init_path:
                node = node.get(k, {}) if isinstance(node, dict) else {}
            subcat = node.get("subcategory", {}) if isinstance(node, dict) else {}
            pag = (subcat.get("productsFilter", {})
                         .get("payload", {})
                         .get("productsFilter", {})
                         .get("pagination", {}))
            if pag and isinstance(pag, dict):
                total_pages = pag.get("totalPages", 0)
                if total_pages and int(total_pages) > 0:
                    return int(total_pages)
                # Если totalPages отсутствует — вычисляем из totalItems/perPage
                total_items = pag.get("totalItems", 0)
                per_page = pag.get("perPage", 36) or 36
                if total_items and int(total_items) > 0:
                    return math.ceil(int(total_items) / int(per_page))
        return 0

    def get_total_pages(self, html):
        """Ситилинк: определяем число страниц.

        Приоритет 1 — __NEXT_DATA__ subcategory.productsFilter.pagination
                       (оба пути: initialState и pageProps.initialState).
        Приоритет 2 — __NEXT_DATA__ рекурсивный поиск totalPages только
                       внутри subcategory (изоляция от discussion/review).
        Приоритет 3 — DOM-элементы пагинации PaginationElement__page*.
        Приоритет 4 — счётчик товаров data-meta-product-count.
        """
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))

                # Приоритет 1: жёсткий путь через оба SSR-пути
                total_pages = self._get_subcategory_pagination(data)
                if total_pages > 0:
                    print(f"[{self.SOURCE_NAME}] Страниц из __NEXT_DATA__ (subcategory): {total_pages}", flush=True)
                    return total_pages

                # Приоритет 2: рекурсивный поиск внутри subcategory
                # (не по всему дереву — discussion/review тоже имеют totalPages)
                for init_path in (("props", "initialState"), ("props", "pageProps", "initialState")):
                    node = data
                    for k in init_path:
                        node = node.get(k, {}) if isinstance(node, dict) else {}
                    subcat = node.get("subcategory", {}) if isinstance(node, dict) else {}
                    if subcat:
                        all_totals = self._find_json_values(subcat, "totalPages")
                        valid = []
                        for v in all_totals:
                            try:
                                n = int(v)
                                if n > 0:
                                    valid.append(n)
                            except (TypeError, ValueError):
                                pass
                        if valid:
                            total_pages = max(valid)
                            print(f"[{self.SOURCE_NAME}] Страниц из __NEXT_DATA__ (recursive subcategory): {total_pages}", flush=True)
                            return total_pages
            except Exception:
                pass

        soup = BeautifulSoup(html, "lxml")

        # 2. DOM-элементы пагинации
        pages = soup.select("[data-meta-name^='PaginationElement__page']")
        max_page = 1
        for el in pages:
            num = el.get("data-meta-page-number", "")
            if num.isdigit():
                max_page = max(max_page, int(num))

        if max_page > 1:
            return max_page

        # 3. Счётчик товаров
        count_el = soup.select_one("[data-meta-product-count]")
        if count_el:
            try:
                total = int(count_el.get("data-meta-product-count", 0))
                if total > 36:
                    computed = math.ceil(total / 36)
                    print(f"[{self.SOURCE_NAME}] Вычислено из счётчика: "
                          f"{total} товаров → {computed} стр.", flush=True)
                    return computed
            except (ValueError, TypeError):
                pass

        return max_page

    def get_page_url(self, page_num):
        return f"{self.CATALOG_URL}?p={page_num}"
