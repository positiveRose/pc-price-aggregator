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

    def get_total_pages(self, html):
        """Ситилинк: определяем число страниц.

        Приоритет 1 — __NEXT_DATA__ (SSR, всегда присутствует в HTML, не
        зависит от React-гидратации и скролла).
        Приоритет 2 — DOM-элементы пагинации PaginationElement__page*.
        Приоритет 3 — счётчик товаров data-meta-product-count.
        """
        # 1. __NEXT_DATA__
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                pag = (data.get("props", {})
                           .get("initialState", {})
                           .get("subcategory", {})
                           .get("productsFilter", {})
                           .get("payload", {})
                           .get("productsFilter", {})
                           .get("pagination", {}))
                total_pages = pag.get("totalPages", 0)
                if total_pages and int(total_pages) > 0:
                    print(f"[{self.SOURCE_NAME}] Страниц из __NEXT_DATA__: {total_pages}", flush=True)
                    return int(total_pages)
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
