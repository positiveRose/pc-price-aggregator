"""
Парсер Ситилинк — видеокарты.
Основан на citilink_simple.py из Итерации 1, переработан под общий интерфейс.
Пагинация: ?p=N, всего ~8 страниц.
"""

from bs4 import BeautifulSoup

from base_parser import BaseParser


class CitilinkParser(BaseParser):
    SOURCE_NAME = "citilink"
    CATALOG_URL = "https://www.citilink.ru/catalog/videokarty/"
    BASE_URL = "https://www.citilink.ru"
    CARD_SELECTOR = '[data-meta-name="ProductVerticalSnippet"]'

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
        """Ситилинк: ищем максимальный номер страницы в пагинации."""
        soup = BeautifulSoup(html, "lxml")
        pages = soup.select("[data-meta-name^='PaginationElement__page']")
        max_page = 1
        for el in pages:
            num = el.get("data-meta-page-number", "")
            if num.isdigit():
                max_page = max(max_page, int(num))
        return max_page

    def get_page_url(self, page_num):
        return f"{self.CATALOG_URL}?p={page_num}"
