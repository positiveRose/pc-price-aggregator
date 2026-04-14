"""
Парсер DNS-Shop — видеокарты.

DNS рендерит каталог через JavaScript, поэтому используем Playwright.
Селекторы:
- Карточка: div.catalog-product
- Название: a.catalog-product__name
- Цена: div.product-buy__price
- ID: data-id на карточке
"""

import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class DnsParser(BaseParser):
    SOURCE_NAME = "dns"
    CATALOG_URL = "https://www.dns-shop.ru/catalog/17a89aab16404e77/videokarty/"
    BASE_URL = "https://www.dns-shop.ru"
    CARD_SELECTOR = "div.catalog-product"
    BROWSER = "firefox"

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
        link = card.select_one("a.catalog-product__name")
        if not link:
            return None

        name = link.get_text(strip=True)
        href = link.get("href", "")
        url = href if href.startswith("http") else self.BASE_URL + href

        # Цена — ищем в нескольких местах (DNS меняет классы)
        price = self._extract_price(card)
        if not price:
            return None

        # ID товара
        product_id = card.get("data-id", "")
        if not product_id:
            product_id = card.get("data-product-id", "unknown")

        return {
            "id": product_id,
            "name": name,
            "price": price,
            "url": url,
            "in_stock": True,
        }

    def _extract_price(self, card):
        """Извлекает цену из карточки, пробуя разные селекторы."""
        # Вариант 1: div.product-buy__price
        price_el = card.select_one("div.product-buy__price")
        if price_el:
            return self._parse_price_text(price_el.get_text())

        # Вариант 2: любой элемент с классом содержащим "price"
        price_el = card.select_one("[class*='product-buy__price']")
        if price_el:
            return self._parse_price_text(price_el.get_text())

        # Вариант 3: data-product-price атрибут
        price_attr = card.get("data-product-price")
        if price_attr:
            try:
                return int(float(price_attr))
            except (ValueError, TypeError):
                pass

        return None

    def _parse_price_text(self, text):
        """Извлекает число из текста цены ('54 999 ₽' → 54999)."""
        digits = re.sub(r"[^\d]", "", text)
        if digits:
            return int(digits)
        return None
