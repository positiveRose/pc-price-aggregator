"""
Парсер Регард (regard.ru) — видеокарты.

Структура карточки:
- Контейнер: div с классом Card_wrap__*
- Название: img[alt] внутри ссылки на /product/
- Ссылка: a[href*="/product/"]
- Цена: элемент с классом CardPrice_price__*
- ID: из URL (/product/459278/...)
"""

import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class RegardParser(BaseParser):
    SOURCE_NAME = "regard"
    CATALOG_URL = "https://www.regard.ru/catalog/1013/videokarty"
    BASE_URL = "https://www.regard.ru"
    CARD_SELECTOR = "div[class^='Card_wrap']"
    _CATEGORY = "GPU"
    BROWSER_RESTART_EVERY = 5

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
        # Ссылка на товар
        link = card.select_one('a[href*="/product/"]')
        if not link:
            return None

        href = link.get("href", "")
        url = href if href.startswith("http") else self.BASE_URL + href

        # Название: сначала из alt картинки, потом из текста ссылки
        img = card.select_one("img[alt]")
        name = img["alt"].strip() if img and img.get("alt", "").strip() else ""
        if not name:
            name = link.get_text(strip=True)
        if not name:
            return None

        # Цена — из элемента CardPrice_price__*
        price = self._extract_price(card)
        if not price:
            return None

        # ID из URL: /product/459278/... → 459278
        product_id = self._extract_id(href)

        return {
            "id": product_id,
            "name": name,
            "price": price,
            "url": url,
            "in_stock": True,
        }

    def _extract_price(self, card):
        """Извлекает цену из карточки."""
        # Основной селектор
        price_el = card.find(class_=re.compile(r"CardPrice_price__"))
        if price_el:
            return self._parse_price_text(price_el.get_text())

        # Fallback — первый Price_price__
        price_el = card.find(class_=re.compile(r"Price_price__"))
        if price_el:
            return self._parse_price_text(price_el.get_text())

        return None

    def _parse_price_text(self, text):
        """'20 990₽' → 20990"""
        digits = re.sub(r"[^\d]", "", text)
        if digits:
            val = int(digits)
            if 300 < val < 10_000_000:
                return val
        return None

    def _extract_id(self, href):
        """/product/459278/nakopitel-... → 459278"""
        match = re.search(r"/product/(\d+)", href)
        if not match:
            # Используем URL как fallback вместо "unknown" во избежание конфликтов source_id
            return href.split("?")[0].rstrip("/").rsplit("/", 1)[-1] or href
        return match.group(1)

    def get_total_pages(self, html):
        """Регард: ищем максимальный номер в пагинации или считаем из общего числа."""
        soup = BeautifulSoup(html, "lxml")

        # Вариант 1: числа в пагинации (1, 2, 3, ... 29)
        pagination = soup.find("div", class_=re.compile(r"Pagination_pagination"))
        if pagination:
            max_page = 1
            for el in pagination.find_all("a"):
                text = el.get_text(strip=True)
                if text.isdigit():
                    max_page = max(max_page, int(text))
            if max_page > 1:
                return max_page

        # Вариант 2: "693 товара" / 24 на страницу
        count_el = soup.find(class_=re.compile(r"ListingPageTitle_count"))
        if count_el:
            digits = re.sub(r"[^\d]", "", count_el.get_text())
            if digits:
                total = int(digits)
                return (total + 23) // 24  # ceil division

        return 1

    def get_page_url(self, page_num):
        return f"{self.CATALOG_URL}?page={page_num}"
