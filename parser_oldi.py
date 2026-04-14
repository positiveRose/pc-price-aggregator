"""
Парсер OLDI (oldi.ru) — видеокарты.

Структура карточки:
- Ссылка на товар: a[href*="/catalog/element/"]
- Название: текст ссылки
- Цена: <strong> в пределах ближайшего родительского блока
- Пагинация: /catalog/{category}/page-{N}/
"""

import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class OldiParser(BaseParser):
    SOURCE_NAME = "oldi"
    CATALOG_URL = "https://www.oldi.ru/catalog/videokarta/"
    BASE_URL = "https://www.oldi.ru"
    CARD_SELECTOR = 'a[href*="/catalog/element/"]'
    WAIT_TIMEOUT = 20000
    DELAY_BETWEEN_PAGES = 3

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen_ids = set()

        links = soup.select('a[href*="/catalog/element/"]')

        for link in links:
            try:
                href = link.get("href", "")
                product_id = self._extract_id(href)
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                name = link.get_text(strip=True)
                if not name or len(name) < 5:
                    continue

                url = href if href.startswith("http") else self.BASE_URL + href

                price = self._find_price_near(link)
                if not price:
                    continue

                products.append({
                    "id": product_id,
                    "name": name,
                    "price": price,
                    "url": url,
                    "in_stock": True,
                })
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Ошибка карточки: {e}")
                continue

        return products

    def _find_price_near(self, link):
        """Ищет цену в ближайших родительских элементах (до 6 уровней вверх)."""
        el = link
        for _ in range(6):
            el = el.parent
            if el is None:
                break
            # Цена в <strong> — формат "14 700 c" или "14 700 ₽"
            strong = el.find("strong")
            if strong:
                price = self._parse_price_text(strong.get_text())
                if price:
                    return price
        return None

    def _parse_price_text(self, text):
        """'14 700 c' → 14700"""
        digits = re.sub(r"[^\d]", "", text)
        if digits:
            val = int(digits)
            # Санитарная проверка: цена от 300 до 10 000 000 руб.
            if 300 < val < 10_000_000:
                return val
        return None

    def _extract_id(self, href):
        """/catalog/element/02028195/ → 02028195"""
        match = re.search(r"/catalog/element/(\w+)", href)
        return match.group(1) if match else None

    def get_total_pages(self, html):
        """Пагинация: /catalog/videokarta/page-10/ → 10."""
        soup = BeautifulSoup(html, "lxml")
        max_page = 1
        for a in soup.find_all("a", href=re.compile(r"/page-\d+/")):
            m = re.search(r"/page-(\d+)/", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def get_page_url(self, page_num):
        base = self.CATALOG_URL.rstrip("/")
        return f"{base}/page-{page_num}/"
