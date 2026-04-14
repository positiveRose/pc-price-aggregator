"""
Парсер e2e4 (e2e4online.ru) — видеокарты.

Сайт на Nuxt.js (SSR). Playwright рендерит страницу,
затем ищем карточки товаров в сгенерированном HTML.

Структура карточки (Nuxt/Vue компоненты):
- Контейнер: div.product-card  или  div.catalog-item
- Название: a.product-card__name  или  a[href*="/catalog/item/"]
- Цена: span.price  или  div.product-card__price
- ID: из URL /catalog/item/{slug}/
"""

import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class E2e4Parser(BaseParser):
    SOURCE_NAME = "e2e4"
    CATALOG_URL = "https://e2e4online.ru/catalog/videokarty-11/"
    BASE_URL = "https://e2e4online.ru"
    # Ждём появления ссылок на товары
    CARD_SELECTOR = 'a[href*="/catalog/item/"]'
    WAIT_TIMEOUT = 25000
    DELAY_BETWEEN_PAGES = 4

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen_ids = set()

        # Стратегия 1: найти все ссылки на товары
        links = soup.select('a[href*="/catalog/item/"]')

        for link in links:
            try:
                href = link.get("href", "")
                product_id = self._extract_id(href)
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                name = link.get_text(strip=True)
                if not name or len(name) < 5:
                    # Попробовать alt изображения
                    img = link.find("img")
                    if img and img.get("alt", "").strip():
                        name = img["alt"].strip()
                    else:
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

        # Стратегия 2 (fallback): ищем по классам карточек
        if not products:
            products = self._parse_by_card_classes(soup)

        return products

    def _parse_by_card_classes(self, soup):
        """Запасной парсинг по классам карточек."""
        products = []
        seen_ids = set()

        # Возможные классы карточек на e2e4
        card_selectors = [
            "div.product-card",
            "div.catalog-item",
            "li.catalog-item",
            "article.product",
        ]

        cards = []
        for sel in card_selectors:
            cards = soup.select(sel)
            if cards:
                break

        for card in cards:
            try:
                link = card.find("a", href=re.compile(r"/catalog/item/"))
                if not link:
                    continue

                href = link.get("href", "")
                product_id = self._extract_id(href)
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)

                name = link.get_text(strip=True) or ""
                if not name:
                    img = card.find("img")
                    name = img.get("alt", "").strip() if img else ""
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
            except Exception:
                continue

        return products

    def _find_price_near(self, element):
        """Ищет цену рядом с элементом (до 7 уровней вверх)."""
        price_classes = re.compile(r"price", re.I)
        el = element
        for _ in range(7):
            el = el.parent
            if el is None:
                break
            # Ищем элемент с классом содержащим "price"
            price_el = el.find(class_=price_classes)
            if price_el:
                price = self._parse_price_text(price_el.get_text())
                if price:
                    return price
            # Или в <strong> / <b>
            for tag in ("strong", "b"):
                bold = el.find(tag)
                if bold:
                    price = self._parse_price_text(bold.get_text())
                    if price:
                        return price
        return None

    def _parse_price_text(self, text):
        """'29 990 ₽' → 29990"""
        digits = re.sub(r"[^\d]", "", text)
        if digits:
            val = int(digits)
            if 300 < val < 10_000_000:
                return val
        return None

    def _extract_id(self, href):
        """/catalog/item/some-slug-12345/ → slug-12345"""
        match = re.search(r"/catalog/item/([^/?#]+)", href)
        return match.group(1) if match else None

    def get_total_pages(self, html):
        """Ищет максимальный номер страницы в пагинации."""
        soup = BeautifulSoup(html, "lxml")
        max_page = 1

        # Вариант 1: ссылки с ?page=N
        for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
            m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))

        # Вариант 2: ссылки с ?PAGEN_1=N (Bitrix-style)
        if max_page == 1:
            for a in soup.find_all("a", href=re.compile(r"PAGEN_1=\d+")):
                m = re.search(r"PAGEN_1=(\d+)", a.get("href", ""))
                if m:
                    max_page = max(max_page, int(m.group(1)))

        # Вариант 3: /page-N/
        if max_page == 1:
            for a in soup.find_all("a", href=re.compile(r"/page-\d+")):
                m = re.search(r"/page-(\d+)", a.get("href", ""))
                if m:
                    max_page = max(max_page, int(m.group(1)))

        return max_page

    def get_page_url(self, page_num):
        base = self.CATALOG_URL.rstrip("/")
        return f"{base}/?page={page_num}"
