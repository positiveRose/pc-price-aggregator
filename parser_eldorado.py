"""
Парсер Эльдорадо (eldorado.ru) — видеокарты.

Сайт Next.js с SSR. Товары встроены в __NEXT_DATA__ как Redux state:
  initialState → *-module → products → {productId: {name, price, code, ...}}

Playwright необходим для рендеринга страницы и получения __NEXT_DATA__.
Пагинация: ?page=N
"""

import json
import re
import time

from bs4 import BeautifulSoup

from base_parser import BaseParser


class EldoradoParser(BaseParser):
    SOURCE_NAME = "eldorado"
    CATALOG_URL = "https://www.eldorado.ru/c/videokarty/"
    BASE_URL = "https://www.eldorado.ru"
    CARD_SELECTOR = "script#__NEXT_DATA__"
    WAIT_TIMEOUT = 25000
    DELAY_BETWEEN_PAGES = 5

    def parse_products(self, html):
        """Извлекает товары из __NEXT_DATA__ JSON."""
        # Стратегия 1: JSON из __NEXT_DATA__
        products = self._parse_next_data(html)

        # Стратегия 2: fallback на HTML-карточки
        if not products:
            products = self._parse_html_cards(html)

        return products

    def _parse_next_data(self, html):
        """Парсит __NEXT_DATA__ — Redux state с товарами."""
        products = []
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

        # Рекурсивно ищем словарь вида {"productId": {id, name, price, ...}}
        items_map = self._find_products_map(data)
        if not items_map:
            return []

        seen = set()
        for pid_key, item in items_map.items():
            try:
                if not isinstance(item, dict):
                    continue
                pid = str(item.get("productId") or item.get("id") or pid_key)
                if not pid or pid in seen:
                    continue
                seen.add(pid)

                name = item.get("name") or item.get("title") or ""
                if not name or len(name) < 5:
                    continue

                price = item.get("price") or item.get("salePrice")
                if not price:
                    continue
                try:
                    price = int(float(price))
                except (ValueError, TypeError):
                    continue
                if not (300 < price < 10_000_000):
                    continue

                code = item.get("code") or item.get("slug") or pid
                url = f"{self.BASE_URL}/cat/{code}/"

                products.append({
                    "id":       pid,
                    "name":     name,
                    "price":    price,
                    "url":      url,
                    "in_stock": True,
                })
            except Exception:
                continue

        return products

    def _find_products_map(self, obj, depth=0):
        """Рекурсивно ищет словарь {productId_str: {id, name, price, ...}}.
        Собирает всех кандидатов и возвращает наибольшего."""
        candidates = []
        self._collect_map_candidates(obj, depth, candidates)
        if not candidates:
            return None
        return max(candidates, key=lambda d: len(d))

    def _collect_map_candidates(self, obj, depth, candidates):
        """Рекурсивно собирает все словари {digit_key: product_dict}."""
        if depth > 12:
            return
        if isinstance(obj, dict):
            if obj:
                sample_keys = list(obj.keys())[:10]
                sample_vals = [obj[k] for k in sample_keys]
                # Ключи — числа, значения — словари с хоть каким-то полем товара
                if (all(str(k).isdigit() for k in sample_keys) and
                        all(isinstance(v, dict) for v in sample_vals) and
                        any(("productId" in v or "id" in v or
                             "name" in v or "title" in v)
                            for v in sample_vals)):
                    candidates.append(obj)
                    return  # не спускаемся внутрь найденного словаря
            for v in obj.values():
                self._collect_map_candidates(v, depth + 1, candidates)
        elif isinstance(obj, list):
            for item in obj:
                self._collect_map_candidates(item, depth + 1, candidates)

    def _parse_html_cards(self, html):
        """Fallback: HTML-парсинг карточек."""
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

    def get_total_pages(self, html):
        soup = BeautifulSoup(html, "lxml")
        max_page = 1
        for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
            m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        # Числа в блоке пагинации
        for el in soup.select("[class*='pagination'] a, [class*='pager'] a"):
            text = el.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
        return max_page

    def get_page_url(self, page_num):
        base = self.CATALOG_URL.rstrip("/")
        return f"{base}/?page={page_num}"
