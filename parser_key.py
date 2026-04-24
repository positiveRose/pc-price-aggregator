"""
Парсер КЕЙ (key.ru) — комплектующие ПК.

key.ru — специализированный магазин комплектующих на 1С-Битрикс.
Playwright + HTML parsing. Пагинация: ?PAGEN_1=N.

URL товаров: /catalog/{категория}/{название}-{id}/
"""
import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class KeyParser(BaseParser):
    SOURCE_NAME = "key"
    CATALOG_URL = "https://www.key.ru/catalog/videokarty/"
    BASE_URL = "https://www.key.ru"
    CARD_SELECTOR = "[class*='catalog'], [class*='product-item'], [class*='goods']"
    WAIT_TIMEOUT = 20000
    DELAY_BETWEEN_PAGES = 3

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen = set()

        # Битрикс: ссылки на товары содержат ID в конце пути
        for a in soup.find_all("a", href=re.compile(r"/catalog/[^/]+/[^/]+-\d+/")):
            try:
                href = a.get("href", "")
                m = re.search(r"-(\d+)/?$", href)
                pid = m.group(1) if m else href.rstrip("/").rsplit("/", 1)[-1]
                if not pid or pid in seen:
                    continue
                seen.add(pid)

                name = a.get("title", "").strip() or a.get_text(strip=True)
                if not name or len(name) < 5:
                    continue

                price = self._find_price_near(a)
                if not price:
                    continue

                url = href if href.startswith("http") else self.BASE_URL + href
                products.append({
                    "id": pid, "name": name, "price": price,
                    "url": url, "in_stock": True,
                })
            except Exception:
                continue

        return products

    def _find_price_near(self, link):
        """Ищет цену в родительских элементах (до 8 уровней)."""
        el = link
        for _ in range(8):
            el = el.parent
            if el is None:
                break
            for cand in el.select(
                "[class*='price'], [class*='Price'], [class*='cost'], "
                "[class*='sum'], strong, b"
            ):
                digits = re.sub(r"[^\d]", "", cand.get_text())
                if digits:
                    val = int(digits)
                    if 300 < val < 10_000_000:
                        return val
        return None

    def get_total_pages(self, html):
        soup = BeautifulSoup(html, "lxml")
        max_page = 1
        # Битрикс: ?PAGEN_1=N
        for a in soup.find_all("a", href=re.compile(r"PAGEN_\d+=\d+")):
            m = re.search(r"PAGEN_\d+=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        if max_page == 1:
            for a in soup.select("[class*='paginat'] a, [class*='pager'] a, [class*='pages'] a"):
                t = a.get_text(strip=True)
                if t.isdigit() and 1 < int(t) <= 300:
                    max_page = max(max_page, int(t))
        return max_page

    def get_page_url(self, page_num):
        return f"{self.CATALOG_URL}?PAGEN_1={page_num}"
