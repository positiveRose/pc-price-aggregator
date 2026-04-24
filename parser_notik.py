"""
Парсер Нотик (notik.ru) — комплектующие ПК.

Playwright + HTML parsing.
URL каталога: /search_catalog/filter/{категория}/
Пагинация: /page-{N}/
URL товаров: /goods/{id}/
"""
import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class NotikParser(BaseParser):
    SOURCE_NAME = "notik"
    # /search_catalog/filter/ — реальный каталог с товарными карточками
    CATALOG_URL = "https://www.notik.ru/search_catalog/filter/videocard/"
    BASE_URL = "https://www.notik.ru"
    CARD_SELECTOR = "[class*='item'], [class*='product'], [class*='goods']"
    WAIT_TIMEOUT = 20000
    DELAY_BETWEEN_PAGES = 3

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen = set()

        # Notik: ссылки на товары /goods/{id}/ или /search_catalog/.../{slug}/
        for a in soup.find_all("a", href=re.compile(r"/goods/\d+/")):
            try:
                href = a.get("href", "")
                m = re.search(r"/goods/(\d+)/", href)
                pid = m.group(1) if m else None
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
        # Нотик: пагинация /page-N/ в href
        for a in soup.find_all("a", href=re.compile(r"/page-(\d+)/")):
            m = re.search(r"/page-(\d+)/", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        if max_page == 1:
            for a in soup.select("[class*='paginat'] a, [class*='pager'] a, [class*='page'] a"):
                t = a.get_text(strip=True)
                if t.isdigit() and 1 < int(t) <= 500:
                    max_page = max(max_page, int(t))
        return max_page

    def get_page_url(self, page_num):
        base = self.CATALOG_URL.rstrip("/")
        return base + "/" if page_num == 1 else f"{base}/page-{page_num}/"
