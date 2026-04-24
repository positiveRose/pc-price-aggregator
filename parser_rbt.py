"""
Парсер RBT (rbt.ru) — комплектующие ПК.

RBT — крупная розничная сеть электроники (Урал/Сибирь).
Playwright + HTML parsing. Пагинация: ?page=N.
URL товаров: /catalog/{категория}/{slug}/
"""
import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class RbtParser(BaseParser):
    SOURCE_NAME = "rbt"
    CATALOG_URL = "https://rbt.ru/catalog/videokarty/"
    BASE_URL = "https://rbt.ru"
    CARD_SELECTOR = "[class*='product'], [class*='catalog-item'], [class*='ProductCard']"
    WAIT_TIMEOUT = 20000
    DELAY_BETWEEN_PAGES = 3

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen = set()

        # rbt.ru: ссылки на товары /catalog/{категория}/{slug}/
        for a in soup.find_all("a", href=re.compile(r"/catalog/[^/]+/[^/?#]+")):
            try:
                href = a.get("href", "")
                parts = [x for x in href.strip("/").split("/") if x]
                # Товары имеют ≥3 сегмента: catalog / категория / слаг
                if len(parts) < 3:
                    continue
                if "?" in href:
                    continue

                pid = parts[-1]
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
        for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        if max_page == 1:
            for a in soup.select("[class*='paginat'] a, [class*='pager'] a, [class*='page'] a"):
                t = a.get_text(strip=True)
                if t.isdigit() and 1 < int(t) <= 500:
                    max_page = max(max_page, int(t))
        return max_page

    def get_page_url(self, page_num):
        return f"{self.CATALOG_URL}?page={page_num}"
