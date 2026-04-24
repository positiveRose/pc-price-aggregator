"""
Парсер Ultra (ultra.ru) — комплектующие ПК.

Playwright + HTML parsing. Пагинация: ?page=N.
URL товаров: /{категория}/{slug}.html или /{категория}/{id}/
"""
import re

from bs4 import BeautifulSoup

from base_parser import BaseParser


class UltraParser(BaseParser):
    SOURCE_NAME = "ultra"
    CATALOG_URL = "https://www.ultra.ru/videokarty/"
    BASE_URL = "https://www.ultra.ru"
    CARD_SELECTOR = "[class*='product'], [class*='catalog-item'], [class*='goods']"
    WAIT_TIMEOUT = 20000
    DELAY_BETWEEN_PAGES = 3

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen = set()

        # ultra.ru — ссылки на товары: как минимум один сегмент пути после категории
        for a in soup.find_all("a", href=re.compile(r"^/[^/]+/[^/?#]+")):
            try:
                href = a.get("href", "")
                parts = [x for x in href.strip("/").split("/") if x]
                if len(parts) < 2:
                    continue
                if "?" in href or "#" in href:
                    continue

                pid = parts[-1].rstrip(".html")
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
