"""
Парсер NIX.ru — комплектующие ПК.

Прямые HTTP-запросы (без Playwright):
  GET /price/price_list.html?section={section}&page={N}

Товары в HTML-таблице: <tr> строки, <a href="/autocatalog/..."> — название,
цифровые <td> — цена.
"""

import re
import time

import requests
from bs4 import BeautifulSoup

from base_parser import BaseParser


class NixParser(BaseParser):
    SOURCE_NAME = "nix"
    CATALOG_URL = "https://www.nix.ru/price/price_list.html?section=video_cards_all"
    BASE_URL = "https://www.nix.ru"
    CARD_SELECTOR = ""
    DELAY_BETWEEN_PAGES = 2

    _SECTION = "video_cards_all"

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
        "Referer": "https://www.nix.ru/",
    }

    # ------------------------------------------------------------------ #
    # Точка входа                                                         #
    # ------------------------------------------------------------------ #

    def run(self):
        s = requests.Session()
        s.headers.update(self._HEADERS)
        products = []
        page = 1

        while page <= self.MAX_PAGES:
            url = (
                f"{self.BASE_URL}/price/price_list.html"
                f"?section={self._SECTION}&page={page}"
            )
            print(f"[{self.SOURCE_NAME}] Страница {page}: {url}")
            try:
                resp = s.get(url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Ошибка: {e}")
                break

            page_products = self.parse_products(resp.text)
            if not page_products:
                print(f"[{self.SOURCE_NAME}] Стр. {page}: товаров нет — стоп")
                break

            total = self.get_total_pages(resp.text)
            print(f"[{self.SOURCE_NAME}] Стр. {page}/{total}: {len(page_products)} товаров")
            products.extend(page_products)

            if page >= total:
                break
            page += 1
            time.sleep(self.DELAY_BETWEEN_PAGES)

        print(f"[{self.SOURCE_NAME}] Итого: {len(products)}")
        return products

    # ------------------------------------------------------------------ #
    # Парсинг HTML                                                        #
    # ------------------------------------------------------------------ #

    def parse_products(self, html):
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen = set()

        for row in soup.find_all("tr"):
            link = row.find("a", href=re.compile(r"/autocatalog/"))
            if not link:
                continue

            href = link.get("href", "")
            # ID = последний непустой сегмент пути
            parts = [p for p in href.strip("/").split("/") if p]
            product_id = parts[-1] if parts else ""
            if not product_id or product_id in seen:
                continue
            seen.add(product_id)

            name = re.sub(r"\s+", " ", link.get_text(" ", strip=True)).strip()
            if not name or len(name) < 5:
                continue

            # Цена — первая ячейка с числом 4-7 цифр (пробелы — разделители тысяч)
            price = None
            for td in row.find_all("td"):
                raw = re.sub(r"\s", "", td.get_text())
                if re.fullmatch(r"\d{4,7}", raw):
                    val = int(raw)
                    if 300 < val < 10_000_000:
                        if price is None or val < price:
                            price = val

            if not price:
                continue

            url = self.BASE_URL + href if href.startswith("/") else href
            products.append({
                "id":       product_id,
                "name":     name,
                "price":    price,
                "url":      url,
                "in_stock": True,
            })

        return products

    def get_total_pages(self, html):
        soup = BeautifulSoup(html, "lxml")
        max_page = 1
        # Ссылки вида ?page=N или &page=N
        for a in soup.find_all("a", href=re.compile(r"page=\d+")):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        # Числа в ссылках пагинации
        for a in soup.select("a"):
            t = a.get_text(strip=True)
            if t.isdigit() and 1 < int(t) <= 200:
                max_page = max(max_page, int(t))
        return max_page

    def get_page_url(self, page_num):
        return (
            f"{self.BASE_URL}/price/price_list.html"
            f"?section={self._SECTION}&page={page_num}"
        )
