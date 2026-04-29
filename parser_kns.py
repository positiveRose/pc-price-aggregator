"""
Парсер KNS (kns.ru) — комплектующие ПК.

Сайт возвращает cp1251 (несмотря на заголовок utf-8).
Товары встроены в HTML как window.goodsList.push({...}) блоки (30/стр).
URL-адреса берутся из <a href="/product/..."> тегов, порядок совпадает.
requests + BeautifulSoup, без Playwright.
"""

import re
import time

import requests
from bs4 import BeautifulSoup

from base_parser import get_requests_proxies

BASE_URL = "https://www.kns.ru"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


def _fetch(session, url, proxies):
    resp = session.get(url, timeout=30, proxies=proxies)
    resp.raise_for_status()
    return resp.text  # сайт реально UTF-8, Content-Type корректен


def _extract_goods(html: str) -> list:
    """Парсит window.goodsList.push({...}) блоки → список dict."""
    items = []
    blocks = re.findall(r"goodsList\.push\(\{(.+?)\}\);", html, re.DOTALL)
    for block in blocks:
        item = {}
        for m in re.finditer(r"(\w+):\s*'([^']*?)'", block):
            item[m.group(1)] = m.group(2)
        if "item_id" in item and "price" in item and "item_name" in item:
            items.append(item)
    return items


def _extract_product_urls(html: str) -> list:
    """Извлекает уникальные ссылки /product/.../ в порядке появления."""
    soup = BeautifulSoup(html, "lxml")
    seen = set()
    urls = []
    for a in soup.select('a[href^="/product/"]'):
        href = a.get("href", "")
        if href not in seen:
            seen.add(href)
            urls.append(BASE_URL + href)
    return urls


def _get_total_pages(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    max_page = 1
    for a in soup.find_all("a", href=re.compile(r"/page\d+/")):
        m = re.search(r"/page(\d+)/", a.get("href", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def _parse_price(price_str: str) -> int | None:
    """'33252.6000' → 33253"""
    try:
        val = round(float(price_str))
        if 300 < val < 10_000_000:
            return val
    except (ValueError, TypeError):
        pass
    return None


class KnsParser:
    SOURCE_NAME = "kns"
    _CATEGORY = "GPU"
    CATALOG_URL = f"{BASE_URL}/catalog/komplektuyuschie/videokarty/"
    MAX_PAGES = 30

    def run(self):
        session = requests.Session()
        session.headers.update(_HEADERS)
        proxies = get_requests_proxies()

        all_products = []
        seen_ids = set()

        try:
            html = _fetch(session, self.CATALOG_URL, proxies)
        except Exception as e:
            print(f"[kns] Ошибка загрузки: {e}")
            return []

        total = min(_get_total_pages(html), self.MAX_PAGES)
        print(f"[kns] Страниц: {total}")

        for page_num in range(1, total + 1):
            if page_num > 1:
                time.sleep(2)
                url = self.CATALOG_URL.rstrip("/") + f"/page{page_num}/"
                try:
                    html = _fetch(session, url, proxies)
                except Exception as e:
                    print(f"[kns] Ошибка стр. {page_num}: {e}")
                    continue

            goods = _extract_goods(html)
            urls = _extract_product_urls(html)

            added = 0
            for i, item in enumerate(goods):
                item_id = item.get("item_id", "").strip()
                if not item_id or item_id in seen_ids:
                    continue

                name = item.get("item_name", "").strip()
                if not name or len(name) < 5:
                    continue

                price = _parse_price(item.get("price", ""))
                if not price:
                    continue

                product_url = urls[i] if i < len(urls) else self.CATALOG_URL

                seen_ids.add(item_id)
                all_products.append({
                    "id": item_id,
                    "name": name,
                    "price": price,
                    "url": product_url,
                    "in_stock": True,
                    "category": self._CATEGORY,
                })
                added += 1

            print(f"[kns] Стр. {page_num}: {added} товаров")

        print(f"[kns] Итого: {len(all_products)}")
        return all_products
