"""
Парсер FCenter (fcenter.ru) — комплектующие ПК.

SSR сайт. requests + BeautifulSoup.
Структура карточки:
  <div class="goods-item">
    <div class="goods-info">
      <a class="goods-name" href="/product/goods/{id}-{slug}">название</a>
      <div class="goods-price">7 99100 р</div>  ← цена в копейках (÷100 = рубли)
    </div>
  </div>
Пагинация: ?view=2&offset=N&max=24
"""

import re
import time

import requests
from bs4 import BeautifulSoup

from base_parser import get_requests_proxies

BASE_URL = "https://fcenter.ru"
_PAGE_SIZE = 24

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://fcenter.ru/",
}


def _fetch(session, url, proxies):
    resp = session.get(url, timeout=30, proxies=proxies)
    resp.raise_for_status()
    return resp.text  # сайт реально UTF-8, Content-Type корректен


def _parse_price(price_el) -> int | None:
    """
    Структура: <div class="do-price">7 991<sup class="kop">00</sup> р</div>
    Убираем <sup> с копейками, берём только рубли.
    """
    if not price_el:
        return None
    # Убираем sup (копейки)
    for sup in price_el.find_all("sup"):
        sup.decompose()
    digits = re.sub(r"[^\d]", "", price_el.get_text())
    if digits:
        val = int(digits)
        if 300 < val < 10_000_000:
            return val
    return None


class FcenterParser:
    SOURCE_NAME = "fcenter"
    _CATEGORY = "GPU"
    _TYPE_ID = 7
    MAX_PAGES = 50

    @property
    def _catalog_url(self):
        return f"{BASE_URL}/product/type/{self._TYPE_ID}"

    def run(self):
        session = requests.Session()
        session.headers.update(_HEADERS)
        proxies = get_requests_proxies()

        all_products = []
        seen_ids = set()
        offset = 0

        while offset // _PAGE_SIZE < self.MAX_PAGES:
            url = f"{self._catalog_url}?view=2&offset={offset}&max={_PAGE_SIZE}"
            try:
                html = _fetch(session, url, proxies)
            except Exception as e:
                print(f"[fcenter] Ошибка offset={offset}: {e}")
                break

            products = self._parse_page(html)
            if not products:
                break

            added = 0
            for p in products:
                p["category"] = self._CATEGORY
                if p["id"] not in seen_ids:
                    seen_ids.add(p["id"])
                    all_products.append(p)
                    added += 1

            page_num = offset // _PAGE_SIZE + 1
            print(f"[fcenter] Стр. {page_num}: {added} товаров")

            if len(products) < _PAGE_SIZE:
                break  # последняя страница

            offset += _PAGE_SIZE
            time.sleep(2)

        print(f"[fcenter] Итого: {len(all_products)}")
        return all_products

    def _parse_page(self, html: str) -> list:
        soup = BeautifulSoup(html, "lxml")
        products = []

        for card in soup.select("div.pic-table-item"):
            # ID — из data-goods-id атрибута кнопки корзины
            shopping_btn = card.select_one("[data-goods-id]")
            if not shopping_btn:
                continue
            item_id = shopping_btn.get("data-goods-id", "").strip()
            if not item_id:
                continue

            # Название
            name_a = card.select_one("a.goods-link")
            if not name_a:
                continue
            name = name_a.get("title", "").strip() or name_a.get_text(strip=True)
            if not name or len(name) < 5:
                continue

            # URL
            href = name_a.get("href", "")
            url = BASE_URL + href if href.startswith("/") else href

            # Цена — убираем <sup> с копейками перед парсингом
            price_el = card.select_one("div.do-price")
            price = _parse_price(price_el)
            if not price:
                continue

            products.append({
                "id": item_id,
                "name": name,
                "price": price,
                "url": url,
                "in_stock": True,
            })

        return products
