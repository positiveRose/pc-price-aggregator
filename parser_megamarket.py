"""
Парсер МегаМаркет (megamarket.ru) — комплектующие ПК.

Playwright + stealth (antibot обходится браузером).
Стратегии разбора HTML:
  1. __NEXT_DATA__ (JSON встроен в страницу)
  2. HTML-карточки товаров (fallback)

URL категорий: /catalog/{slug}/
Пагинация: ?p=N
"""
import json
import re

from base_parser import BaseParser


class MegamarketParser(BaseParser):
    SOURCE_NAME = "megamarket"
    CATALOG_URL = "https://megamarket.ru/catalog/videokarty/"
    BASE_URL = "https://megamarket.ru"
    # Пустой CARD_SELECTOR — ждём фиксированное время (5 сек), затем парсим __NEXT_DATA__
    CARD_SELECTOR = ""
    WAIT_TIMEOUT = 25000
    DELAY_BETWEEN_PAGES = 5
    MAX_PAGES = 50

    # ------------------------------------------------------------------ #
    # Разбор HTML / __NEXT_DATA__                                         #
    # ------------------------------------------------------------------ #

    def parse_products(self, html):
        products = self._parse_next_data(html)
        if not products:
            products = self._parse_html_cards(html)
        return products

    def _parse_next_data(self, html):
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

        found = {}
        self._deep_collect(data, found, depth=0)
        return list(found.values())

    def _deep_collect(self, obj, found, depth):
        """Рекурсивно собирает объекты-товары из JSON-дерева."""
        if depth > 15:
            return
        if isinstance(obj, dict):
            product = self._try_as_product(obj)
            if product and product["id"] not in found:
                found[product["id"]] = product
                return
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    self._deep_collect(v, found, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._deep_collect(item, found, depth + 1)

    def _try_as_product(self, obj):
        """Проверяет, похож ли словарь на товар; возвращает продукт или None."""
        # ID
        pid = str(
            obj.get("goodsId") or obj.get("id") or obj.get("offerId")
            or obj.get("sku") or obj.get("productId") or ""
        )
        if not pid or not any(c.isdigit() for c in pid):
            return None

        # Название
        name = (
            obj.get("shortName") or obj.get("name") or obj.get("title")
            or obj.get("goodsName") or obj.get("displayName") or ""
        )
        if not name or len(str(name)) < 5:
            return None

        # Цена
        price = self._extract_price(obj)
        if not price:
            return None

        # URL
        url_path = (
            obj.get("webUrl") or obj.get("url") or obj.get("link") or obj.get("href")
            or f"/catalog/{pid}/"
        )
        if not str(url_path).startswith("http"):
            url_path = self.BASE_URL + str(url_path)

        return {
            "id":       pid,
            "name":     str(name),
            "price":    price,
            "url":      url_path,
            "in_stock": True,
        }

    def _extract_price(self, obj):
        """Извлекает цену из объекта товара (рубли или копейки)."""
        # Прямые поля
        for key in ("finalPrice", "salePrice", "price", "minPrice",
                    "discountPrice", "basePrice", "currentPrice"):
            raw = obj.get(key)
            if raw is not None:
                p = self._to_rubles(raw)
                if p:
                    return p

        # Вложенный объект prices / price
        for field in ("prices", "price"):
            sub = obj.get(field)
            if isinstance(sub, dict):
                for pk in ("finalPrice", "salePrice", "discount", "sale",
                           "current", "base", "value", "price"):
                    raw = sub.get(pk)
                    if raw is not None:
                        p = self._to_rubles(raw)
                        if p:
                            return p
        return None

    def _to_rubles(self, raw):
        try:
            val = int(float(str(raw).replace(" ", "").replace(",", ".")))
            if val > 10_000_000:
                val //= 100
            if 300 < val < 10_000_000:
                return val
        except (ValueError, TypeError):
            pass
        return None

    # ------------------------------------------------------------------ #
    # HTML fallback (карточки)                                            #
    # ------------------------------------------------------------------ #

    def _parse_html_cards(self, html):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        products = []
        seen = set()

        for card in soup.select(
            "[class*='ProductCard'], [class*='product-card'], "
            "[class*='CatalogCard'], [class*='catalog-card'], "
            "[class*='SnippetCard'], [data-product-id], [data-goods-id]"
        ):
            try:
                link = card.select_one("a[href]")
                if not link:
                    continue
                href = link.get("href", "")
                pid = re.sub(r"[^\w-]", "", href.rstrip("/").rsplit("/", 1)[-1])
                if not pid or pid in seen:
                    continue
                seen.add(pid)

                name = link.get("title", "").strip()
                if not name:
                    el = card.select_one("[class*='name'], [class*='title'], [class*='Name']")
                    name = el.get_text(strip=True) if el else link.get_text(strip=True)
                if not name or len(name) < 5:
                    continue

                price = None
                for cand in card.select(
                    "[class*='price'], [class*='Price'], [class*='cost']"
                ):
                    digits = re.sub(r"[^\d]", "", cand.get_text())
                    if digits:
                        p = self._to_rubles(digits)
                        if p:
                            price = p
                            break
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

    def get_total_pages(self, html):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        max_page = 1
        for a in soup.find_all("a", href=re.compile(r"[?&]p=\d+")):
            m = re.search(r"[?&]p=(\d+)", a.get("href", ""))
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
        return base + "/" if page_num == 1 else f"{base}/?p={page_num}"
