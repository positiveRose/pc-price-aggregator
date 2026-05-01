"""
Парсер M.Video (mvideo.ru) — видеокарты.

Прямые HTTP-запросы к BFF API (без Playwright):
  /bff/products/v2/search      GET  — список productId + total
  /bff/product-details/list    POST — названия товаров
  /bff/products/prices         GET  — цены (productIds)

categoryId берётся из CATALOG_URL (последнее число, напр. videokarty-5429 → 5429).
"""

import re
import time

import requests

from base_parser import BaseParser


class MvideoParser(BaseParser):
    SOURCE_NAME = "mvideo"
    CATALOG_URL = "https://www.mvideo.ru/komputernye-komplektuushhie-5427/videokarty-5429"
    BASE_URL = "https://www.mvideo.ru"
    CARD_SELECTOR = ""   # не используется
    PAGE_LIMIT = 36
    DELAY_BETWEEN_PAGES = 2

    def __init__(self):
        self._session = None

    def _get_session(self):
        """Создаёт новую сессию для каждого экземпляра парсера."""
        if self._session is None:
            s = requests.Session()
            s.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Origin": "https://www.mvideo.ru",
                "Referer": "https://www.mvideo.ru/",
                "x-client-name": "ru.mvideo.product-detail-page",
                "x-client-version": "2.0.0",
                "x-app-version": "1.0.0",
            })
            # Прогрев: получаем cookies с главной и страницы категории
            for url in ("https://www.mvideo.ru/", self.CATALOG_URL):
                try:
                    s.get(url, timeout=20)
                    time.sleep(1)
                except Exception:
                    pass
            self._session = s
        return self._session

    # ------------------------------------------------------------------ #
    # Точка входа                                                         #
    # ------------------------------------------------------------------ #

    def run(self):
        category_id = self._extract_category_id(self.CATALOG_URL)
        if not category_id:
            print(f"[{self.SOURCE_NAME}] Не удалось извлечь categoryId из {self.CATALOG_URL}")
            return []

        print(f"[{self.SOURCE_NAME}] categoryId={category_id} | {self.CATALOG_URL}")

        try:
            product_ids = self._fetch_all_ids(category_id)
        except Exception as e:
            print(f"[{self.SOURCE_NAME}] Ошибка listing API: {e}")
            return []

        if not product_ids:
            print(f"[{self.SOURCE_NAME}] Listing вернул 0 товаров")
            return []

        print(f"[{self.SOURCE_NAME}] Получено ID: {len(product_ids)}")

        details = self._fetch_details(product_ids)
        prices  = self._fetch_prices(product_ids)

        products = []
        for pid in product_ids:
            info  = details.get(pid)
            price = prices.get(pid)
            if not info or not price:
                continue
            name       = info["name"]
            translit   = info["translit"]
            url = (
                f"{self.BASE_URL}/products/{translit}-{pid}"
                if translit
                else f"{self.BASE_URL}/products/{pid}"
            )
            products.append({
                "id":       pid,
                "name":     name,
                "price":    price,
                "url":      url,
                "in_stock": True,
            })

        print(f"[{self.SOURCE_NAME}] Найдено товаров: {len(products)}")
        return products

    # ------------------------------------------------------------------ #
    # BFF API — шаг 1: получить список ID                                 #
    # ------------------------------------------------------------------ #

    def _fetch_all_ids(self, category_id):
        s = self._get_session()
        # Посещаем страницу категории — сайт может требовать cookies с неё
        try:
            s.get(self.CATALOG_URL, timeout=20)
        except Exception:
            pass
        s.headers.update({"Referer": self.CATALOG_URL})

        url = "https://www.mvideo.ru/bff/products/v2/search"
        all_ids = []
        offset = 0
        total = None
        pages = 0

        while True:
            params = {
                "categoryIds": category_id,
                "offset":      offset,
                "limit":       self.PAGE_LIMIT,
                "doTranslit":  "true",
                "context":     "",
            }
            resp = s.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            body = data.get("body", data)
            pids = (body.get("products")
                    or body.get("productIds")
                    or body.get("items")
                    or [])

            if total is None:
                for field in ("total", "totalItems", "totalCount",
                              "total_count", "count", "size"):
                    val = body.get(field)
                    if val is not None:
                        total = int(val)
                        break
                else:
                    total = 0
                print(f"[{self.SOURCE_NAME}] Всего товаров: {total}")
                self._last_total = total

            # Прекращаем только при пустом ответе
            if not pids:
                break

            all_ids.extend(str(p) for p in pids)
            pages += 1
            offset += self.PAGE_LIMIT

            if (total > 0 and offset >= total) or pages >= self.MAX_PAGES:
                break
            time.sleep(self.DELAY_BETWEEN_PAGES)

        return list(dict.fromkeys(all_ids))  # порядок сохранён, дубли убраны

    # ------------------------------------------------------------------ #
    # BFF API — шаг 2: названия товаров (POST)                            #
    # ------------------------------------------------------------------ #

    def _fetch_details(self, product_ids):
        s = self._get_session()
        url = "https://www.mvideo.ru/bff/product-details/list"
        details = {}
        batch_size = 24

        for i in range(0, len(product_ids), batch_size):
            batch = product_ids[i:i + batch_size]
            try:
                resp = None
                for wait in (0, 15, 30):
                    if wait:
                        print(f"[{self.SOURCE_NAME}] details batch {i}: ждём {wait}с перед retry...")
                        time.sleep(wait)
                    resp = s.post(url, json={"productIds": batch}, timeout=30)
                    if resp.status_code != 403:
                        break
                    # Сбрасываем сессию при 403 — пересоздаём cookies
                    self._session = None
                    s = self._get_session()
                resp.raise_for_status()
                data = resp.json()

                for item in data.get("body", {}).get("products", []):
                    pid      = str(item.get("productId") or item.get("id") or "")
                    brand    = item.get("brandName") or ""
                    name     = item.get("name") or item.get("title") or ""
                    full     = f"{brand} {name}".strip() if brand else name
                    translit = item.get("nameTranslit") or ""
                    if pid and full:
                        details[pid] = {"name": full, "translit": translit}

                time.sleep(1)
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Ошибка details batch {i}: {e}")

        return details

    # ------------------------------------------------------------------ #
    # BFF API — шаг 3: цены (GET, productIds)                             #
    # ------------------------------------------------------------------ #

    def _fetch_prices(self, product_ids):
        s = self._get_session()
        url = "https://www.mvideo.ru/bff/products/prices"
        prices = {}
        batch_size = 24

        for i in range(0, len(product_ids), batch_size):
            batch = product_ids[i:i + batch_size]
            try:
                params = {"productIds": ",".join(batch)}
                resp = s.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                for item in data.get("body", {}).get("materialPrices", []):
                    pb  = item.get("price", item)
                    pid = str(pb.get("productId") or item.get("productId") or "")
                    val = pb.get("salePrice") or pb.get("basePrice")
                    if pid and val:
                        try:
                            price = int(float(val))
                            if 300 < price < 10_000_000:
                                prices[pid] = price
                        except (ValueError, TypeError):
                            pass

                time.sleep(1)
            except Exception as e:
                print(f"[{self.SOURCE_NAME}] Ошибка prices batch {i}: {e}")

        return prices

    # ------------------------------------------------------------------ #
    # Утилиты                                                             #
    # ------------------------------------------------------------------ #

    def _extract_category_id(self, url):
        """videokarty-5429 → '5429'"""
        m = re.search(r"-(\d{4,})/?$", url.rstrip("/"))
        return m.group(1) if m else None

    # Эти методы не используются, но требуются ABC
    def parse_products(self, html):
        return []

    def get_total_pages(self, html):
        return 1

    def get_page_url(self, page_num):
        return self.CATALOG_URL
