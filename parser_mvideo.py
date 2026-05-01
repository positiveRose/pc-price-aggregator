"""
Парсер M.Video (mvideo.ru).

Все запросы к BFF API выполняются через Playwright (page.evaluate → fetch),
чтобы браузер автоматически подставлял mvid-token и остальные заголовки,
которые сервер требует для /bff/product-details/list.

categoryId берётся из _BFF_ID (переопределяется в parser_mvideo_categories.py),
либо из CATALOG_URL (последнее число, напр. videokarty-5429 → 5429).
"""

import re
import time

from base_parser import BaseParser


class MvideoParser(BaseParser):
    SOURCE_NAME = "mvideo"
    CATALOG_URL = "https://www.mvideo.ru/komputernye-komplektuushhie-5427/videokarty-5429"
    BASE_URL    = "https://www.mvideo.ru"
    CARD_SELECTOR = ""
    PAGE_LIMIT = 36
    DELAY_BETWEEN_PAGES = 2

    # ------------------------------------------------------------------ #
    # Точка входа                                                         #
    # ------------------------------------------------------------------ #

    def run(self):
        category_id = self._extract_category_id(self.CATALOG_URL)
        if not category_id:
            print(f"[{self.SOURCE_NAME}] Не удалось извлечь categoryId из {self.CATALOG_URL}")
            return []

        print(f"[{self.SOURCE_NAME}] categoryId={category_id} | {self.CATALOG_URL}")

        products = self._fetch_via_playwright(category_id)
        print(f"[{self.SOURCE_NAME}] Найдено товаров: {len(products)}")
        return products

    # ------------------------------------------------------------------ #
    # Playwright — fetch из браузерного контекста                         #
    # ------------------------------------------------------------------ #

    def _fetch_via_playwright(self, category_id):
        """Получает товары через fetch() внутри браузерного контекста Playwright."""
        from playwright.sync_api import sync_playwright

        collected = {}   # pid -> {name, translit, price}

        with sync_playwright() as p:
            browser, page = self._create_browser(p)
            try:
                print(f"[{self.SOURCE_NAME}] Загружаю страницу каталога...")
                page.goto(self.CATALOG_URL, wait_until="domcontentloaded", timeout=60000)
                # Ждём JS-инициализации (mvid-token, куки и прочее)
                time.sleep(6)

                offset   = 0
                total    = None
                page_num = 0

                while True:
                    # ── Шаг 1: listing (productIds) ─────────────────────────
                    try:
                        search_data = page.evaluate(
                            """
                            async ([catId, off, lim]) => {
                                const url = `/bff/products/v2/search?categoryIds=${catId}&offset=${off}&limit=${lim}&doTranslit=true&context=`;
                                const resp = await fetch(url, {
                                    headers: {'Accept': 'application/json, text/plain, */*'}
                                });
                                return await resp.json();
                            }
                            """,
                            [category_id, offset, self.PAGE_LIMIT],
                        )
                    except Exception as e:
                        print(f"[{self.SOURCE_NAME}] search ошибка offset={offset}: {e}")
                        break

                    body = search_data.get("body", search_data)
                    pids = [
                        str(x)
                        for x in (
                            body.get("products")
                            or body.get("productIds")
                            or body.get("items")
                            or []
                        )
                    ]

                    if total is None:
                        for field in ("total", "totalItems", "totalCount",
                                      "total_count", "count", "size"):
                            if body.get(field) is not None:
                                total = int(body[field])
                                break
                        else:
                            total = 0
                        print(f"[{self.SOURCE_NAME}] Всего товаров: {total}")
                        self._last_total = total

                    if not pids:
                        print(f"[{self.SOURCE_NAME}] Пустой ответ listing на offset={offset}")
                        break

                    print(f"[{self.SOURCE_NAME}] offset={offset}: получено {len(pids)} ID")

                    # ── Шаг 2: details (названия) ────────────────────────────
                    try:
                        details_data = page.evaluate(
                            """
                            async (pids) => {
                                const resp = await fetch('/bff/product-details/list', {
                                    method: 'POST',
                                    headers: {
                                        'Content-Type': 'application/json',
                                        'Accept': 'application/json, text/plain, */*'
                                    },
                                    body: JSON.stringify({productIds: pids})
                                });
                                return await resp.json();
                            }
                            """,
                            pids,
                        )

                        for item in details_data.get("body", {}).get("products", []):
                            pid      = str(item.get("productId") or item.get("id") or "")
                            brand    = item.get("brandName") or ""
                            name     = item.get("name") or item.get("title") or ""
                            full     = f"{brand} {name}".strip() if brand else name
                            translit = item.get("nameTranslit") or ""
                            if pid and full:
                                collected.setdefault(pid, {})
                                collected[pid]["name"]     = full
                                collected[pid]["translit"] = translit

                    except Exception as e:
                        print(f"[{self.SOURCE_NAME}] details ошибка offset={offset}: {e}")

                    # ── Шаг 3: prices ────────────────────────────────────────
                    try:
                        prices_data = page.evaluate(
                            """
                            async (pids) => {
                                const url = `/bff/products/prices?productIds=${pids.join(',')}`;
                                const resp = await fetch(url, {
                                    headers: {'Accept': 'application/json, text/plain, */*'}
                                });
                                return await resp.json();
                            }
                            """,
                            pids,
                        )

                        for item in prices_data.get("body", {}).get("materialPrices", []):
                            pb  = item.get("price", item)
                            pid = str(pb.get("productId") or item.get("productId") or "")
                            val = pb.get("salePrice") or pb.get("basePrice")
                            if pid and val:
                                try:
                                    price = int(float(val))
                                    if 300 < price < 10_000_000:
                                        collected.setdefault(pid, {})["price"] = price
                                except (ValueError, TypeError):
                                    pass

                    except Exception as e:
                        print(f"[{self.SOURCE_NAME}] prices ошибка offset={offset}: {e}")

                    page_num += 1
                    offset   += self.PAGE_LIMIT
                    if (total > 0 and offset >= total) or page_num >= self.MAX_PAGES:
                        break
                    time.sleep(self.DELAY_BETWEEN_PAGES)

            finally:
                browser.close()

        # Собираем итоговый список
        products = []
        for pid, info in collected.items():
            name  = info.get("name")
            price = info.get("price")
            if not name or not price:
                continue
            translit = info.get("translit", "")
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

        return products

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
