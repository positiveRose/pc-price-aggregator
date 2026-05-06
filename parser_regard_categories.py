"""
Парсеры Регард для всех категорий комплектующих ПК.
URL формат: https://www.regard.ru/catalog/hits?q=BASE64({"byCategory":ID})
Все категории запускаются в одной браузерной сессии через run_all_categories()
чтобы не плодить несколько Chromium-инстансов одновременно (экономия памяти).
"""

import time

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from base_parser import _CHROMIUM_ARGS, PARSER_PROXY
from parser_regard import RegardParser

# Прямые URL каталога regard.ru (проверены — возвращают реальные товары)
REGARD_CATEGORIES = {
    "GPU":    "https://www.regard.ru/catalog/1013/videokarty",
    "CPU":    "https://www.regard.ru/catalog/1001/processory",
    "MB":     "https://www.regard.ru/catalog/1000/materinskie-platy",
    "RAM":    "https://www.regard.ru/catalog/1010/operativnaya-pamyat",
    "SSD":    "https://www.regard.ru/catalog/1015/ssd",
    "HDD":    "https://www.regard.ru/catalog/1014/zhestkie-diski",
    "PSU":    "https://www.regard.ru/catalog/1225/bloki-pitaniya",
    "CASE":   "https://www.regard.ru/catalog/1032/korpusa",
    # 1003 = хаб-страница категорий (Card_wrap=0, товаров нет)
    # 1008 = жидкостное охлаждение — реальный листинг с Card_wrap ✓
    "COOLER": "https://www.regard.ru/catalog/1008/zidkostnoe-oxlazdenie-szo",
}

_CARD_SELECTOR = RegardParser.CARD_SELECTOR
_WAIT_TIMEOUT  = 45000
_PAGE_DELAY    = 5   # секунд между страницами одной категории
_CAT_DELAY     = 15  # секунд между категориями


def _make_parser(category, url):
    class _Parser(RegardParser):
        SOURCE_NAME = "regard"
        CATALOG_URL = url
        WAIT_TIMEOUT = _WAIT_TIMEOUT
        DELAY_BETWEEN_PAGES = _PAGE_DELAY
        MAX_PAGES = 15
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

        def get_page_url(self, page_num):
            # hits URL содержит ?, обычный — нет
            sep = "&" if "?" in self.CATALOG_URL else "?"
            return f"{self.CATALOG_URL}{sep}page={page_num}"

    _Parser.__name__ = f"Regard{category}Parser"
    _Parser.__qualname__ = f"Regard{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"regard-{cat.lower()}": _make_parser(cat, url)
    for cat, url in REGARD_CATEGORIES.items()
}


def _load_page(page, url):
    """Загружает страницу Regard с обработкой WAF-редиректа."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=120000)
    except Exception as e:
        print(f"[regard] goto timeout/error на {url}: {e}")
        time.sleep(5)

    try:
        page.wait_for_selector(_CARD_SELECTOR, timeout=_WAIT_TIMEOUT)
        time.sleep(2)
    except Exception:
        time.sleep(10)
        try:
            page.wait_for_selector(_CARD_SELECTOR, timeout=30000)
            time.sleep(2)
        except Exception:
            preview = page.content()[:300].replace("\n", " ")
            print(f"[regard] Карточки не появились на {url}")
            print(f"[regard] HTML preview: {preview}")

    # Скроллим для подгрузки lazy-loading карточек
    for _ in range(10):
        page.evaluate("window.scrollBy(0, 800)")
        time.sleep(0.4)
    time.sleep(1)

    return page.content()


def run_all_categories(keys=None):
    """Запускает категории Регард в одной браузерной сессии.

    Один Chromium-инстанс на все категории — не плодим по браузеру на
    каждую из 9 категорий с 15 страницами каждая.

    Args:
        keys: список ключей из CATEGORY_PARSERS; None = все категории.

    Returns:
        dict {key: [products]}
    """
    if keys is None:
        keys = list(CATEGORY_PARSERS.keys())

    results = {k: [] for k in keys}

    print(f"[regard] Запуск run_all_categories, категорий: {len(keys)}", flush=True)
    with sync_playwright() as p:
        print("[regard] Запускаю Chromium...", flush=True)
        browser = p.chromium.launch(headless=True, args=_CHROMIUM_ARGS)
        print("[regard] Chromium запущен.", flush=True)

        ctx_opts = {
            "viewport": {"width": 1920, "height": 1080},
            "locale": "ru-RU",
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }
        if PARSER_PROXY:
            ctx_opts["proxy"] = {"server": PARSER_PROXY}

        context = browser.new_context(**ctx_opts)
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        for key in keys:
            parser_cls = CATEGORY_PARSERS.get(key)
            if not parser_cls:
                continue
            parser = parser_cls()
            all_products = []

            try:
                html = _load_page(page, parser_cls.CATALOG_URL)
                all_products.extend(parser.parse_products(html))

                total_pages = min(parser.get_total_pages(html), parser.MAX_PAGES)
                print(f"[regard] [{parser_cls._CATEGORY}] Страниц: {total_pages}")

                for page_num in range(2, total_pages + 1):
                    time.sleep(_PAGE_DELAY)
                    page_url = parser.get_page_url(page_num)
                    print(f"[regard] [{parser_cls._CATEGORY}] Стр. {page_num}: {page_url}")
                    try:
                        html = _load_page(page, page_url)
                        all_products.extend(parser.parse_products(html))
                    except Exception as e:
                        print(f"[regard] Ошибка на стр. {page_num}: {e}")

            except Exception as e:
                print(f"[{key}] ОШИБКА: {e}")

            print(f"[regard] [{parser_cls._CATEGORY}] Найдено товаров: {len(all_products)}")
            results[key] = all_products
            time.sleep(_CAT_DELAY)

        try:
            browser.close()
        except Exception:
            pass

    return results
