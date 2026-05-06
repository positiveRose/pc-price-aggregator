"""
Парсеры Ситилинк для всех категорий комплектующих ПК.
Используют единую фабричную функцию — никакого дублирования кода.
Все категории запускаются в одной браузерной сессии через run_all_categories()
чтобы не плодить несколько Chromium-инстансов одновременно (экономия памяти).
"""

import time

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from base_parser import _CHROMIUM_ARGS, PARSER_PROXY
from parser_citilink import CitilinkParser

# Категория → URL каталога на Ситилинк
CITILINK_CATEGORIES = {
    "GPU":    "https://www.citilink.ru/catalog/videokarty/",
    "CPU":    "https://www.citilink.ru/catalog/processory/",
    "MB":     "https://www.citilink.ru/catalog/materinskie-platy/",
    "RAM":    "https://www.citilink.ru/catalog/moduli-pamyati/",
    "SSD":    "https://www.citilink.ru/catalog/ssd-nakopiteli/",
    "HDD":    "https://www.citilink.ru/catalog/zhestkie-diski/",
    "PSU":    "https://www.citilink.ru/catalog/bloki-pitaniya/",
    "CASE":   "https://www.citilink.ru/catalog/korpusa/",
    "COOLER": "https://www.citilink.ru/catalog/sistemy-ohlazhdeniya-processora/",
}

_CARD_SELECTOR = CitilinkParser.CARD_SELECTOR
_WAIT_TIMEOUT  = 45000
_PAGE_DELAY    = 5   # секунд между страницами одной категории
_CAT_DELAY     = 15  # секунд между категориями


def _make_parser(category, url):
    """Создаёт класс парсера для конкретной категории Ситилинк."""
    class _Parser(CitilinkParser):
        SOURCE_NAME = "citilink"
        CATALOG_URL = url
        WAIT_TIMEOUT = _WAIT_TIMEOUT
        DELAY_BETWEEN_PAGES = _PAGE_DELAY
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Citilink{category}Parser"
    _Parser.__qualname__ = f"Citilink{category}Parser"
    return _Parser


# Словарь: ключ для CLI → класс парсера
CATEGORY_PARSERS = {
    f"citilink-{cat.lower()}": _make_parser(cat, url)
    for cat, url in CITILINK_CATEGORIES.items()
}


def _load_page(page, url):
    """Загружает страницу Citilink с обработкой WAF-редиректа."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=120000)
    except Exception as e:
        print(f"[citilink] goto timeout/error на {url}: {e}")
        time.sleep(5)

    try:
        page.wait_for_selector(_CARD_SELECTOR, timeout=_WAIT_TIMEOUT)
        time.sleep(2)
    except Exception:
        # WAF может не успеть сделать редирект — ждём ещё
        time.sleep(10)
        try:
            page.wait_for_selector(_CARD_SELECTOR, timeout=30000)
            time.sleep(2)
        except Exception:
            preview = page.content()[:300].replace("\n", " ")
            print(f"[citilink] Карточки не появились на {url}")
            print(f"[citilink] HTML preview: {preview}")

    # Скроллим для подгрузки lazy-loading карточек
    for _ in range(10):
        page.evaluate("window.scrollBy(0, 800)")
        time.sleep(0.4)
    time.sleep(1)

    return page.content()


def run_all_categories(keys=None):
    """Запускает категории Ситилинк в одной браузерной сессии.

    Один Chromium-инстанс на все категории — в отличие от запуска каждого
    парсера отдельно, не плодим по браузеру на каждую из 9 категорий.

    Args:
        keys: список ключей из CATEGORY_PARSERS; None = все категории.

    Returns:
        dict {key: [products]}
    """
    if keys is None:
        keys = list(CATEGORY_PARSERS.keys())

    results = {k: [] for k in keys}

    print(f"[citilink] Запуск run_all_categories, категорий: {len(keys)}", flush=True)
    with sync_playwright() as p:
        print("[citilink] Запускаю Chromium...", flush=True)
        browser = p.chromium.launch(headless=True, args=_CHROMIUM_ARGS)
        print("[citilink] Chromium запущен.", flush=True)

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
                print(f"[citilink] [{parser_cls._CATEGORY}] Страниц: {total_pages}")

                for page_num in range(2, total_pages + 1):
                    time.sleep(_PAGE_DELAY)
                    page_url = parser.get_page_url(page_num)
                    print(f"[citilink] [{parser_cls._CATEGORY}] Стр. {page_num}: {page_url}")
                    try:
                        html = _load_page(page, page_url)
                        all_products.extend(parser.parse_products(html))
                    except Exception as e:
                        print(f"[citilink] Ошибка на стр. {page_num}: {e}")

            except Exception as e:
                print(f"[{key}] ОШИБКА: {e}")

            print(f"[citilink] [{parser_cls._CATEGORY}] Найдено товаров: {len(all_products)}")
            results[key] = all_products
            time.sleep(_CAT_DELAY)

        try:
            browser.close()
        except Exception:
            pass

    return results
