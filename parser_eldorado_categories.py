"""
Парсеры Эльдорадо для всех категорий комплектующих ПК.
URL формат: https://www.eldorado.ru/c/{category-slug}/

Примечание: Эльдорадо использует Group-IB защиту (JS-challenge).
Для корректной работы все категории запускаются в одной браузерной
сессии через run_all_categories() — браузер проходит challenge один
раз, затем парсит все категории подряд.
"""

import time

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from base_parser import _CHROMIUM_ARGS, PARSER_PROXY
from parser_eldorado import EldoradoParser


def _goto_and_get_content(page, url, delay):
    """Переходит на страницу и дожидается окончания навигации перед page.content()."""
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
    except Exception:
        # Если networkidle не дождались — ждём хотя бы domcontentloaded
        try:
            page.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            pass
    time.sleep(delay)
    return page.content()

ELDORADO_CATEGORIES = {
    "GPU":    "https://www.eldorado.ru/c/videokarty/",
    "CPU":    "https://www.eldorado.ru/c/protsessory/",
    "MB":     "https://www.eldorado.ru/c/materinskie-platy/",
    "RAM":    "https://www.eldorado.ru/c/operativnaya-pamyat/",
    "SSD":    "https://www.eldorado.ru/c/tverdotelnye-nakopiteli-ssd/",
    "HDD":    "https://www.eldorado.ru/c/zhestkie-diski/",
    "PSU":    "https://www.eldorado.ru/c/bloki-pitaniya/",
    "CASE":   "https://www.eldorado.ru/c/korpusa/",
    "COOLER": "https://www.eldorado.ru/c/sistemy-okhlazhdeniya/",
}


def _make_parser(category, url):
    class _Parser(EldoradoParser):
        SOURCE_NAME = "eldorado"
        CATALOG_URL = url
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Eldorado{category}Parser"
    _Parser.__qualname__ = f"Eldorado{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"eldorado-{cat.lower()}": _make_parser(cat, url)
    for cat, url in ELDORADO_CATEGORIES.items()
}


def run_all_categories(keys=None):
    """Запускает категории Эльдорадо в одной браузерной сессии.

    Браузер загружает главную страницу, проходит Group-IB JS-challenge,
    затем последовательно парсит все запрошенные категории.

    Args:
        keys: список ключей из CATEGORY_PARSERS; None = все категории.

    Returns:
        dict {key: [products]}
    """
    if keys is None:
        keys = list(CATEGORY_PARSERS.keys())

    results = {k: [] for k in keys}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=_CHROMIUM_ARGS)
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

        # Прогрев: загружаем главную, ждём пока Group-IB выдаст cookie.
        # headless=False нужен чтобы пройти Group-IB JS-challenge.
        print("[eldorado] Прогрев сессии (Group-IB challenge)...")
        try:
            page.goto("https://www.eldorado.ru/", wait_until="domcontentloaded", timeout=60000)
        except Exception:
            pass
        # Ждём появления __NEXT_DATA__ (признак что challenge пройден и страница загружена)
        try:
            page.wait_for_function(
                "!!document.getElementById('__NEXT_DATA__')",
                timeout=60000,
            )
        except Exception:
            pass
        print("[eldorado] Прогрев завершён.")

        for key in keys:
            parser_cls = CATEGORY_PARSERS.get(key)
            if not parser_cls:
                continue
            parser = parser_cls()
            url = parser_cls.CATALOG_URL
            print(f"[eldorado] Загружаю: {url}")
            try:
                html = _goto_and_get_content(page, url, parser_cls.DELAY_BETWEEN_PAGES)
                all_products = parser.parse_products(html)

                total_pages = min(parser.get_total_pages(html), parser.MAX_PAGES)
                print(f"[eldorado] Страниц: {total_pages}")

                for page_num in range(2, total_pages + 1):
                    page_url = parser.get_page_url(page_num)
                    print(f"[eldorado] Загружаю стр. {page_num}: {page_url}")
                    try:
                        html = _goto_and_get_content(page, page_url, parser_cls.DELAY_BETWEEN_PAGES)
                        products = parser.parse_products(html)
                        all_products.extend(products)
                    except Exception as e:
                        print(f"[eldorado] Ошибка на стр. {page_num}: {e}")

                print(f"[eldorado] Найдено товаров: {len(all_products)}")
                results[key] = all_products
            except Exception as e:
                print(f"[{key}] ОШИБКА: {e}")

        try:
            browser.close()
        except Exception:
            pass

    return results
