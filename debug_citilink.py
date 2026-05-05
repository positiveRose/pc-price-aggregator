"""
Отладка парсера Ситилинк — полный прогон по всем страницам.
Запуск: python debug_citilink.py
"""
import time
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
from parser_citilink import CitilinkParser

CATEGORY = "GPU"
URL = "https://www.citilink.ru/catalog/videokarty/"
CARD_SELECTOR = '[data-meta-name="ProductVerticalSnippet"]'
WAIT_TIMEOUT = 45000
DELAY_BETWEEN_PAGES = 5

print("=" * 60)
print(f"Ситилинк дебаг — категория {CATEGORY}")
print(f"URL: {URL}")
print("=" * 60)

parser = CitilinkParser()
all_products = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        locale="ru-RU",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    )
    page = context.new_page()
    Stealth().apply_stealth_sync(page)

    # ── Страница 1 ──────────────────────────────────────────────────
    print(f"\n[Страница 1] Захожу: {URL}")
    try:
        page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    except Exception as e:
        print(f"  goto() ошибка: {e}")

    print(f"  Жду карточки (timeout={WAIT_TIMEOUT}ms)...")
    try:
        page.wait_for_selector(CARD_SELECTOR, timeout=WAIT_TIMEOUT)
        print("  -> Карточки НАЙДЕНЫ")
    except Exception:
        print("  -> Карточки не появились, жду ещё 10 сек (WAF редирект?)")
        time.sleep(10)
        try:
            page.wait_for_selector(CARD_SELECTOR, timeout=30000)
            print("  -> Карточки НАЙДЕНЫ после ожидания")
        except Exception:
            print("  -> КАРТОЧКИ НЕ НАЙДЕНЫ")

    time.sleep(2)
    html1 = page.content()

    # Определяем кол-во страниц
    total_pages = parser.get_total_pages(html1)
    print(f"  Всего страниц: {total_pages}")

    # Парсим страницу 1
    products1 = parser.parse_products(html1)
    print(f"  Товаров на странице 1: {len(products1)}")
    all_products.extend(products1)

    # ── Остальные страницы ──────────────────────────────────────────
    for page_num in range(2, total_pages + 1):
        url = f"{URL}?p={page_num}"
        print(f"\n[Страница {page_num}/{total_pages}] Жду {DELAY_BETWEEN_PAGES} сек...")
        time.sleep(DELAY_BETWEEN_PAGES)

        print(f"  Захожу: {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
        except Exception as e:
            print(f"  goto() ошибка: {e}")

        print(f"  Жду карточки (timeout={WAIT_TIMEOUT}ms)...")
        try:
            page.wait_for_selector(CARD_SELECTOR, timeout=WAIT_TIMEOUT)
            print("  -> Карточки НАЙДЕНЫ")
        except Exception:
            print("  -> Карточки не появились, жду ещё 10 сек...")
            time.sleep(10)
            try:
                page.wait_for_selector(CARD_SELECTOR, timeout=30000)
                print("  -> Карточки НАЙДЕНЫ после ожидания")
            except Exception:
                print("  -> КАРТОЧКИ НЕ НАЙДЕНЫ — пропускаю страницу")
                continue

        time.sleep(2)
        html = page.content()
        products = parser.parse_products(html)
        print(f"  Товаров на странице {page_num}: {len(products)}")
        all_products.extend(products)

    browser.close()

# ── Итог ────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print(f"ИТОГО товаров: {len(all_products)}")
print("=" * 60)

if all_products:
    print("\nПервые 5 товаров:")
    print("-" * 60)
    for prod in all_products[:5]:
        print(f"  ID:   {prod['id']}")
        print(f"  Имя:  {prod['name']}")
        print(f"  Цена: {prod['price']:,} руб.")
        print(f"  URL:  {prod['url']}")
        print()
else:
    print("\n[!] Товаров 0 — что-то пошло не так")
