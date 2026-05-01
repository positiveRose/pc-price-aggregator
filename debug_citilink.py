"""
Отладка парсера Ситилинк.
Запуск: python debug_citilink.py
"""
import time
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
from parser_citilink import CitilinkParser

URL = "https://www.citilink.ru/catalog/videokarty/"
CARD_SELECTOR = '[data-meta-name="ProductVerticalSnippet"]'

print("=" * 60)
print("ШАГ 1: Открываю браузер (видимый режим)...")
print("=" * 60)

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

    print(f"\nШАГ 2: Захожу на {URL}")
    page.goto(URL, wait_until="domcontentloaded", timeout=60000)

    print(f"\nШАГ 3: Жду карточки товаров...")
    try:
        page.wait_for_selector(CARD_SELECTOR, timeout=15000)
        print("  -> Карточки НАЙДЕНЫ на странице!")
    except Exception:
        print("  -> КАРТОЧКИ НЕ НАЙДЕНЫ! Возможно антибот или изменилась разметка.")

    time.sleep(2)
    html = page.content()

    print("\nШАГ 4: Пауза 5 сек — посмотри на открытый браузер...")
    time.sleep(5)
    browser.close()

print("\n" + "=" * 60)
print("ШАГ 5: Парсю HTML через BeautifulSoup...")
print("=" * 60)

soup = BeautifulSoup(html, "lxml")
cards = soup.select(CARD_SELECTOR)
print(f"  Найдено карточек в HTML: {len(cards)}")

print("\nШАГ 6: Разбираю каждую карточку...")
parser = CitilinkParser()
products = parser.parse_products(html)

print(f"\n  Итого товаров после разбора: {len(products)}")

if products:
    print("\nПервые 5 товаров:")
    print("-" * 60)
    for p in products[:5]:
        print(f"  ID:    {p['id']}")
        print(f"  Имя:   {p['name']}")
        print(f"  Цена:  {p['price']:,} руб.")
        print(f"  URL:   {p['url']}")
        print()
else:
    print("\n  [!] Товаров 0 — разбираем почему:")
    if not cards:
        print("  Причина: карточки не нашлись в HTML.")
        print("  Открой citilink_page.html и найди Ctrl+F: 'ProductVerticalSnippet'")
    else:
        print("  Карточки нашлись, но данные не извлеклись. Смотрим первую карточку:")
        card = cards[0]
        link = card.select_one("a[title]")
        price_el = card.select_one("[data-meta-price]")
        print(f"  a[title]          = {link}")
        print(f"  [data-meta-price] = {price_el}")

print("\nШАГ 7: Сохраняю HTML страницы в citilink_page.html...")
with open("citilink_page.html", "w", encoding="utf-8") as f:
    f.write(html)
print("  -> Файл сохранён. Открой его в браузере, Ctrl+F -> 'data-meta-price'")

print("\nГотово!")