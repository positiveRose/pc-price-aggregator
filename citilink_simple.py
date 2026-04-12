"""
Итерация 1: Простой парсер Ситилинк — один файл, минимум кода.
Скачивает страницу каталога видеокарт и выводит список товаров с ценами.

Что тут происходит:
1. Playwright открывает настоящий браузер (Chromium)
2. Браузер загружает страницу и выполняет JS (проходит антибот-проверку)
3. BeautifulSoup разбирает HTML и достаёт данные из карточек товаров
4. Результат печатается в консоль

Запуск:
    python citilink_simple.py                    — все видеокарты
    python citilink_simple.py "RTX 5070"         — поиск по названию
    python citilink_simple.py "RX 9060"          — поиск по названию
"""

import re
import sys
import time

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


# ==================== НАСТРОЙКИ ====================

# URL каталога видеокарт Ситилинк
CATALOG_URL = "https://www.citilink.ru/catalog/videokarty/"

# Сколько ждать после загрузки страницы (секунды)
# Нужно чтобы JS успел отработать и показать товары
WAIT_AFTER_LOAD = 10

# Базовый URL для построения полных ссылок
BASE_URL = "https://www.citilink.ru"


# ==================== ФУНКЦИИ ====================

def get_page_html(url):
    """
    Открывает страницу в браузере и возвращает HTML после загрузки.

    Почему Playwright, а не requests?
    - Ситилинк использует JS-защиту (показывает "Загрузка..." и проверяет браузер)
    - requests не умеет выполнять JS — получает только страницу-заглушку
    - Playwright управляет настоящим Chromium — как будто ты сам открыл сайт
    """
    with sync_playwright() as p:
        # headless=True — браузер работает невидимо (без окна)
        # Поменяй на headless=False чтобы увидеть что происходит
        browser = p.chromium.launch(headless=True)
        try:
            # Создаём контекст с русской локалью и нормальным размером окна
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                locale="ru-RU",
            )
            page = context.new_page()

            print(f"Загружаю: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Ждём появления карточек товаров (вместо слепого sleep)
            # Если за 15 сек не появятся — fallback на обычное ожидание
            try:
                page.wait_for_selector(
                    '[data-meta-name="ProductVerticalSnippet"]',
                    timeout=15000,
                )
                # Небольшая пауза чтобы все карточки успели подгрузиться
                time.sleep(2)
            except Exception:
                print("Карточки не появились за 15 сек, жду ещё...")
                time.sleep(WAIT_AFTER_LOAD)

            html = page.content()
        finally:
            # Браузер закроется даже если произошла ошибка
            browser.close()

    return html


def parse_products(html):
    """
    Разбирает HTML страницы и достаёт данные товаров.

    Как найти нужные селекторы:
    1. Открой страницу в Chrome
    2. Правый клик на товаре → "Просмотреть код"
    3. Ищи уникальные атрибуты (data-meta-name, классы)

    В Ситилинке каждая карточка — это div с data-meta-name="ProductVerticalSnippet"
    """
    soup = BeautifulSoup(html, "lxml")

    # Находим все карточки товаров
    cards = soup.select('div[data-meta-name="ProductVerticalSnippet"]')

    products = []

    for card in cards:
        try:
            product = parse_one_card(card)
            if product:
                products.append(product)
        except Exception as e:
            # Если одна карточка сломалась — пропускаем, парсим остальные
            print(f"  Ошибка при парсинге карточки: {e}")
            continue

    return products


def parse_one_card(card):
    """
    Извлекает данные из одной карточки товара.
    Возвращает словарь с данными или None если карточка пустая.
    """
    # --- Название и ссылка ---
    # Ситилинк хранит полное название в атрибуте title у ссылки
    link = card.select_one("a[title]")
    if not link:
        return None

    name = link["title"].strip()
    href = link["href"]
    url = href if href.startswith("http") else BASE_URL + href

    # --- Цена ---
    # Цена хранится в data-атрибуте data-meta-price (число без пробелов)
    price_el = card.select_one("[data-meta-price]")
    if not price_el:
        return None

    try:
        price = int(float(price_el["data-meta-price"]))
    except (ValueError, TypeError):
        return None

    # --- ID товара ---
    # Берём из атрибута data-meta-product-id на карточке (надёжнее чем парсить URL)
    product_id = card.get("data-meta-product-id", "")
    if not product_id:
        # Fallback: извлекаем из URL (/product/...-1973797/)
        parts = href.rstrip("/").split("-")
        product_id = parts[-1] if parts[-1].isdigit() else "unknown"

    # --- Наличие ---
    # Ситилинк по умолчанию показывает в каталоге ТОЛЬКО товары в наличии.
    # Если товар появился на странице каталога — он в наличии.
    in_stock = True

    return {
        "id": product_id,
        "name": name,
        "price": price,
        "url": url,
        "in_stock": in_stock,
    }


def print_results(products):
    """Красиво печатает результаты в консоль."""
    print(f"\n{'='*70}")
    print(f"Найдено товаров: {len(products)}")
    print(f"{'='*70}\n")

    for i, p in enumerate(products, 1):
        stock = "В наличии" if p["in_stock"] else "Нет в наличии"
        print(f"{i:2}. {p['name'][:65]}")
        print(f"    Цена: {p['price']:,} руб. | {stock}")
        print(f"    {p['url']}")
        print()

    # Статистика
    all_prices = [p["price"] for p in products]
    in_stock_count = sum(1 for p in products if p["in_stock"])
    if all_prices:
        print(f"{'='*70}")
        print(f"Мин. цена: {min(all_prices):,} руб.")
        print(f"Макс. цена: {max(all_prices):,} руб.")
        print(f"Средняя: {sum(all_prices) // len(all_prices):,} руб.")
        print(f"В наличии: {in_stock_count} из {len(products)}")


def filter_products(products, query):
    """
    Фильтрует товары по поисковому запросу.
    Каждое слово запроса ищется как начало слова в названии.

    "RTX 5070"  → найдёт "RTX 5070 GV-..." и "RTX 5070TI"
    "RTX 5070TI" → найдёт только "RTX 5070TI", не "RTX 5070"
    "RX 9060"   → найдёт "RX 9060XT"
    """
    query_words = query.lower().split()
    results = []
    for p in products:
        name_words = re.findall(r"[a-zа-яё0-9]+", p["name"].lower())
        # Каждое слово запроса должно быть началом какого-то слова в названии
        if all(
            any(nw.startswith(qw) for nw in name_words)
            for qw in query_words
        ):
            results.append(p)
    return results


# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    # Берём поисковый запрос из аргументов командной строки
    # python citilink_simple.py "RTX 5070"
    search_query = sys.argv[1] if len(sys.argv) > 1 else None

    print("Парсер Ситилинк — Итерация 1")
    print("-" * 40)

    # 1. Скачиваем страницу через браузер
    html = get_page_html(CATALOG_URL)

    # 2. Парсим карточки товаров
    products = parse_products(html)

    # 3. Фильтруем по запросу (если указан)
    if search_query:
        print(f"Ищу: \"{search_query}\"")
        products = filter_products(products, search_query)

    # 4. Выводим результат
    if products:
        print_results(products)
    else:
        if search_query:
            print(f"Ничего не найдено по запросу \"{search_query}\".")
            print("Попробуй другие слова или запусти без аргументов для полного списка.")
        else:
            print("Товары не найдены. Возможно:")
            print("- Сайт заблокировал запрос")
            print("- Изменилась структура HTML")
            print("- Страница не успела загрузиться (увеличь WAIT_AFTER_LOAD)")
