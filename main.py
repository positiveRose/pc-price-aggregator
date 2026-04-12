"""
Итерация 2: Агрегатор цен — Ситилинк + Регард → SQLite → сравнение.

Запуск:
    python main.py                     — парсить все магазины
    python main.py citilink            — только Ситилинк
    python main.py regard              — только Регард
    python main.py --show              — показать сохранённые данные без парсинга
    python main.py "RTX 5070"          — парсить всё и искать по запросу
    python main.py --pages 3           — парсить первые 3 страницы (по умолчанию: все)
"""

import re
import sys

import database as db
from parser_citilink import CitilinkParser
from parser_regard import RegardParser


def filter_by_query(items, query):
    """
    Фильтрует список по поисковому запросу (начало слова).
    "RTX 5070" найдёт "RTX 5070 Ti", но не "RTX 50700".
    """
    query_words = query.lower().split()
    results = []
    for item in items:
        name_words = re.findall(r"[a-zа-яё0-9]+", item["name"].lower())
        if all(
            any(nw.startswith(qw) for nw in name_words)
            for qw in query_words
        ):
            results.append(item)
    return results


# Все доступные парсеры
PARSERS = {
    "citilink": CitilinkParser,
    "regard": RegardParser,
}


def run_parsers(sources=None, max_pages=None):
    """Запускает парсеры и сохраняет результаты в БД."""
    if sources is None:
        sources = list(PARSERS.keys())

    all_products = {}

    for name in sources:
        parser_cls = PARSERS.get(name)
        if not parser_cls:
            print(f"Неизвестный источник: {name}")
            continue

        parser = parser_cls()
        if max_pages is not None:
            parser.MAX_PAGES = max_pages
        try:
            products = parser.run()
            all_products[name] = products

            # Сохраняем в базу
            saved, updated = db.save_products(products, name)
            print(f"[{name}] Сохранено: {saved} новых, {updated} обновлено")
        except Exception as e:
            print(f"[{name}] ОШИБКА: {e}")

    return all_products


def print_comparison(search_query=None):
    """Выводит сравнение цен из базы данных."""
    offers = db.get_all_offers()

    if search_query:
        offers = filter_by_query(offers, search_query)

    if not offers:
        print("Нет данных для сравнения.")
        return

    # Группируем по названию
    grouped = {}
    for o in offers:
        name = o["name"]
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(o)

    print(f"\n{'='*70}")
    print(f"СРАВНЕНИЕ ЦЕН — {len(grouped)} товаров")
    print(f"{'='*70}\n")

    for i, (name, shop_offers) in enumerate(grouped.items(), 1):
        # Сортируем по цене
        shop_offers.sort(key=lambda x: x["price"])
        best = shop_offers[0]

        print(f"{i:2}. {name[:65]}")
        has_comparison = len(shop_offers) > 1
        for o in shop_offers:
            marker = " ★" if has_comparison and o["price"] == best["price"] else ""
            source_label = o["source"].upper().ljust(9)
            print(f"    {source_label} {o['price']:>10,} руб.{marker}")
        print()

    # Общая статистика
    all_prices = [o["price"] for o in offers]
    sources_count = len(set(o["source"] for o in offers))
    print(f"{'='*70}")
    print(f"Источников: {sources_count}")
    print(f"Предложений: {len(offers)}")
    print(f"Мин. цена: {min(all_prices):,} руб.")
    print(f"Макс. цена: {max(all_prices):,} руб.")
    print(f"★ = лучшая цена")


def main():
    args = sys.argv[1:]

    # Режим просмотра без парсинга
    if "--show" in args:
        # Убираем все флаги, оставляем только поисковый запрос
        skip_next = False
        query_parts = []
        for a in args:
            if skip_next:
                skip_next = False
                continue
            if a in ("--show", "--pages"):
                if a == "--pages":
                    skip_next = True
                continue
            query_parts.append(a)
        query = " ".join(query_parts) if query_parts else None
        print_comparison(query)
        return

    # Определяем какие парсеры запускать, лимит страниц, поисковый запрос
    sources = []
    search_query = None
    max_pages = None  # None = все страницы

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--pages" and i + 1 < len(args) and args[i + 1].isdigit():
            max_pages = int(args[i + 1])
            i += 2
            continue
        elif arg.lower() in PARSERS:
            sources.append(arg.lower())
        else:
            search_query = arg
        i += 1

    sources = sources or None  # None = все парсеры

    # Запускаем парсеры
    print("Агрегатор цен — Итерация 2")
    print("-" * 40)
    all_products = run_parsers(sources, max_pages=max_pages)

    # Выводим сравнение
    print_comparison(search_query)


if __name__ == "__main__":
    main()
