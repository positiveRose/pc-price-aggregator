"""
Итерация 5: Агрегатор цен — Ситилинк + Регард + DNS + OLDI + e2e4 + МВидео + Эльдорадо → SQLite → веб-интерфейс.

Запуск:
    python main.py                     — парсить все магазины
    python main.py citilink            — только Ситилинк (GPU)
    python main.py regard              — только Регард (GPU)
    python main.py oldi                — все категории OLDI
    python main.py e2e4                — все категории e2e4
    python main.py mvideo              — все категории МВидео
    python main.py eldorado            — все категории Эльдорадо
    python main.py citilink-all        — все категории Ситилинк
    python main.py regard-all          — все категории Регард
    python main.py --show              — показать сохранённые данные без парсинга
    python main.py "RTX 5070"          — парсить всё и искать по запросу
    python main.py --pages 3           — парсить первые 3 страницы (по умолчанию: все)
    python main.py --match             — запустить матчинг товаров
    python main.py --web               — запустить веб-сервер
"""

import re
import sys

import database as db
from parser_citilink import CitilinkParser
from parser_citilink_categories import CATEGORY_PARSERS as CITILINK_PARSERS
from parser_dns import DnsParser
from parser_e2e4_categories import CATEGORY_PARSERS as E2E4_PARSERS
from parser_eldorado_categories import CATEGORY_PARSERS as ELDORADO_PARSERS
from parser_mvideo_categories import CATEGORY_PARSERS as MVIDEO_PARSERS
from parser_oldi_categories import CATEGORY_PARSERS as OLDI_PARSERS
from parser_regard import RegardParser
from parser_regard_categories import CATEGORY_PARSERS as REGARD_PARSERS


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
# citilink          — только GPU (обратная совместимость)
# citilink-gpu/cpu/mb/ram/ssd/hdd/psu/case/cooler — конкретная категория
# citilink-all      — все категории Ситилинк сразу
# oldi-all / e2e4-all / mvideo-all / eldorado-all / key-all — аналогично
PARSERS = {
    "citilink": CitilinkParser,
    "regard": RegardParser,
    "dns": DnsParser,
    **CITILINK_PARSERS,
    **REGARD_PARSERS,
    **OLDI_PARSERS,
    **E2E4_PARSERS,
    **MVIDEO_PARSERS,
    **ELDORADO_PARSERS,
}

# Алиасы для запуска всех категорий конкретного магазина
_ALL_ALIASES = {
    "citilink-all": list(CITILINK_PARSERS.keys()),
    "regard-all":   list(REGARD_PARSERS.keys()),
    "oldi-all":     list(OLDI_PARSERS.keys()),
    "e2e4-all":     list(E2E4_PARSERS.keys()),
    "mvideo-all":   list(MVIDEO_PARSERS.keys()),
    "eldorado-all": list(ELDORADO_PARSERS.keys()),
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

            # Сохраняем в базу — используем SOURCE_NAME парсера, не ключ словаря
            source = getattr(parser_cls, "SOURCE_NAME", None) or name
            saved, updated = db.save_products(products, source)
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
            marker = " *" if has_comparison and o["price"] == best["price"] else ""
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
    print(f"* = лучшая цена")


def main():
    args = sys.argv[1:]

    # Инициализируем БД при любом режиме
    db.init_db()

    # Режим матчинга
    if "--match" in args:
        from matcher import run_matching
        run_matching()
        return

    # Режим веб-сервера
    if "--web" in args:
        import uvicorn
        uvicorn.run("web_app:app", host="localhost", port=8000, reload=True)
        return

    # Режим просмотра без парсинга
    if "--show" in args:
        # Убираем все флаги, оставляем только поисковый запрос
        skip_next = False
        query_parts = []
        for a in args:
            if skip_next:
                skip_next = False
                continue
            if a in ("--show", "--pages", "--match", "--web"):
                if a == "--pages":
                    skip_next = True
                continue
            query_parts.append(a)
        query = " ".join(query_parts) if query_parts else None
        print_comparison(query)
        return

    # Определяем какие парсеры запускать, лимит страниц, поисковый запрос
    sources = []
    query_parts = []
    max_pages = None  # None = все страницы

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--pages" and i + 1 < len(args) and args[i + 1].isdigit():
            max_pages = int(args[i + 1])
            i += 2
            continue
        elif (arg.lower() in PARSERS
              or arg.lower() in _ALL_ALIASES
              or arg.lower() in ("oldi", "e2e4", "mvideo", "eldorado")):
            sources.append(arg.lower())
        else:
            query_parts.append(arg)
        i += 1

    # -all алиасы → разворачиваем в список всех категорий магазина
    for alias, keys in list(_ALL_ALIASES.items()):
        if alias in sources:
            sources.remove(alias)
            sources.extend(keys)

    # Короткие имена магазинов → все их категории
    _SHOP_TO_PARSERS = {
        "oldi":     list(OLDI_PARSERS.keys()),
        "e2e4":     list(E2E4_PARSERS.keys()),
        "mvideo":   list(MVIDEO_PARSERS.keys()),
        "eldorado": list(ELDORADO_PARSERS.keys()),
    }
    for shop, keys in _SHOP_TO_PARSERS.items():
        if shop in sources:
            sources.remove(shop)
            sources.extend(keys)

    sources = sources or None  # None = все парсеры
    search_query = " ".join(query_parts) if query_parts else None

    # Запускаем парсеры
    print("Агрегатор цен — Итерация 5")
    print("-" * 40)
    all_products = run_parsers(sources, max_pages=max_pages)

    # Выводим сравнение
    print_comparison(search_query)


if __name__ == "__main__":
    main()
