"""
Агрегатор цен — 8 магазинов → SQLite → веб-интерфейс.

Магазины: Ситилинк, Регард, OLDI, МВидео, Эльдорадо, WB, KNS, FCenter

Запуск:
    python main.py                     — парсить все магазины
    python main.py citilink            — только Ситилинк (GPU)
    python main.py regard              — только Регард (GPU)
    python main.py oldi                — все категории OLDI
    python main.py mvideo              — все категории МВидео
    python main.py eldorado            — все категории Эльдорадо
    python main.py wb                  — все категории Wildberries
    python main.py kns                 — все категории KNS
    python main.py fcenter             — все категории FCenter
    python main.py citilink-all        — все категории Ситилинк
    python main.py regard-all          — все категории Регард
    python main.py --show              — показать сохранённые данные без парсинга
    python main.py "RTX 5070"          — парсить всё и искать по запросу
    python main.py --pages 3           — парсить первые 3 страницы (по умолчанию: все)
    python main.py --match             — запустить матчинг товаров
    python main.py --web               — запустить веб-сервер
"""

import sys
import time

import database as db
from database import _name_tokens, _query_word_matches
from parser_citilink import CitilinkParser
from parser_citilink_categories import CATEGORY_PARSERS as CITILINK_PARSERS
from parser_eldorado_categories import CATEGORY_PARSERS as ELDORADO_PARSERS
from parser_mvideo_categories import CATEGORY_PARSERS as MVIDEO_PARSERS
from parser_oldi_categories import CATEGORY_PARSERS as OLDI_PARSERS
from parser_regard import RegardParser
from parser_regard_categories import CATEGORY_PARSERS as REGARD_PARSERS
from parser_wb_categories import CATEGORY_PARSERS as WB_PARSERS
from parser_kns_categories import CATEGORY_PARSERS as KNS_PARSERS
from parser_fcenter_categories import CATEGORY_PARSERS as FCENTER_PARSERS


def filter_by_query(items, query):
    """
    Фильтрует список по поисковому запросу.
    'RTX 5070' найдёт 'RTX 5070 Ti', 'RTX5060' найдёт '5060'.
    'видеокарту' найдёт 'видеокарта' (русское склонение).
    """
    query_words = query.lower().split()
    results = []
    for item in items:
        tokens = _name_tokens(item["name"])
        if all(_query_word_matches(qw, tokens) for qw in query_words):
            results.append(item)
    return results


# Все доступные парсеры
PARSERS = {
    "citilink": CitilinkParser,
    "regard":   RegardParser,
    **CITILINK_PARSERS,
    **REGARD_PARSERS,
    **OLDI_PARSERS,
    **MVIDEO_PARSERS,
    **ELDORADO_PARSERS,
    **WB_PARSERS,
    **KNS_PARSERS,
    **FCENTER_PARSERS,
}

# Алиасы для запуска всех категорий конкретного магазина
_ALL_ALIASES = {
    "citilink-all": list(CITILINK_PARSERS.keys()),
    "regard-all":   list(REGARD_PARSERS.keys()),
    "oldi-all":     list(OLDI_PARSERS.keys()),
    "mvideo-all":   list(MVIDEO_PARSERS.keys()),
    "eldorado-all": list(ELDORADO_PARSERS.keys()),
    "wb-all":       list(WB_PARSERS.keys()),
    "kns-all":      list(KNS_PARSERS.keys()),
    "fcenter-all":  list(FCENTER_PARSERS.keys()),
}


def run_parsers(sources=None, max_pages=None):
    """Запускает парсеры и сохраняет результаты в БД."""
    if sources is None:
        sources = list(PARSERS.keys())

    all_products = {}

    # Эльдорадо использует Group-IB защиту — все его категории запускаем
    # в одной браузерной сессии, чтобы JS-challenge прошёл один раз.
    eldorado_keys = [k for k in sources if k in ELDORADO_PARSERS]
    if eldorado_keys:
        from parser_eldorado_categories import run_all_categories
        run_ids = {}
        for key in eldorado_keys:
            parser_cls = ELDORADO_PARSERS[key]
            source = getattr(parser_cls, "SOURCE_NAME", "eldorado")
            category = getattr(parser_cls, "_CATEGORY", None)
            run_ids[key] = db.start_parse_run(key, source, category)

        try:
            eldorado_results = run_all_categories(eldorado_keys)
        except Exception as e:
            print(f"[eldorado] ОШИБКА run_all_categories: {e}")
            eldorado_results = {k: [] for k in eldorado_keys}

        for key in eldorado_keys:
            parser_cls = ELDORADO_PARSERS[key]
            source = getattr(parser_cls, "SOURCE_NAME", "eldorado")
            category = getattr(parser_cls, "_CATEGORY", None)
            products = eldorado_results.get(key, [])
            all_products[key] = products
            run_id = run_ids.get(key)
            try:
                saved, updated = db.save_products(products, source)
                if products:
                    present_ids = [str(p["id"]) for p in products]
                    db.mark_missing_as_out_of_stock(source, present_ids, category=category)
                db.finish_parse_run(run_id, "ok", len(products), saved, updated, None)
                print(f"[{key}] Сохранено: {saved} новых, {updated} обновлено")
            except Exception as e:
                if run_id:
                    db.finish_parse_run(run_id, "error", 0, 0, 0, error_msg=str(e))
                print(f"[{key}] ОШИБКА при сохранении: {e}")

    _last_source_seen = {}  # source → время последнего запуска категории

    for name in sources:
        if name in ELDORADO_PARSERS:
            continue  # уже обработан выше

        parser_cls = PARSERS.get(name)
        if not parser_cls:
            print(f"Неизвестный источник: {name}")
            continue

        source = getattr(parser_cls, "SOURCE_NAME", None) or name
        category = getattr(parser_cls, "_CATEGORY", None)

        # Задержка между категориями одного магазина (защита от бана)
        _INTER_CATEGORY_DELAY = 30  # секунд
        if source in _last_source_seen:
            elapsed = time.time() - _last_source_seen[source]
            if elapsed < _INTER_CATEGORY_DELAY:
                wait = _INTER_CATEGORY_DELAY - elapsed
                print(f"[{name}] Пауза {wait:.0f}с между категориями {source}...")
                time.sleep(wait)
        _last_source_seen[source] = time.time()

        run_id = None
        try:
            run_id = db.start_parse_run(name, source, category)
            parser = parser_cls()
            if max_pages is not None:
                parser.MAX_PAGES = max_pages
            products = parser.run()
            all_products[name] = products

            # Сохраняем в базу — используем SOURCE_NAME парсера, не ключ словаря
            saved, updated = db.save_products(products, source)
            if products:
                present_ids = [str(p["id"]) for p in products]
                db.mark_missing_as_out_of_stock(source, present_ids, category=category)
            expected = getattr(parser, "_last_total", None)
            db.finish_parse_run(run_id, "ok", len(products), saved, updated, expected)
            print(f"[{name}] Сохранено: {saved} новых, {updated} обновлено")
        except Exception as e:
            if run_id:
                db.finish_parse_run(run_id, "error", 0, 0, 0, error_msg=str(e))
            print(f"[{name}] ОШИБКА: {e}")

    return all_products


def print_comparison(search_query=None):
    """Выводит сравнение цен из базы данных.

    Без поискового запроса — только статистика по магазинам.
    С запросом — список подходящих товаров с ценами.
    """
    offers = db.get_all_offers()

    if not offers:
        print("Нет данных для сравнения.")
        return

    # Без запроса — только сводка по магазинам
    if not search_query:
        from collections import Counter
        counts = Counter(o["source"] for o in offers)
        total_products = len(set(o["name"] for o in offers))
        print(f"\n{'='*70}")
        print(f"БАЗА ДАННЫХ — {total_products} товаров, {len(offers)} предложений")
        print(f"{'='*70}")
        for source, cnt in sorted(counts.items()):
            print(f"  {source.upper().ljust(12)} {cnt:>6} предложений")
        print(f"{'='*70}")
        print("Для поиска: python main.py --show \"RTX 4070\"")
        return

    filtered = filter_by_query(offers, search_query)
    if not filtered:
        print(f"Ничего не найдено по запросу: {search_query!r}")
        return

    # Группируем по названию
    grouped = {}
    for o in filtered:
        name = o["name"]
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(o)

    print(f"\n{'='*70}")
    print(f"РЕЗУЛЬТАТЫ: «{search_query}» — {len(grouped)} товаров")
    print(f"{'='*70}\n")

    for i, (name, shop_offers) in enumerate(grouped.items(), 1):
        shop_offers.sort(key=lambda x: x["price"])
        best = shop_offers[0]

        print(f"{i:2}. {name[:65]}")
        has_comparison = len(shop_offers) > 1
        for o in shop_offers:
            marker = " *" if has_comparison and o["price"] == best["price"] else ""
            source_label = o["source"].upper().ljust(9)
            print(f"    {source_label} {o['price']:>10,} руб.{marker}")
        print()

    all_prices = [o["price"] for o in filtered]
    sources_count = len(set(o["source"] for o in filtered))
    print(f"{'='*70}")
    print(f"Источников: {sources_count}  |  Предложений: {len(filtered)}")
    print(f"Мин. цена: {min(all_prices):,} руб.  |  Макс. цена: {max(all_prices):,} руб.")
    print(f"* = лучшая цена")


def main():
    args = sys.argv[1:]

    # Инициализируем БД при любом режиме
    db.init_db()

    # Режим аудита полноты парсинга
    if "--audit" in args:
        rows = db.get_audit_summary()
        if not rows:
            print("Нет данных. Сначала запустите хотя бы один парсер.")
            return
        header = f"{'Магазин':<12} {'Категория':<8} {'Последний запуск':<20} {'Статус':<8} {'Найдено':>8} {'Ожидалось':>10} {'В БД':>7} {'Покрытие':>9}"
        print(header)
        print("-" * len(header))
        for r in rows:
            last_run = (r["last_run"] or "—")[:19]
            found = "—" if r["items_found"] is None else str(r["items_found"])
            expected = "—" if r["expected_total"] is None else str(r["expected_total"])
            db_count = str(r["db_count"])
            coverage = f"{r['coverage_pct']}%" if r["coverage_pct"] is not None else "—"
            print(f"{r['source']:<12} {(r['category'] or '—'):<8} {last_run:<20} "
                  f"{r['status']:<8} {found:>8} {expected:>10} {db_count:>7} {coverage:>9}")
        return

    # Режим матчинга
    if "--match" in args:
        from matcher import run_matching
        run_matching()
        return

    # Режим веб-сервера
    if "--web" in args:
        import os
        import uvicorn
        port = int(os.environ.get("PORT", 8000))
        uvicorn.run("web_app:app", host="0.0.0.0", port=port, reload=False)
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
              or arg.lower() in (
                  "oldi", "mvideo", "eldorado", "wb", "kns", "fcenter",
              )):
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
        "mvideo":   list(MVIDEO_PARSERS.keys()),
        "eldorado": list(ELDORADO_PARSERS.keys()),
        "wb":       list(WB_PARSERS.keys()),
        "kns":      list(KNS_PARSERS.keys()),
        "fcenter":  list(FCENTER_PARSERS.keys()),
    }
    for shop, keys in _SHOP_TO_PARSERS.items():
        if shop in sources:
            sources.remove(shop)
            sources.extend(keys)

    sources = sources or None  # None = все парсеры
    search_query = " ".join(query_parts) if query_parts else None

    # Запускаем парсеры
    print("Агрегатор цен — Итерация 10")
    print("-" * 40)
    all_products = run_parsers(sources, max_pages=max_pages)

    # Выводим сравнение
    print_comparison(search_query)


if __name__ == "__main__":
    main()
