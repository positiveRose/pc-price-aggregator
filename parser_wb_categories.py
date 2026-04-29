"""
Парсеры Wildberries для всех категорий комплектующих ПК.

Использует catalog.wb.ru с точными category/subject ID из меню WB
(main-menu-ru-ru-v3.json) — это исключает шум из текстового поиска
(ноутбучные CPU/RAM и т.д.).
"""

from parser_wb import WbParser

WB_CATEGORIES = {
    "GPU":    {"shard": "electronic73", "query": "subject=3274"},
    "CPU":    {"shard": "electronic71", "query": "subject=3698"},
    "MB":     {"shard": "electronic72", "query": "subject=3690"},
    "RAM":    {"shard": "electronic72", "query": "subject=3357"},
    "SSD":    {"shard": "electronic72", "query": "cat=131997"},
    "HDD":    {"shard": "electronic73", "query": "cat=132000"},
    "PSU":    {"shard": "electronic72", "query": "subject=8994"},
    "CASE":   {"shard": "electronic72", "query": "subject=4066"},
    "COOLER": {"shard": "electronic72", "query": "cat=132001"},
}


def _make_parser(category, cfg):
    class _Parser(WbParser):
        SOURCE_NAME = "wb"
        CATALOG_SHARD = cfg["shard"]
        CATALOG_QUERY = cfg["query"]
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Wb{category}Parser"
    _Parser.__qualname__ = f"Wb{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"wb-{cat.lower()}": _make_parser(cat, cfg)
    for cat, cfg in WB_CATEGORIES.items()
}
