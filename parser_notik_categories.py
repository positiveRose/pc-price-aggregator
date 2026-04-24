"""
Парсеры Нотик для всех категорий комплектующих ПК.
URL категорий: /search_catalog/filter/{slug}/
Товары по URL: /goods/{id}/
"""
from parser_notik import NotikParser

NOTIK_CATEGORIES = {
    "GPU":    "https://www.notik.ru/search_catalog/filter/videocard/",
    "CPU":    "https://www.notik.ru/search_catalog/filter/processor/",
    "MB":     "https://www.notik.ru/search_catalog/filter/mainboard/",
    "RAM":    "https://www.notik.ru/search_catalog/filter/memory/",
    "SSD":    "https://www.notik.ru/search_catalog/filter/ssd/",
    "HDD":    "https://www.notik.ru/search_catalog/filter/hdd/",
    "PSU":    "https://www.notik.ru/search_catalog/filter/psu/",
    "CASE":   "https://www.notik.ru/search_catalog/filter/case/",
    "COOLER": "https://www.notik.ru/search_catalog/filter/cooler/",
}


def _make_parser(category, url):
    class _Parser(NotikParser):
        SOURCE_NAME = "notik"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Notik{category}Parser"
    _Parser.__qualname__ = f"Notik{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"notik-{cat.lower()}": _make_parser(cat, url)
    for cat, url in NOTIK_CATEGORIES.items()
}
