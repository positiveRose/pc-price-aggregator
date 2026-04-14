"""
Парсеры e2e4 для всех категорий комплектующих ПК.
URL формат: https://e2e4online.ru/catalog/{slug}-{id}/
"""

from parser_e2e4 import E2e4Parser

E2E4_CATEGORIES = {
    "GPU":    "https://e2e4online.ru/catalog/videokarty-11/",
    "CPU":    "https://e2e4online.ru/catalog/protsessory-12/",
    "MB":     "https://e2e4online.ru/catalog/materinskie-platy-13/",
    "RAM":    "https://e2e4online.ru/catalog/moduli-pamyati-14/",
    "SSD":    "https://e2e4online.ru/catalog/ssd-nakopiteli-30/",
    "HDD":    "https://e2e4online.ru/catalog/zhestkie-diski-15/",
    "PSU":    "https://e2e4online.ru/catalog/bloki-pitaniya-22/",
    "CASE":   "https://e2e4online.ru/catalog/korpusa-23/",
    "COOLER": "https://e2e4online.ru/catalog/sistemy-ohlazhdeniya-24/",
}


def _make_parser(category, url):
    class _Parser(E2e4Parser):
        SOURCE_NAME = "e2e4"
        CATALOG_URL = url
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"E2e4{category}Parser"
    _Parser.__qualname__ = f"E2e4{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"e2e4-{cat.lower()}": _make_parser(cat, url)
    for cat, url in E2E4_CATEGORIES.items()
}
