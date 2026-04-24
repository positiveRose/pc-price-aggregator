"""
Парсеры Эльдорадо для всех категорий комплектующих ПК.
URL формат: https://www.eldorado.ru/c/{category-slug}/
"""

from parser_eldorado import EldoradoParser

ELDORADO_CATEGORIES = {
    "GPU":    "https://www.eldorado.ru/c/videokarty/",
    "CPU":    "https://www.eldorado.ru/c/protsessory/",
    "MB":     "https://www.eldorado.ru/c/materinskie-platy/",
    "RAM":    "https://www.eldorado.ru/c/operativnaya-pamyat/",
    "SSD":    "https://www.eldorado.ru/c/ssd/",
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
