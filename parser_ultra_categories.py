"""
Парсеры Ultra для всех категорий комплектующих ПК.
URL категорий: /{раздел}/
"""
from parser_ultra import UltraParser

ULTRA_CATEGORIES = {
    "GPU":    "https://www.ultra.ru/videokarty/",
    "CPU":    "https://www.ultra.ru/protsessory/",
    "MB":     "https://www.ultra.ru/materinskie-platy/",
    "RAM":    "https://www.ultra.ru/operativnaya-pamyat/",
    "SSD":    "https://www.ultra.ru/ssd-nakopiteli/",
    "HDD":    "https://www.ultra.ru/zhestkie-diski/",
    "PSU":    "https://www.ultra.ru/bloki-pitaniya/",
    "CASE":   "https://www.ultra.ru/kompyuternye-korpusa/",
    "COOLER": "https://www.ultra.ru/sistemy-ohlazhdeniya/",
}


def _make_parser(category, url):
    class _Parser(UltraParser):
        SOURCE_NAME = "ultra"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Ultra{category}Parser"
    _Parser.__qualname__ = f"Ultra{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"ultra-{cat.lower()}": _make_parser(cat, url)
    for cat, url in ULTRA_CATEGORIES.items()
}
