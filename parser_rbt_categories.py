"""
Парсеры RBT для всех категорий комплектующих ПК.
URL категорий: /catalog/{раздел}/
"""
from parser_rbt import RbtParser

RBT_CATEGORIES = {
    "GPU":    "https://rbt.ru/catalog/videokarty/",
    "CPU":    "https://rbt.ru/catalog/protsessory/",
    "MB":     "https://rbt.ru/catalog/materinskie-platy/",
    "RAM":    "https://rbt.ru/catalog/operativnaya-pamyat/",
    "SSD":    "https://rbt.ru/catalog/ssd-nakopiteli/",
    "HDD":    "https://rbt.ru/catalog/zhestkie-diski/",
    "PSU":    "https://rbt.ru/catalog/bloki-pitaniya/",
    "CASE":   "https://rbt.ru/catalog/kompyuternye-korpusa/",
    "COOLER": "https://rbt.ru/catalog/sistemy-ohlazhdeniya/",
}


def _make_parser(category, url):
    class _Parser(RbtParser):
        SOURCE_NAME = "rbt"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Rbt{category}Parser"
    _Parser.__qualname__ = f"Rbt{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"rbt-{cat.lower()}": _make_parser(cat, url)
    for cat, url in RBT_CATEGORIES.items()
}
