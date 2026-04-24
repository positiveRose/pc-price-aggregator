"""
Парсеры Технопарк для всех категорий комплектующих ПК.
URL категорий: /catalog/computers/{раздел}/
"""
from parser_technopark import TechnopaркParser

TECHNOPARK_CATEGORIES = {
    "GPU":    "https://www.technopark.ru/catalog/computers/videokarty/",
    "CPU":    "https://www.technopark.ru/catalog/computers/protsessory/",
    "MB":     "https://www.technopark.ru/catalog/computers/materinskie-platy/",
    "RAM":    "https://www.technopark.ru/catalog/computers/operativnaya-pamyat/",
    "SSD":    "https://www.technopark.ru/catalog/computers/ssd/",
    "HDD":    "https://www.technopark.ru/catalog/computers/zhestkie-diski/",
    "PSU":    "https://www.technopark.ru/catalog/computers/bloki-pitaniya/",
    "CASE":   "https://www.technopark.ru/catalog/computers/korpusa/",
    "COOLER": "https://www.technopark.ru/catalog/computers/sistemy-ohlazhdeniya/",
}


def _make_parser(category, url):
    class _Parser(TechnopaркParser):
        SOURCE_NAME = "technopark"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Technopark{category}Parser"
    _Parser.__qualname__ = f"Technopark{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"technopark-{cat.lower()}": _make_parser(cat, url)
    for cat, url in TECHNOPARK_CATEGORIES.items()
}
