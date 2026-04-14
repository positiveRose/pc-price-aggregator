"""
Парсеры Ozon для всех категорий комплектующих ПК.
URL формат: https://www.ozon.ru/category/{slug}-{id}/
"""

from parser_ozon import OzonParser

OZON_CATEGORIES = {
    "GPU":    "https://www.ozon.ru/category/videokarty-i-karty-videozahvata-15720/",
    "CPU":    "https://www.ozon.ru/category/protsessory-15726/",
    "MB":     "https://www.ozon.ru/category/materinskie-platy-15725/",
    "RAM":    "https://www.ozon.ru/category/operativnaya-pamyat-15724/",
    "SSD":    "https://www.ozon.ru/category/ssd-nakopiteli-15712/",
    "HDD":    "https://www.ozon.ru/category/zhestkie-diski-ssd-i-setevye-nakopiteli-15710/",
    "PSU":    "https://www.ozon.ru/category/bloki-pitaniya-15727/",
    "CASE":   "https://www.ozon.ru/category/korpusa-dlya-kompyuterov-15734/",
    "COOLER": "https://www.ozon.ru/category/kulery-dlya-protsessora/",
}


def _make_parser(category, url):
    class _Parser(OzonParser):
        SOURCE_NAME = "ozon"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Ozon{category}Parser"
    _Parser.__qualname__ = f"Ozon{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"ozon-{cat.lower()}": _make_parser(cat, url)
    for cat, url in OZON_CATEGORIES.items()
}
