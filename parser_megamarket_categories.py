"""
Парсеры МегаМаркет для всех категорий комплектующих ПК.
Каждая категория задаётся URL каталога на megamarket.ru.
"""
from parser_megamarket import MegamarketParser

MEGAMARKET_CATEGORIES = {
    "GPU":    "https://megamarket.ru/catalog/videokarty/",
    "CPU":    "https://megamarket.ru/catalog/protsessory/",
    "MB":     "https://megamarket.ru/catalog/materinskie-platy/",
    "RAM":    "https://megamarket.ru/catalog/operativnaya-pamyat/",
    "SSD":    "https://megamarket.ru/catalog/ssd-nakopiteli/",
    "HDD":    "https://megamarket.ru/catalog/zhestkie-diski-dlya-pk/",
    "PSU":    "https://megamarket.ru/catalog/bloki-pitaniya/",
    "CASE":   "https://megamarket.ru/catalog/kompyuternye-korpusa/",
    "COOLER": "https://megamarket.ru/catalog/kulery-dlya-protsessorov/",
}


def _make_parser(category, url):
    class _Parser(MegamarketParser):
        SOURCE_NAME = "megamarket"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Megamarket{category}Parser"
    _Parser.__qualname__ = f"Megamarket{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"megamarket-{cat.lower()}": _make_parser(cat, url)
    for cat, url in MEGAMARKET_CATEGORIES.items()
}
