"""
Парсеры Wildberries для всех категорий комплектующих ПК.
Базовый путь: wildberries.ru/catalog/elektronika/noutbuki-i-kompyutery/komplektuyushchie-dlya-pk/{slug}
"""

from parser_wb import WbParser

_BASE = "https://www.wildberries.ru/catalog/elektronika/noutbuki-i-kompyutery/komplektuyushchie-dlya-pk"

WB_CATEGORIES = {
    "GPU": f"{_BASE}/videokarty",
    "CPU": f"{_BASE}/protsessory",
    "MB":  f"{_BASE}/materinskie-platy",
    "RAM": f"{_BASE}/operativnaya-pamyat",
    "SSD": f"{_BASE}/ssd-nakopiteli",
    # HDD и SSD объединены на WB в одну категорию
    "HDD": f"{_BASE}/zhestkie-diski-i-ssd",
}

# PSU / CASE / COOLER на WB не найдены в разделе komplektuyushchie-dlya-pk


def _make_parser(category, url):
    class _Parser(WbParser):
        SOURCE_NAME = "wb"
        CATALOG_URL = url
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
    f"wb-{cat.lower()}": _make_parser(cat, url)
    for cat, url in WB_CATEGORIES.items()
}
