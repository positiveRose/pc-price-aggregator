"""
Парсеры OLDI для всех категорий комплектующих ПК.
URL формат: https://www.oldi.ru/catalog/{slug}/
Пагинация:  https://www.oldi.ru/catalog/{slug}/page-{N}/
"""

from parser_oldi import OldiParser

OLDI_CATEGORIES = {
    "GPU":    "https://www.oldi.ru/catalog/videokarta/",
    "CPU":    "https://www.oldi.ru/catalog/processor/",
    "MB":     "https://www.oldi.ru/catalog/materinskaya_plata/",
    "RAM":    "https://www.oldi.ru/catalog/6587/",
    "SSD":    "https://www.oldi.ru/catalog/vnutrenniy_ssd_disk/",
    "HDD":    "https://www.oldi.ru/catalog/vnutrenniy_jestkiy_disk/",
    "PSU":    "https://www.oldi.ru/catalog/blok_pitaniya_komputera/",
    "CASE":   "https://www.oldi.ru/catalog/komputerniy_korpus/",
    "COOLER": "https://www.oldi.ru/catalog/kuler_dlya_processora/",
}


def _make_parser(category, url):
    class _Parser(OldiParser):
        SOURCE_NAME = "oldi"
        CATALOG_URL = url
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

        def get_page_url(self, page_num):
            base = self.CATALOG_URL.rstrip("/")
            return f"{base}/page-{page_num}/"

    _Parser.__name__ = f"Oldi{category}Parser"
    _Parser.__qualname__ = f"Oldi{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"oldi-{cat.lower()}": _make_parser(cat, url)
    for cat, url in OLDI_CATEGORIES.items()
}
