"""
Парсеры KNS для всех категорий комплектующих ПК.
URL формат: https://www.kns.ru/catalog/komplektuyuschie/{slug}/
"""

from parser_kns import KnsParser, BASE_URL

KNS_CATEGORIES = {
    "GPU":    "/catalog/komplektuyuschie/videokarty/",
    "CPU":    "/catalog/komplektuyuschie/protsessory/",
    "MB":     "/catalog/komplektuyuschie/materinskie-platy/",
    "RAM":    "/catalog/komplektuyuschie/pamyat/",
    "SSD":    "/catalog/komplektuyuschie/ssd/",
    "HDD":    "/catalog/komplektuyuschie/zhestkie-diski/",
    "PSU":    "/catalog/komplektuyuschie/bloki-pitaniya/",
    "CASE":   "/catalog/komplektuyuschie/korpusa/",
    "COOLER": "/catalog/komplektuyuschie/kulery/",
}


def _make_parser(category, url_path):
    class _Parser(KnsParser):
        SOURCE_NAME = "kns"
        _CATEGORY = category
        CATALOG_URL = BASE_URL + url_path

    _Parser.__name__ = f"Kns{category}Parser"
    _Parser.__qualname__ = f"Kns{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"kns-{cat.lower()}": _make_parser(cat, path)
    for cat, path in KNS_CATEGORIES.items()
}
