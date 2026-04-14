"""
Парсеры Регард для всех категорий комплектующих ПК.
URL формат: https://www.regard.ru/catalog/hits?q=BASE64({"byCategory":ID})
"""

from parser_regard import RegardParser

# Точные URL из каталога regard.ru
REGARD_CATEGORIES = {
    "GPU":    "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDEzfQ",
    "CPU":    "https://www.regard.ru/catalog/1001/processory",
    "MB":     "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDAwfQ",
    "RAM":    "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDEwfQ",
    "SSD":    "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDE1fQ",
    "HDD":    "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDE0fQ",
    "PSU":    "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMjI1fQ",
    "CASE":   "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDMyfQ",
    "COOLER": "https://www.regard.ru/catalog/hits?q=eyJieUNhdGVnb3J5IjoxMDAzfQ",
}


def _make_parser(category, url):
    class _Parser(RegardParser):
        SOURCE_NAME = "regard"
        CATALOG_URL = url
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

        def get_page_url(self, page_num):
            # hits URL содержит ?, обычный — нет
            sep = "&" if "?" in self.CATALOG_URL else "?"
            return f"{self.CATALOG_URL}{sep}page={page_num}"

    _Parser.__name__ = f"Regard{category}Parser"
    _Parser.__qualname__ = f"Regard{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"regard-{cat.lower()}": _make_parser(cat, url)
    for cat, url in REGARD_CATEGORIES.items()
}
