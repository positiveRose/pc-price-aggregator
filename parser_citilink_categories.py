"""
Парсеры Ситилинк для всех категорий комплектующих ПК.
Используют единую фабричную функцию — никакого дублирования кода.
"""

from parser_citilink import CitilinkParser

# Категория → URL каталога на Ситилинк
CITILINK_CATEGORIES = {
    "GPU":    "https://www.citilink.ru/catalog/videokarty/",
    "CPU":    "https://www.citilink.ru/catalog/processory/",
    "MB":     "https://www.citilink.ru/catalog/materinskie-platy/",
    "RAM":    "https://www.citilink.ru/catalog/moduli-pamyati/",
    "SSD":    "https://www.citilink.ru/catalog/ssd-nakopiteli/",
    "HDD":    "https://www.citilink.ru/catalog/zhestkie-diski/",
    "PSU":    "https://www.citilink.ru/catalog/bloki-pitaniya/",
    "CASE":   "https://www.citilink.ru/catalog/korpusa/",
    "COOLER": "https://www.citilink.ru/catalog/sistemy-ohlazhdeniya-processora/",
}


def _make_parser(category, url, wait_timeout=15000):
    """Создаёт класс парсера для конкретной категории Ситилинк."""
    class _Parser(CitilinkParser):
        SOURCE_NAME = "citilink"
        CATALOG_URL = url
        WAIT_TIMEOUT = wait_timeout
        _CATEGORY = category

        def parse_products(self, html):
            products = super().parse_products(html)
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Citilink{category}Parser"
    _Parser.__qualname__ = f"Citilink{category}Parser"
    return _Parser


# Словарь: ключ для CLI → класс парсера
# Пример: "citilink-cpu", "citilink-ram", "citilink-all" (все сразу)
CATEGORY_PARSERS = {
    f"citilink-{cat.lower()}": _make_parser(
        cat, url,
        wait_timeout=30000 if cat == "COOLER" else 15000
    )
    for cat, url in CITILINK_CATEGORIES.items()
}
