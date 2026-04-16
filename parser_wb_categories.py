"""
Парсеры Wildberries для всех категорий комплектующих ПК.
Каждая категория задаётся поисковым запросом к search.wb.ru API.
"""

from parser_wb import WbParser

WB_CATEGORIES = {
    "GPU":    "видеокарта",
    "CPU":    "процессор для компьютера",
    "MB":     "материнская плата",
    "RAM":    "оперативная память",
    "SSD":    "SSD накопитель",
    "HDD":    "жесткий диск внутренний",
    "PSU":    "блок питания ATX",
    "CASE":   "корпус компьютерный",
    "COOLER": "кулер для процессора",
}


def _make_parser(category, query):
    class _Parser(WbParser):
        SOURCE_NAME = "wb"
        SEARCH_QUERY = query
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
    f"wb-{cat.lower()}": _make_parser(cat, query)
    for cat, query in WB_CATEGORIES.items()
}
