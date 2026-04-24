"""
Парсеры Wildberries для всех категорий комплектующих ПК.

GPU использует menu_redirect_131994 (подтверждённый ID категории).
Остальные категории используют точные текстовые запросы — WB exactmatch
возвращает товары, где все слова запроса присутствуют в названии/категории.

Если найдёшь правильные menu_redirect ID для других категорий
(DevTools → Network на странице категории WB), замени запросы.
"""

from parser_wb import WbParser

WB_CATEGORIES = {
    "GPU":    "menu_redirect_131994 видеокарты",
    "CPU":    "процессор",
    "MB":     "материнская плата",
    "RAM":    "оперативная память",
    "SSD":    "SSD накопитель",
    "HDD":    "жёсткий диск внутренний",
    "PSU":    "блок питания ATX",
    "CASE":   "корпус компьютерный ATX",
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
