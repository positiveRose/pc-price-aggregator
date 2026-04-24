"""
Парсеры Онлайн Трейд для всех категорий комплектующих ПК.
Идентификаторы категорий (c###) специфичны для onlinetrade.ru.
"""
from parser_onlinetrade import OnlinetradeParser

ONLINETRADE_CATEGORIES = {
    "GPU":    "https://www.onlinetrade.ru/catalogue/videokarty-c396/",
    "CPU":    "https://www.onlinetrade.ru/catalogue/protsessory-c387/",
    "MB":     "https://www.onlinetrade.ru/catalogue/materinskie_platy-c388/",
    "RAM":    "https://www.onlinetrade.ru/catalogue/operativnaya_pamyat-c1099/",
    "SSD":    "https://www.onlinetrade.ru/catalogue/ssd_nakopiteli-c1127/",
    "HDD":    "https://www.onlinetrade.ru/catalogue/zhostkie_diski-c390/",
    "PSU":    "https://www.onlinetrade.ru/catalogue/bloki_pitaniya-c394/",
    "CASE":   "https://www.onlinetrade.ru/catalogue/kompyuternye_korpusa-c393/",
    "COOLER": "https://www.onlinetrade.ru/catalogue/kulery_dlya_cpu-c1135/",
}


def _make_parser(category, url):
    class _Parser(OnlinetradeParser):
        SOURCE_NAME = "onlinetrade"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Onlinetrade{category}Parser"
    _Parser.__qualname__ = f"Onlinetrade{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"onlinetrade-{cat.lower()}": _make_parser(cat, url)
    for cat, url in ONLINETRADE_CATEGORIES.items()
}
