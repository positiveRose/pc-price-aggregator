"""
Парсеры Регард для всех категорий комплектующих ПК.
URL формат: https://www.regard.ru/catalog/hits?q=BASE64({"byCategory":ID})
Каждая категория запускается в отдельном браузере через parser.run()
с BROWSER_RESTART_EVERY=5 (защита от утечек памяти внутри категории).
"""

from parser_regard import RegardParser

# Прямые URL каталога regard.ru (проверены — возвращают реальные товары)
REGARD_CATEGORIES = {
    "GPU":    "https://www.regard.ru/catalog/1013/videokarty",
    "CPU":    "https://www.regard.ru/catalog/1001/processory",
    "MB":     "https://www.regard.ru/catalog/1000/materinskie-platy",
    "RAM":    "https://www.regard.ru/catalog/1010/operativnaya-pamyat",
    "SSD":    "https://www.regard.ru/catalog/1015/ssd",
    "HDD":    "https://www.regard.ru/catalog/1014/zhestkie-diski",
    "PSU":    "https://www.regard.ru/catalog/1225/bloki-pitaniya",
    "CASE":   "https://www.regard.ru/catalog/1032/korpusa",
    # 1003 = хаб-страница категорий (Card_wrap=0, товаров нет)
    # 1008 = жидкостное охлаждение — реальный листинг с Card_wrap ✓
    "COOLER": "https://www.regard.ru/catalog/1008/zidkostnoe-oxlazdenie-szo",
}


def _make_parser(category, url):
    class _Parser(RegardParser):
        SOURCE_NAME = "regard"
        CATALOG_URL = url
        WAIT_TIMEOUT = 45000
        DELAY_BETWEEN_PAGES = 5
        MAX_PAGES = 15
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
