"""
Парсеры КЕЙ для всех категорий комплектующих ПК.
Каждая категория задаётся URL каталога на key.ru.
"""
from parser_key import KeyParser

KEY_CATEGORIES = {
    "GPU":    "https://www.key.ru/catalog/videokarty/",
    "CPU":    "https://www.key.ru/catalog/protsessory-dlya-pk/",
    "MB":     "https://www.key.ru/catalog/materinskie-platy/",
    "RAM":    "https://www.key.ru/catalog/operativnaya-pamyat/",
    "SSD":    "https://www.key.ru/catalog/ssd-nakopiteli/",
    "HDD":    "https://www.key.ru/catalog/vnutrennie-zhestkie-diski/",
    "PSU":    "https://www.key.ru/catalog/bloki-pitaniya-atx/",
    "CASE":   "https://www.key.ru/catalog/kompyuternye-korpusa/",
    "COOLER": "https://www.key.ru/catalog/kulerv-dlya-protsessorov/",
}


def _make_parser(category, url):
    class _Parser(KeyParser):
        SOURCE_NAME = "key"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Key{category}Parser"
    _Parser.__qualname__ = f"Key{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"key-{cat.lower()}": _make_parser(cat, url)
    for cat, url in KEY_CATEGORIES.items()
}
