"""
Парсеры M.Video для всех категорий комплектующих ПК.
URL формат: https://www.mvideo.ru/{category-slug}-{category-id}
"""

from parser_mvideo import MvideoParser

# CPU (5430) и SSD (5634) возвращают 0 через v2/search API —
# эти категории недоступны через BFF API МВидео.
MVIDEO_CATEGORIES = {
    "GPU":    "https://www.mvideo.ru/komputernye-komplektuushhie-5427/videokarty-5429",
    "MB":     "https://www.mvideo.ru/komputernye-komplektuushhie-5427/materinskie-platy-5431",
    "RAM":    "https://www.mvideo.ru/komputernye-komplektuushhie-5427/operativnaya-pamyat-5432",
    "HDD":    "https://www.mvideo.ru/komputernye-komplektuushhie-5427/zhestkie-diski-5433",
    "PSU":    "https://www.mvideo.ru/komputernye-komplektuushhie-5427/bloki-pitaniya-5435",
    "CASE":   "https://www.mvideo.ru/komputernye-komplektuushhie-5427/korpusa-5436",
    "COOLER": "https://www.mvideo.ru/komputernye-komplektuushhie-5427/sistemy-ohlazhdeniya-5437",
}


def _make_parser(category, url):
    class _Parser(MvideoParser):
        SOURCE_NAME = "mvideo"
        CATALOG_URL = url
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Mvideo{category}Parser"
    _Parser.__qualname__ = f"Mvideo{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"mvideo-{cat.lower()}": _make_parser(cat, url)
    for cat, url in MVIDEO_CATEGORIES.items()
}
