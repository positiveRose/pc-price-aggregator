"""
Парсеры M.Video для всех категорий комплектующих ПК.

ВАЖНО: BFF API МВидео использует собственные categoryId, которые НЕ совпадают
с числами из URL. Правильные BFF ID найдены перебором (debug_mvideo_ids.py):
  BFF 5429 → GPU   BFF 5431 → CPU   BFF 5432 → MB    BFF 5433 → RAM
  BFF 5434 → CASE  BFF 5435 → PSU   BFF 5436 → SSD   BFF 5445 → HDD
COOLER: надёжного BFF ID не найдено (5437 возвращает 2 сетевые карты).
"""

from parser_mvideo import MvideoParser

# (catalog_url — для посещения страницы и рефера, bff_id — для BFF API)
MVIDEO_CATEGORIES = {
    "GPU":  ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/videokarty-5429",       "5429"),
    "CPU":  ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/protsessory-5430",      "5431"),
    "MB":   ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/materinskie-platy-5431","5432"),
    "RAM":  ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/operativnaya-pamyat-5432","5433"),
    "SSD":  ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/ssd-nakopiteli-5634",  "5436"),
    "HDD":  ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/zhestkie-diski-5433",  "5445"),
    "PSU":  ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/bloki-pitaniya-5435",  "5435"),
    "CASE": ("https://www.mvideo.ru/komputernye-komplektuushhie-5427/korpusa-5436",          "5434"),
}


def _make_parser(category, catalog_url, bff_id):
    class _Parser(MvideoParser):
        SOURCE_NAME = "mvideo"
        CATALOG_URL = catalog_url
        _CATEGORY = category
        _BFF_ID = bff_id  # правильный BFF categoryId

        def _extract_category_id(self, url):
            return self._BFF_ID

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Mvideo{category}Parser"
    _Parser.__qualname__ = f"Mvideo{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"mvideo-{cat.lower()}": _make_parser(cat, url, bff_id)
    for cat, (url, bff_id) in MVIDEO_CATEGORIES.items()
}
