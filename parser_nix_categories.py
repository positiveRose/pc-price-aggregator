"""
Парсеры NIX.ru для всех категорий комплектующих ПК.
URL формат: https://www.nix.ru/price/price_list.html?section={section}&page={N}
"""

from parser_nix import NixParser

NIX_CATEGORIES = {
    "GPU":    "video_cards_all",
    "CPU":    "cpu_all",
    "MB":     "motherboards_all",
    "RAM":    "memory_modules_all",
    "SSD":    "ssd_all",
    "HDD":    "hdd_all",
    "PSU":    "power_supplies_all",
    "CASE":   "cases_all",
    "COOLER": "coolers_fans_all",
}


def _make_parser(category, section):
    class _Parser(NixParser):
        SOURCE_NAME = "nix"
        CATALOG_URL = f"https://www.nix.ru/price/price_list.html?section={section}"
        _SECTION = section
        _CATEGORY = category

        def run(self):
            products = super().run()
            for p in products:
                p["category"] = self._CATEGORY
            return products

    _Parser.__name__ = f"Nix{category}Parser"
    _Parser.__qualname__ = f"Nix{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"nix-{cat.lower()}": _make_parser(cat, section)
    for cat, section in NIX_CATEGORIES.items()
}
