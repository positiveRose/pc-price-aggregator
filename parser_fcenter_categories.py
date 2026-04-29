"""
Парсеры FCenter для всех категорий комплектующих ПК.
URL формат: https://fcenter.ru/product/type/{id}
"""

from parser_fcenter import FcenterParser

FCENTER_CATEGORIES = {
    "GPU":    7,
    "CPU":    3,
    "MB":     2,
    "RAM":    4,
    "SSD":    186,
    "HDD":    5,
    "PSU":    107,
    "CASE":   10,
}


def _make_parser(category, type_id):
    class _Parser(FcenterParser):
        SOURCE_NAME = "fcenter"
        _CATEGORY = category
        _TYPE_ID = type_id

    _Parser.__name__ = f"Fcenter{category}Parser"
    _Parser.__qualname__ = f"Fcenter{category}Parser"
    return _Parser


CATEGORY_PARSERS = {
    f"fcenter-{cat.lower()}": _make_parser(cat, tid)
    for cat, tid in FCENTER_CATEGORIES.items()
}
