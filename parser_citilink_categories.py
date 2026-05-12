"""
Парсеры Ситилинк для всех категорий комплектующих ПК.

Каждая категория запускается в отдельном subprocess — это обеспечивает
полную изоляцию: таймаут убивает subprocess целиком без повреждения
greenlet/asyncio состояния родительского процесса. Предыдущий подход
с pkill + threading.Timer приводил к:
  greenlet.error: cannot switch to a different thread (which happens to have exited)
после чего все оставшиеся страницы категории падали.

Результаты пишутся инкрементально после каждой страницы — так что даже
при принудительном убийстве subprocess данные от уже загруженных страниц
не теряются.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from base_parser import _CHROMIUM_ARGS, PARSER_PROXY
from parser_citilink import CitilinkParser

# На Railway --dns-over-https-mode=secure вешает Chromium намертво если DoH-пакеты
# дропаются (без RST) — Playwright-таймаут не срабатывает, браузер ждёт DNS вечно.
_CITILINK_CHROMIUM_ARGS = [a for a in _CHROMIUM_ARGS
                           if "dns-over-https" not in a] + [
    "--no-sandbox",
    "--disable-setuid-sandbox",
]

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

_CARD_SELECTOR = CitilinkParser.CARD_SELECTOR
_WAIT_TIMEOUT  = 45000
_PAGE_DELAY    = 5   # секунд между страницами одной категории
_CAT_DELAY     = 5   # секунд между категориями
_CAT_HARD_TIMEOUT = 700  # секунд на одну категорию до принудительного kill subprocess


def _make_parser(category, url):
    """Создаёт класс парсера для конкретной категории Ситилинк."""
    class _Parser(CitilinkParser):
        SOURCE_NAME = "citilink"
        CATALOG_URL = url
        WAIT_TIMEOUT = _WAIT_TIMEOUT
        DELAY_BETWEEN_PAGES = _PAGE_DELAY
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
CATEGORY_PARSERS = {
    f"citilink-{cat.lower()}": _make_parser(cat, url)
    for cat, url in CITILINK_CATEGORIES.items()
}


def _load_page(page, url):
    """Загружает страницу Citilink с обработкой WAF-редиректа."""
    try:
        page.goto(url, wait_until="commit", timeout=60000)
    except Exception as e:
        print(f"[citilink] goto timeout/error на {url}: {e}", flush=True)
        time.sleep(5)

    try:
        page.wait_for_selector(_CARD_SELECTOR, timeout=_WAIT_TIMEOUT)
        time.sleep(2)
    except Exception:
        # WAF может не успеть сделать редирект — ждём ещё
        time.sleep(10)
        try:
            page.wait_for_selector(_CARD_SELECTOR, timeout=30000)
            time.sleep(2)
        except Exception:
            preview = page.content()[:300].replace("\n", " ")
            print(f"[citilink] Карточки не появились на {url}", flush=True)
            print(f"[citilink] HTML preview: {preview}", flush=True)

    # Скроллим для подгрузки lazy-loading карточек
    for _ in range(10):
        page.evaluate("window.scrollBy(0, 800)")
        time.sleep(0.4)
    # Докручиваем до самого низа, чтобы пагинация гарантированно отрисовалась
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(2.5)

    return page.content()


def _run_category_internal(key, output_path=None):
    """Запускает одну категорию в текущем процессе.

    Пишет результаты инкрементально в output_path (JSON) после каждой
    страницы — так данные сохраняются даже если процесс будет убит.

    Args:
        key: ключ из CATEGORY_PARSERS ('citilink-gpu' и т.д.)
        output_path: путь к файлу для записи результатов; None = не писать.

    Returns:
        list of product dicts
    """
    parser_cls = CATEGORY_PARSERS.get(key)
    if not parser_cls:
        return []

    parser = parser_cls()
    cat = parser_cls._CATEGORY
    all_products = []

    ctx_opts = {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "ru-RU",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    }
    if PARSER_PROXY:
        ctx_opts["proxy"] = {"server": PARSER_PROXY}

    def _save(products):
        """Записывает текущие результаты в файл (инкрементальное сохранение)."""
        if output_path:
            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(products, f, ensure_ascii=False)
            except Exception as e:
                print(f"[citilink] [{cat}] Ошибка записи в {output_path}: {e}", flush=True)

    print(f"[citilink] [{cat}] Запускаю Chromium...", flush=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=_CITILINK_CHROMIUM_ARGS)
        context = browser.new_context(**ctx_opts)
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        # Блокируем изображения, видео и шрифты — снижает OOM-крashi Chromium.
        # На результат парсинга не влияет: в БД сохраняются только текст и URL.
        def _block_resources(route):
            if route.request.resource_type in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()
        page.route("**/*", _block_resources)

        try:
            html = _load_page(page, parser_cls.CATALOG_URL)
            all_products.extend(parser.parse_products(html))
            _save(all_products)

            # get_total_pages читает totalPages из __NEXT_DATA__ (SSR-блок),
            # который всегда присутствует в html с первой загрузки страницы.
            total_pages = min(parser.get_total_pages(html), parser.MAX_PAGES)
            print(f"[citilink] [{cat}] Страниц: {total_pages}", flush=True)

            for page_num in range(2, total_pages + 1):
                time.sleep(_PAGE_DELAY)
                page_url = parser.get_page_url(page_num)
                print(f"[citilink] [{cat}] Стр. {page_num}: {page_url}", flush=True)
                try:
                    html = _load_page(page, page_url)
                    all_products.extend(parser.parse_products(html))
                    _save(all_products)
                except Exception as e:
                    print(f"[citilink] Ошибка на стр. {page_num}: {e}", flush=True)
                    if "crash" in str(e).lower():
                        # Page crashed (OOM) — пересоздаём context и page
                        print(f"[citilink] [{cat}] Page crash на стр. {page_num}, "
                              f"пересоздаю context...", flush=True)
                        try:
                            context.close()
                        except Exception:
                            pass
                        try:
                            context = browser.new_context(**ctx_opts)
                            page = context.new_page()
                            Stealth().apply_stealth_sync(page)
                            page.route("**/*", _block_resources)
                        except Exception as e2:
                            print(f"[citilink] Не удалось пересоздать context: {e2}", flush=True)
                            break

        except Exception as e:
            print(f"[citilink] [{cat}] ОШИБКА: {e}", flush=True)
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    print(f"[citilink] [{cat}] Найдено товаров: {len(all_products)}", flush=True)
    _save(all_products)
    return all_products


def run_all_categories(keys=None):
    """Запускает категории Ситилинк, каждую в отдельном subprocess.

    Изоляция по процессу: таймаут убивает subprocess целиком — без
    повреждения greenlet/asyncio состояния родительского процесса.
    Данные пишутся инкрементально: при таймауте возвращаются частичные
    результаты от уже успевших загрузиться страниц.

    Args:
        keys: список ключей из CATEGORY_PARSERS; None = все категории.

    Returns:
        dict {key: [products]}
    """
    if keys is None:
        keys = list(CATEGORY_PARSERS.keys())

    results = {k: [] for k in keys}
    proj_dir = str(Path(__file__).parent)

    print(f"[citilink] Запуск run_all_categories, категорий: {len(keys)}", flush=True)

    for key in keys:
        parser_cls = CATEGORY_PARSERS.get(key)
        if not parser_cls:
            continue
        cat = parser_cls._CATEGORY

        # Временный файл для результатов subprocess
        output_fd, output_path = tempfile.mkstemp(suffix=".json")
        os.close(output_fd)

        script = (
            f"import sys\n"
            f"sys.path.insert(0, {proj_dir!r})\n"
            f"from parser_citilink_categories import _run_category_internal\n"
            f"_run_category_internal({key!r}, {output_path!r})\n"
        )

        print(f"[citilink] [{cat}] Запускаю subprocess...", flush=True)
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            # stdout/stderr наследуются от родителя → видно в Railway логах
            # start_new_session=True создаёт новую сессию (process group),
            # что позволяет убить весь дерево (Python + Chromium) одним killpg.
            start_new_session=True,
        )

        timed_out = False
        try:
            proc.wait(timeout=_CAT_HARD_TIMEOUT)
        except subprocess.TimeoutExpired:
            timed_out = True
            print(
                f"[citilink] [{cat}] ТАЙМАУТ {_CAT_HARD_TIMEOUT}s — "
                f"убиваю process group (pid={proc.pid})",
                flush=True,
            )
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass

        # Читаем результаты (могут быть частичными если был таймаут)
        try:
            with open(output_path, encoding="utf-8") as f:
                content = f.read().strip()
            if content:
                products = json.loads(content)
                results[key] = products
                suffix = " (частично, таймаут)" if timed_out else ""
                print(
                    f"[citilink] [{cat}] Итого товаров: {len(products)}{suffix}",
                    flush=True,
                )
            else:
                print(f"[citilink] [{cat}] Нет данных (пустой файл результатов)", flush=True)
        except Exception as e:
            print(f"[citilink] [{cat}] Ошибка чтения результатов: {e}", flush=True)
        finally:
            try:
                os.remove(output_path)
            except Exception:
                pass

        time.sleep(_CAT_DELAY)

    return results
