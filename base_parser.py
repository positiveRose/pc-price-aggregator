"""
Базовый класс для всех парсеров магазинов.

Каждый парсер наследует BaseParser и реализует:
- SOURCE_NAME — название магазина для БД
- CATALOG_URL — URL каталога
- parse_products(html) — извлечение товаров из HTML
- get_total_pages(html) — определение числа страниц
- get_page_url(page_num) — URL конкретной страницы
"""

import os
import socket
import struct
import time
from abc import ABC, abstractmethod
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth


# ── DNS-фоллбэк через 8.8.8.8 ───────────────────────────────────────────────
# Если VPN перехватывает DNS и не может разрезолвить российские домены,
# перехватываем socket.getaddrinfo: сначала обычный DNS, при ошибке — 8.8.8.8.
# Никакого прокси не нужно.
# ─────────────────────────────────────────────────────────────────────────────

def _dns_query_udp(hostname: str, nameserver: str = "8.8.8.8") -> str | None:
    """Минимальный A-record запрос через UDP без зависимостей."""
    try:
        # Собираем DNS-пакет вручную
        tid = b"\xAB\xCD"
        header = tid + b"\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00"
        question = b""
        for label in hostname.encode().split(b"."):
            question += bytes([len(label)]) + label
        question += b"\x00\x00\x01\x00\x01"  # TYPE A, CLASS IN

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)
        sock.sendto(header + question, (nameserver, 53))
        data, _ = sock.recvfrom(512)
        sock.close()

        # Пропускаем заголовок (12 байт) и секцию вопроса
        offset = 12
        while data[offset] != 0:
            if data[offset] & 0xC0 == 0xC0:
                offset += 2
                break
            offset += data[offset] + 1
        else:
            offset += 1
        offset += 4  # QTYPE + QCLASS

        ancount = struct.unpack("!H", data[6:8])[0]
        if ancount < 1:
            return None

        # Пропускаем имя в первом ответе
        if data[offset] & 0xC0 == 0xC0:
            offset += 2
        else:
            while data[offset] != 0:
                offset += data[offset] + 1
            offset += 1

        rtype = struct.unpack("!H", data[offset : offset + 2])[0]
        offset += 8   # type(2) + class(2) + ttl(4)
        rdlen = struct.unpack("!H", data[offset : offset + 2])[0]
        offset += 2

        if rtype == 1 and rdlen == 4:  # A-запись
            return ".".join(str(b) for b in data[offset : offset + 4])
    except Exception:
        pass
    return None


_original_getaddrinfo = socket.getaddrinfo


def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    try:
        return _original_getaddrinfo(host, port, family, type, proto, flags)
    except socket.gaierror:
        # DNS через VPN не сработал — пробуем напрямую через 8.8.8.8
        ip = _dns_query_udp(str(host))
        if ip:
            print(f"[dns] VPN-DNS fail → 8.8.8.8: {host} → {ip}")
            return _original_getaddrinfo(ip, port, family, type, proto, flags)
        raise


socket.getaddrinfo = _patched_getaddrinfo

# ── Прокси ──────────────────────────────────────────────────────────────────
# Читается из переменной окружения PARSER_PROXY или файла .parser_proxy.
# Формат: socks5://127.0.0.1:1080  или  http://user:pass@host:port
# Используется всеми парсерами (Playwright + requests).
#
# Настройка:
#   1. Подними SOCKS5-прокси с российским IP (например SSH-туннель):
#          ssh -D 1080 -N user@ru-server
#   2. Создай файл .parser_proxy в папке проекта:
#          socks5://127.0.0.1:1080
# ─────────────────────────────────────────────────────────────────────────────
_PROXY_FILE = Path(__file__).parent / ".parser_proxy"
PARSER_PROXY: str | None = (
    os.getenv("PARSER_PROXY")
    or (_PROXY_FILE.read_text().strip() if _PROXY_FILE.exists() else None)
)

if PARSER_PROXY:
    print(f"[proxy] Используется прокси: {PARSER_PROXY}")

# ── Аргументы Chromium ───────────────────────────────────────────────────────
# DNS-over-HTTPS через Google — обходит VPN-DNS, который не резолвит RU-домены.
# Используется всеми Playwright-парсерами (base + ozon).
# ─────────────────────────────────────────────────────────────────────────────
_CHROMIUM_ARGS = [
    # Критично для контейнеров: без этого Chromium пишет в /dev/shm (64 MB),
    # переполняет его и крашится с "Page crashed". Флаг переключает на /tmp.
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--dns-prefetch-disable",
    "--dns-over-https-mode=secure",
    "--dns-over-https-templates=https://dns.google/dns-query{?dns}",
]


# Ключевые слова для фильтрации нерелевантных товаров по категории.
# Если название товара не содержит НИ ОДНОГО слова из списка — это не тот товар.
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "GPU":    ["видеокарт", "geforce", "radeon", "rtx", "gtx", "arc a", "arc b", "intel arc"],
    "CPU":    ["процессор", "ryzen", "core i3", "core i5", "core i7", "core i9",
               "core ultra", "xeon", "athlon", "threadripper", "intel core"],
    "MB":     ["материнск"],
    "RAM":    ["dimm", "ddr3", "ddr4", "ddr5", "оперативн"],
    "SSD":    ["ssd", "nvme", "твердотельн"],
    "HDD":    ["hdd", "жёсткий диск", "жесткий диск"],
    "PSU":    ["блок питания"],
    "CASE":   ["корпус"],
    "COOLER": ["кулер", "охлажден", "вентилятор"],
}


def filter_by_category(products: list, category: str) -> list:
    """Удаляет товары, чьё название явно не соответствует категории."""
    keywords = _CATEGORY_KEYWORDS.get(category)
    if not keywords:
        return products
    filtered = []
    for p in products:
        name_lower = p.get("name", "").lower()
        if any(kw in name_lower for kw in keywords):
            filtered.append(p)
        else:
            print(f"[filter:{category}] Отфильтрован: {p.get('name', '')!r}")
    return filtered


def get_requests_proxies() -> dict | None:
    """Возвращает словарь proxies для requests.Session, или None."""
    if not PARSER_PROXY:
        return None
    # socks5h — резолвинг имён через прокси (важно для обхода VPN-DNS)
    url = PARSER_PROXY.replace("socks5://", "socks5h://")
    return {"http": url, "https": url}


class BaseParser(ABC):
    """Общий интерфейс для парсеров магазинов."""

    SOURCE_NAME = ""       # 'citilink', 'regard' — переопределить в наследнике
    CATALOG_URL = ""       # URL каталога — переопределить в наследнике
    BASE_URL = ""          # Базовый URL сайта
    CARD_SELECTOR = ""     # CSS-селектор карточки товара
    WAIT_TIMEOUT = 15000   # Таймаут ожидания карточек (мс)
    MAX_PAGES = 50         # Максимум страниц (защита от бесконечного цикла)
    DELAY_BETWEEN_PAGES = 3  # Задержка между страницами (секунды)
    BROWSER = "chromium"   # 'chromium' или 'firefox'
    _last_total = None     # Заполняется парсером если сайт возвращает total товаров

    def _create_browser(self, playwright):
        """Создаёт браузер и страницу со stealth."""
        launcher = getattr(playwright, self.BROWSER)
        browser = launcher.launch(
            headless=True,
            args=_CHROMIUM_ARGS,
        )

        context_opts = {
            "viewport": {"width": 1920, "height": 1080},
            "locale": "ru-RU",
        }
        if self.BROWSER == "chromium":
            context_opts["user_agent"] = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        if PARSER_PROXY:
            context_opts["proxy"] = {"server": PARSER_PROXY}

        context = browser.new_context(**context_opts)
        page = context.new_page()

        if self.BROWSER == "chromium":
            stealth = Stealth()
            stealth.apply_stealth_sync(page)

        return browser, page

    def _load_page(self, page, url):
        """Загружает страницу и ждёт появления карточек."""
        print(f"[{self.SOURCE_NAME}] Загружаю: {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
        except Exception as e:
            print(f"[{self.SOURCE_NAME}] goto timeout/error на {url}: {e}")
            # Ждём ещё и пробуем взять что уже загрузилось
            time.sleep(5)

        if self.CARD_SELECTOR:
            try:
                page.wait_for_selector(
                    self.CARD_SELECTOR,
                    timeout=self.WAIT_TIMEOUT,
                )
                time.sleep(2)
            except Exception:
                # Ждём ещё — WAF мог не успеть сделать редирект
                time.sleep(10)
                try:
                    page.wait_for_selector(self.CARD_SELECTOR, timeout=30000)
                    time.sleep(2)
                except Exception:
                    html_preview = page.content()[:300].replace("\n", " ")
                    print(f"[{self.SOURCE_NAME}] Карточки не появились на {url}")
                    print(f"[{self.SOURCE_NAME}] HTML preview: {html_preview}")

            # Скроллим вниз чтобы подгрузить все карточки (lazy loading)
            for _ in range(10):
                page.evaluate("window.scrollBy(0, 800)")
                time.sleep(0.4)
            time.sleep(1)
        else:
            time.sleep(self.DELAY_BETWEEN_PAGES)

        return page.content()

    BROWSER_RESTART_EVERY = 10  # перезапускать браузер каждые N страниц

    def fetch_all_pages(self):
        """Загружает все страницы каталога, возвращает список HTML."""
        all_html = []

        with sync_playwright() as p:
            browser, page = self._create_browser(p)
            try:
                # Загружаем первую страницу
                html = self._load_page(page, self.CATALOG_URL)
                all_html.append(html)

                # Определяем количество страниц
                total = self.get_total_pages(html)
                total = max(1, min(total, self.MAX_PAGES))
                print(f"[{self.SOURCE_NAME}] Страниц: {total}")

                # Загружаем остальные страницы
                for page_num in range(2, total + 1):
                    try:
                        # Перезапускаем браузер каждые BROWSER_RESTART_EVERY страниц
                        if (page_num - 1) % self.BROWSER_RESTART_EVERY == 0:
                            print(f"[{self.SOURCE_NAME}] Перезапуск браузера перед стр. {page_num}...")
                            browser.close()
                            browser, page = self._create_browser(p)

                        time.sleep(self.DELAY_BETWEEN_PAGES)
                        url = self.get_page_url(page_num)
                        html = self._load_page(page, url)
                        all_html.append(html)
                    except Exception as e:
                        print(f"[{self.SOURCE_NAME}] Ошибка на странице {page_num}: {e}")
                        continue
            finally:
                browser.close()

        return all_html

    @abstractmethod
    def parse_products(self, html):
        """Парсит HTML и возвращает список словарей с товарами."""
        ...

    def get_total_pages(self, html):
        """Определяет количество страниц. Переопределить в наследнике."""
        return 1

    def get_page_url(self, page_num):
        """URL для конкретной страницы. Переопределить в наследнике."""
        return self.CATALOG_URL

    def run(self):
        """Запускает парсер: скачать все страницы → распарсить → вернуть товары."""
        all_html = self.fetch_all_pages()
        products = []
        for html in all_html:
            products.extend(self.parse_products(html))
        print(f"[{self.SOURCE_NAME}] Найдено товаров: {len(products)}")
        return products
