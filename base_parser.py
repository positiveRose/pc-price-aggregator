"""
Базовый класс для всех парсеров магазинов.

Каждый парсер наследует BaseParser и реализует:
- SOURCE_NAME — название магазина для БД
- CATALOG_URL — URL каталога
- parse_products(html) — извлечение товаров из HTML
- get_total_pages(html) — определение числа страниц
- get_page_url(page_num) — URL конкретной страницы
"""

import time
from abc import ABC, abstractmethod

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth


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

    def _create_browser(self, playwright):
        """Создаёт браузер и страницу со stealth."""
        launcher = getattr(playwright, self.BROWSER)
        browser = launcher.launch(
            headless=True,
            args=[
                # Используем системный DNS-резолвер вместо встроенного DoH
                "--disable-features=SecureDns",
                "--dns-prefetch-disable",
            ],
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

        context = browser.new_context(**context_opts)
        page = context.new_page()

        if self.BROWSER == "chromium":
            stealth = Stealth()
            stealth.apply_stealth_sync(page)

        return browser, page

    def _load_page(self, page, url):
        """Загружает страницу и ждёт появления карточек."""
        print(f"[{self.SOURCE_NAME}] Загружаю: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        if self.CARD_SELECTOR:
            try:
                page.wait_for_selector(
                    self.CARD_SELECTOR,
                    timeout=self.WAIT_TIMEOUT,
                )
                time.sleep(2)
            except Exception:
                print(f"[{self.SOURCE_NAME}] Карточки не появились, жду ещё...")
                time.sleep(10)
        else:
            time.sleep(3)

        return page.content()

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
