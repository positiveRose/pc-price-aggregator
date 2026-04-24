# PC Parts Price Aggregator

Агрегатор цен на комплектующие для ПК — парсит 7 крупных российских магазинов, сравнивает цены на одинаковые товары и показывает лучшие предложения.

## Возможности

- **Парсинг 7 магазинов** — Ситилинк, Регард, OLDI, e2e4, МВидео, Эльдорадо, Wildberries
- **18 000+ товаров** в базе данных (GPU, CPU, MB, RAM, SSD, HDD, PSU, корпуса, кулеры)
- **Умный матчинг** — находит одинаковые товары в разных магазинах без LLM, сравнивает цены
- **Поиск и фильтры** — по категории, бренду, магазину, текстовый поиск с поддержкой склонений
- **История цен** — график изменения цены по каждому товару
- **Автопарсинг** — планировщик автоматически обновляет цены по расписанию
- **Авторизация** — регистрация, вход, Google OAuth
- **Корзина** — добавление товаров и сравнение итоговой стоимости
- **Аудит парсеров** — страница мониторинга запусков, статусов и покрытия

## Стек технологий

| Слой | Технологии |
|------|-----------|
| Backend | Python 3.11, FastAPI, Uvicorn |
| БД | SQLite (WAL mode), параметризованные запросы |
| Парсинг | Playwright + playwright-stealth, BeautifulSoup4, Requests |
| Планировщик | APScheduler |
| Авторизация | bcrypt, сессии (Starlette SessionMiddleware), Google OAuth 2.0 |
| Frontend | Jinja2, CSS (тёмная тема) |

## Архитектура

```
main.py                  — CLI запуск парсеров
parser_*.py              — парсеры магазинов (BaseParser + конкретные реализации)
base_parser.py           — базовый класс: Playwright, stealth, DNS-fallback, прокси
matcher.py               — матчинг товаров между магазинами (GPU/CPU/MB/RAM/SSD/HDD/PSU)
database.py              — SQLite: товары, офферы, история цен, пользователи, аудит
web_app.py               — FastAPI веб-интерфейс + scheduler
auth.py                  — bcrypt, сессии
templates/               — Jinja2 шаблоны
static/                  — CSS
```

## Быстрый старт

```bash
# Клонировать репозиторий
git clone https://github.com/YOUR_USERNAME/pc-price-aggregator
cd pc-price-aggregator

# Установить зависимости
pip install -r requirements.txt
playwright install chromium

# Запустить веб-сервер
python main.py --web
# Открыть http://localhost:8000

# Запустить парсинг (все магазины)
python main.py

# Запустить конкретный магазин
python main.py citilink
python main.py wb

# Запустить матчинг товаров
python main.py --match
```

## Переменные окружения

| Переменная | Описание |
|-----------|---------|
| `SESSION_SECRET` | Секрет для сессий (генерируется автоматически) |
| `GOOGLE_CLIENT_ID` | Google OAuth Client ID (опционально) |
| `GOOGLE_CLIENT_SECRET` | Google OAuth Client Secret (опционально) |
| `PARSER_PROXY` | SOCKS5/HTTP прокси для парсеров (опционально) |

Или создай файл `.parser_proxy` с адресом прокси: `socks5://127.0.0.1:1080`

## Интересные технические решения

**DNS-fallback через 8.8.8.8** — при конфликте VPN и DNS патчим `socket.getaddrinfo` и делаем UDP-запрос напрямую к Google DNS, без сторонних библиотек.

**Матчинг без LLM** — regex-извлечение характеристик (чип, память, частота, ёмкость) + группировка по составному ключу. 1000+ совпадений на 17k товаров.

**Stealth Playwright** — обход базового обнаружения headless-браузера через `playwright-stealth`.

**Bulk-запросы в БД** — корзина и поиск загружают все связанные офферы за 3 SQL-запроса вместо N+1.

## Скриншоты

> Добавь скриншоты интерфейса сюда

## Лицензия

MIT
