"""
Веб-интерфейс агрегатора цен — FastAPI + Jinja2.

Запуск:
    python -m uvicorn web_app:app --reload
    или
    python web_app.py
"""

import collections
import json
import logging
import os
import re
import secrets
import time
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPool

import database as db
from auth import hash_password, verify_password, get_current_user

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]{2,}$')

# ------------------------------------------------------------------ #
# CSRF helpers                                                         #
# ------------------------------------------------------------------ #

def _csrf_token(request: Request) -> str:
    """Return (and lazily create) the CSRF token stored in the session."""
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_hex(32)
        request.session["csrf_token"] = token
    return token


def _check_csrf(request: Request, token: str) -> None:
    expected = request.session.get("csrf_token")
    if not expected or not secrets.compare_digest(expected, token or ""):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


# ------------------------------------------------------------------ #
# Rate limiters                                                        #
# ------------------------------------------------------------------ #

class _RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self._max = max_calls
        self._period = period
        self._hits: dict = collections.defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        self._hits[key] = [t for t in self._hits[key] if now - t < self._period]
        if len(self._hits[key]) >= self._max:
            return False
        self._hits[key].append(now)
        return True


_login_limiter = _RateLimiter(5, 300)     # 5 attempts per 5 min per IP
_register_limiter = _RateLimiter(3, 3600)  # 3 registrations per hour per IP


def _client_ip(request: Request) -> str:
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback")

BASE_DIR = Path(__file__).parent

_SECRET_FILE = BASE_DIR / ".session_secret"

_SESSION_SECRET = os.getenv("SESSION_SECRET")
if not _SESSION_SECRET:
    if _SECRET_FILE.exists():
        _SESSION_SECRET = _SECRET_FILE.read_text().strip()
    else:
        _SESSION_SECRET = secrets.token_hex(32)
        _SECRET_FILE.write_text(_SESSION_SECRET)
        logging.info("SESSION_SECRET сгенерирован и сохранён в .session_secret")

# ------------------------------------------------------------------ #
# Scheduler — автоматический запуск парсеров по расписанию            #
# ------------------------------------------------------------------ #

_scheduler = BackgroundScheduler(
    executors={"default": APSThreadPool(max_workers=4)},
    job_defaults={"coalesce": True, "max_instances": 1},
)

# Расписание: (job_id, алиасы парсеров, интервал в часах)
_SCHEDULE = [
    ("job_mvideo",      ["mvideo-all"],      4),
    ("job_wb",          ["wb-all"],          6),
    ("job_regard",      ["regard-all"],      6),
    ("job_oldi",        ["oldi-all"],        6),
    ("job_citilink",    ["citilink-all"],    12),
    ("job_eldorado",    ["eldorado-all"],    12),
    ("job_key",         ["key-all"],         8),
    ("job_kns",         ["kns-all"],         6),
    ("job_fcenter",     ["fcenter-all"],     12),
]


def _make_job(parser_keys: list):
    """Возвращает callable для APScheduler."""
    def _job():
        from main import run_parsers, _ALL_ALIASES
        expanded = []
        for k in parser_keys:
            expanded.extend(_ALL_ALIASES.get(k, [k]))
        try:
            run_parsers(expanded)
        except Exception as e:
            logging.error(f"Scheduler error for {parser_keys}: {e}")
    return _job


@asynccontextmanager
async def lifespan(app_: FastAPI):
    # Startup
    db.init_db()
    from datetime import datetime, timedelta
    for job_id, keys, hours in _SCHEDULE:
        first_run = datetime.now() + timedelta(minutes=2)
        _scheduler.add_job(
            _make_job(keys), "interval", hours=hours,
            id=job_id, replace_existing=True,
            next_run_time=first_run,
        )
    _scheduler.start()
    logging.info("Scheduler запущен. Следующие запуски: %s",
                 {j.id: str(j.next_run_time) for j in _scheduler.get_jobs()})
    yield
    # Shutdown
    _scheduler.shutdown(wait=False)


app = FastAPI(title="PC Parts Aggregator", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=_SESSION_SECRET)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def render(request, name, context=None):
    ctx = context or {}
    ctx["user"] = get_current_user(request)
    ctx["cart_count"] = sum(i.get("qty", 0) for i in request.session.get("cart", []))
    ctx["google_enabled"] = bool(GOOGLE_CLIENT_ID)
    ctx["csrf_token"] = _csrf_token(request)
    return templates.TemplateResponse(request=request, name=name, context=ctx)


def safe_back(url: str) -> str:
    """Разрешает редирект только на относительные пути (защита от open redirect)."""
    if url and url.startswith("/") and not url.startswith("//"):
        return url
    return "/"


def get_cart(request):
    return request.session.get("cart", [])


def save_cart(request, cart):
    request.session["cart"] = cart


# ==================== СТРАНИЦЫ ====================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    stats = db.get_stats()
    store_counts = db.get_store_offer_counts()
    return render(request, "index.html", {"stats": stats, "store_counts": store_counts})


@app.get("/pricing", response_class=HTMLResponse)
async def pricing_page(request: Request):
    return render(request, "pricing.html")


CATEGORY_LABELS = {
    "GPU":    "Видеокарты",
    "CPU":    "Процессоры",
    "MB":     "Материнские платы",
    "RAM":    "Оперативная память",
    "SSD":    "SSD накопители",
    "HDD":    "Жёсткие диски",
    "PSU":    "Блоки питания",
    "CASE":   "Корпуса",
    "COOLER": "Кулеры",
}

# Метка для дропдауна моделей/чипов по категории
_CHIP_LABEL = {
    "GPU":    "Все GPU",
    "CPU":    "Все процессоры",
    "MB":     "Все чипсеты",
    "RAM":    "Все типы RAM",
    "SSD":    "Все типы SSD",
    "HDD":    "Все типы HDD",
    "PSU":    "Все блоки",
    "CASE":   "Все корпуса",
    "COOLER": "Все кулеры",
}


@app.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = Query(default=None),
    brand: str = Query(default=None),
    chip: str = Query(default=None),
    source: str = Query(default=None),
    category: str = Query(default=None),
):
    sources = [source] if source else None
    results = db.search_products(query=q, brand=brand, chip=chip, sources=sources, category=category)
    filters = db.get_filter_options(category=category)
    return render(request, "search_results.html", {
        "query": q,
        "brand": brand,
        "chip": chip,
        "source": source,
        "category": category,
        "results": results,
        "filters": filters,
        "category_labels": CATEGORY_LABELS,
        "chip_label": _CHIP_LABEL.get(category, "Все GPU"),
    })


@app.get("/product/{slug}", response_class=HTMLResponse)
async def product_detail(request: Request, slug: str):
    product = db.get_product_by_slug_with_offers(slug)
    if not product:
        return RedirectResponse(url="/", status_code=302)
    return render(request, "product.html", {"product": product})


@app.get("/api/price-history/{product_id}")
async def price_history_api(product_id: int):
    history = db.get_price_history_for_product(product_id)
    return JSONResponse(history)


# ==================== АВТОРИЗАЦИЯ ====================

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return render(request, "register.html")


@app.post("/register", response_class=HTMLResponse)
async def register_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(default=""),
):
    _check_csrf(request, csrf_token)
    ip = _client_ip(request)
    if not _register_limiter.is_allowed(ip):
        return render(request, "register.html", {"error": "Слишком много попыток. Попробуйте позже."})
    email = email.strip().lower()
    if not _EMAIL_RE.match(email):
        return render(request, "register.html", {"error": "Введите корректный email"})
    if len(password) < 6:
        return render(request, "register.html", {"error": "Пароль минимум 6 символов"})

    user_id = db.create_user(email, hash_password(password))
    if not user_id:
        return render(request, "register.html", {"error": "Этот email уже зарегистрирован"})

    request.session["user_id"] = user_id
    return RedirectResponse(url="/", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return render(request, "login.html")


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(default=""),
):
    _check_csrf(request, csrf_token)
    ip = _client_ip(request)
    if not _login_limiter.is_allowed(ip):
        return render(request, "login.html", {"error": "Слишком много попыток. Подождите 5 минут."})
    email = email.strip().lower()
    user = db.get_user_by_email(email)
    if not user or not verify_password(password, user["password"]):
        return render(request, "login.html", {"error": "Неверный email или пароль"})

    request.session["user_id"] = user["id"]
    return RedirectResponse(url="/", status_code=302)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)


# ==================== ПРОФИЛЬ ====================

@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    if not request.session.get("user_id"):
        return RedirectResponse(url="/login", status_code=302)
    return render(request, "profile.html", {})


@app.post("/profile", response_class=HTMLResponse)
async def profile_update(
    request: Request,
    username: str = Form(default=""),
    csrf_token: str = Form(default=""),
):
    _check_csrf(request, csrf_token)
    if not request.session.get("user_id"):
        return RedirectResponse(url="/login", status_code=302)
    user = get_current_user(request)
    db.update_user_profile(user["id"], username=username.strip() or None)
    return render(request, "profile.html", {"success": "Профиль сохранён"})


@app.post("/profile/delete")
async def profile_delete(request: Request, csrf_token: str = Form(default="")):
    _check_csrf(request, csrf_token)
    if not request.session.get("user_id"):
        return RedirectResponse(url="/login", status_code=302)
    user = get_current_user(request)
    db.delete_user(user["id"])
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)


# ==================== КОРЗИНА ====================

@app.get("/cart", response_class=HTMLResponse)
async def cart_page(request: Request):
    cart = get_cart(request)
    product_ids = [item["product_id"] for item in cart]
    products_map = db.get_products_with_offers_bulk(product_ids)
    enriched = []
    total = 0
    for item in cart:
        product = products_map.get(item["product_id"])
        if not product:
            continue
        best_price = product["offers"][0]["price"] if product["offers"] else 0
        best_source = product["offers"][0]["source"] if product["offers"] else "—"
        subtotal = best_price * item["qty"]
        total += subtotal
        enriched.append({
            "product_id": item["product_id"],
            "slug": product.get("slug") or str(item["product_id"]),
            "name": product["name"],
            "qty": item["qty"],
            "best_price": best_price,
            "best_source": best_source,
            "subtotal": subtotal,
        })
    return render(request, "cart.html", {"cart": enriched, "total": total})


@app.post("/cart/add")
async def cart_add(
    request: Request,
    product_id: int = Form(...),
    back: str = Form(default="/"),
    csrf_token: str = Form(default=""),
):
    _check_csrf(request, csrf_token)
    cart = get_cart(request)
    for item in cart:
        if item["product_id"] == product_id:
            item["qty"] += 1
            save_cart(request, cart)
            return RedirectResponse(url=safe_back(back), status_code=302)
    cart.append({"product_id": product_id, "qty": 1})
    save_cart(request, cart)
    return RedirectResponse(url=safe_back(back), status_code=302)


@app.post("/cart/remove")
async def cart_remove(
    request: Request,
    product_id: int = Form(...),
    csrf_token: str = Form(default=""),
):
    _check_csrf(request, csrf_token)
    cart = [i for i in get_cart(request) if i["product_id"] != product_id]
    save_cart(request, cart)
    return RedirectResponse(url="/cart", status_code=302)


@app.post("/cart/qty")
async def cart_qty(
    request: Request,
    product_id: int = Form(...),
    action: str = Form(...),
    csrf_token: str = Form(default=""),
):
    _check_csrf(request, csrf_token)
    cart = get_cart(request)
    if action not in ("inc", "dec"):
        return RedirectResponse(url="/cart", status_code=302)
    for item in cart:
        if item["product_id"] == product_id:
            if action == "inc":
                item["qty"] += 1
            else:
                item["qty"] = max(1, item["qty"] - 1)
            break
    save_cart(request, cart)
    return RedirectResponse(url="/cart", status_code=302)


@app.post("/cart/clear")
async def cart_clear(request: Request, csrf_token: str = Form(default="")):
    _check_csrf(request, csrf_token)
    save_cart(request, [])
    return RedirectResponse(url="/cart", status_code=302)


# ==================== GOOGLE OAUTH ====================

@app.get("/auth/google")
async def auth_google(request: Request, link: int = 0):
    if not GOOGLE_CLIENT_ID:
        return RedirectResponse(url="/profile", status_code=302)
    state = secrets.token_hex(16)
    request.session["oauth_state"] = state
    request.session["google_action"] = "link" if link else "login"
    params = urllib.parse.urlencode({
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "state": state,
    })
    return RedirectResponse(url=f"https://accounts.google.com/o/oauth2/v2/auth?{params}")


@app.get("/auth/google/callback")
async def auth_google_callback(
    request: Request,
    code: str = None,
    error: str = None,
    state: str = None,
):
    expected_state = request.session.pop("oauth_state", None)
    if error or not code or not expected_state or not secrets.compare_digest(expected_state, state or ""):
        return RedirectResponse(url="/login?error=google", status_code=302)

    # Обмен code на токен
    try:
        data = urllib.parse.urlencode({
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        }).encode()
        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token", data=data, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            token = json.loads(resp.read())

        req2 = urllib.request.Request(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {token['access_token']}"},
        )
        with urllib.request.urlopen(req2) as resp:
            guser = json.loads(resp.read())
    except Exception:
        return RedirectResponse(url="/login?error=google", status_code=302)

    google_id = guser["id"]
    email = guser.get("email", "")
    action = request.session.pop("google_action", "login")

    if action == "link":
        user = get_current_user(request)
        if user:
            db.link_google_account(user["id"], google_id)
        return RedirectResponse(url="/profile", status_code=302)

    # Логин или регистрация
    existing = db.get_user_by_google_id(google_id)
    if existing:
        request.session["user_id"] = existing["id"]
        return RedirectResponse(url="/", status_code=302)

    user_id = db.create_user_google(email, google_id)
    if not user_id:
        # Аккаунт с таким email уже есть — предлагаем войти и привязать
        return RedirectResponse(url="/login?hint=google", status_code=302)

    request.session["user_id"] = user_id
    return RedirectResponse(url="/", status_code=302)


# ------------------------------------------------------------------ #
# Аудит парсеров                                                       #
# ------------------------------------------------------------------ #

@app.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    summary = db.get_audit_summary()
    runs = db.get_parse_runs(limit=50)
    return render(request, "audit.html", {"summary": summary, "runs": runs})


@app.post("/api/run-parser")
async def run_parser_manual(request: Request,
                             background_tasks: BackgroundTasks,
                             parser_key: str = Form(...)):
    from main import PARSERS, _ALL_ALIASES
    if parser_key not in PARSERS and parser_key not in _ALL_ALIASES:
        return JSONResponse({"error": "Unknown parser key"}, status_code=400)
    background_tasks.add_task(_make_job([parser_key]))
    return JSONResponse({"status": "queued", "parser_key": parser_key})


@app.get("/api/scheduler/status")
async def scheduler_status():
    jobs = [{"id": j.id, "next_run": str(j.next_run_time)}
            for j in _scheduler.get_jobs()]
    return JSONResponse({"running": _scheduler.running, "jobs": jobs})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web_app:app", host="localhost", port=8000, reload=True)
