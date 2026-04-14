"""
Веб-интерфейс агрегатора цен — FastAPI + Jinja2.

Запуск:
    python -m uvicorn web_app:app --reload
    или
    python web_app.py
"""

import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import FastAPI, Request, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

import database as db
from auth import hash_password, verify_password, get_current_user

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback")

BASE_DIR = Path(__file__).parent

app = FastAPI(title="PC Parts Aggregator")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv(
        "SESSION_SECRET",
        "f7a3b2c9e1d04f58a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9",
    ),
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Инициализируем БД при старте
db.init_db()


def render(request, name, context=None):
    ctx = context or {}
    ctx["user"] = get_current_user(request)
    ctx["cart_count"] = sum(i.get("qty", 0) for i in request.session.get("cart", []))
    ctx["google_enabled"] = bool(GOOGLE_CLIENT_ID)
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
    return render(request, "index.html")


@app.get("/pricing", response_class=HTMLResponse)
async def pricing_page(request: Request):
    return render(request, "pricing.html")


@app.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = Query(default=None),
    brand: str = Query(default=None),
    chip: str = Query(default=None),
    source: str = Query(default=None),
):
    sources = [source] if source else None
    results = db.search_products(query=q, brand=brand, chip=chip, sources=sources)
    filters = db.get_filter_options()
    return render(request, "search_results.html", {
        "query": q,
        "brand": brand,
        "chip": chip,
        "source": source,
        "results": results,
        "filters": filters,
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
):
    email = email.strip().lower()
    if not email or "@" not in email:
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
):
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
):
    if not request.session.get("user_id"):
        return RedirectResponse(url="/login", status_code=302)
    user = get_current_user(request)
    db.update_user_profile(user["id"], username=username.strip() or None)
    return render(request, "profile.html", {"success": "Профиль сохранён"})


@app.post("/profile/delete")
async def profile_delete(request: Request):
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
    enriched = []
    total = 0
    for item in cart:
        product = db.get_product_with_offers(item["product_id"])
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
):
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
async def cart_remove(request: Request, product_id: int = Form(...)):
    cart = [i for i in get_cart(request) if i["product_id"] != product_id]
    save_cart(request, cart)
    return RedirectResponse(url="/cart", status_code=302)


@app.post("/cart/qty")
async def cart_qty(
    request: Request,
    product_id: int = Form(...),
    action: str = Form(...),
):
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
async def cart_clear(request: Request):
    save_cart(request, [])
    return RedirectResponse(url="/cart", status_code=302)


# ==================== GOOGLE OAUTH ====================

@app.get("/auth/google")
async def auth_google(request: Request, link: int = 0):
    if not GOOGLE_CLIENT_ID:
        return RedirectResponse(url="/profile", status_code=302)
    request.session["google_action"] = "link" if link else "login"
    params = urllib.parse.urlencode({
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
    })
    return RedirectResponse(url=f"https://accounts.google.com/o/oauth2/v2/auth?{params}")


@app.get("/auth/google/callback")
async def auth_google_callback(request: Request, code: str = None, error: str = None):
    if error or not code:
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web_app:app", host="localhost", port=8000, reload=True)
