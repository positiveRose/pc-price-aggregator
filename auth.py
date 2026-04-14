"""
Аутентификация — хеширование паролей и сессии.
"""

import hashlib
import os

import database as db


def hash_password(password):
    salt = os.urandom(16).hex()
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000).hex()
    return f"{salt}${h}"


def verify_password(password, stored):
    if not stored or "$" not in stored:
        return False  # Google-аккаунт без пароля
    salt, h = stored.split("$", 1)
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000).hex() == h


def get_current_user(request):
    user_id = request.session.get("user_id")
    if user_id:
        return db.get_user_by_id(user_id)
    return None
