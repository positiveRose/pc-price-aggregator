"""
Матчинг товаров между магазинами без LLM.

Tier 1 — Код модели (~85%):
  Регард хранит код в скобках: "... MSI OC 12GB (RTX 5070 12G SHADOW 3X OC)"
  Ищем этот код в названии Ситилинка.

Tier 2 — Атрибуты (~10%):
  Извлекаем brand + gpu_chip + memory, матчим по совпадению.

Запуск:
    python matcher.py          — запустить матчинг и показать статистику
"""

import re

import database as db


# ==================== ИЗВЛЕЧЕНИЕ АТРИБУТОВ ====================

BRAND_PATTERN = re.compile(
    r"\b(ASUS|MSI|Gigabyte|Palit|Sapphire|ASRock|AFOX|NINJA|Zotac|"
    r"EVGA|Colorful|Inno3D|PNY|KFA2|Biostar|PowerColor|XFX|Maxsun|ARKTEK)\b",
    re.IGNORECASE,
)

GPU_CHIP_PATTERN = re.compile(
    r"((?:GeForce\s+)?(?:GT\s*\d{3,4}|GTX\s*\d{3,4}|RTX\s*\d{4,5})(?:\s*(?:Ti|SUPER))?)|"
    r"((?:Radeon\s+)?(?:RX\s*\d{3,5}|R[579]\s+\d{3})(?:\s*(?:XT|XTX))?)|"
    r"(ARC\s+A\d{3,4})|"
    r"(Quadro\s+\w+)",
    re.IGNORECASE,
)

MEMORY_PATTERN = re.compile(r"(\d+)\s*(?:\u0413\u0411|GB|G\b)", re.IGNORECASE)


def extract_model_code(name):
    """Извлекает код модели из скобок (формат Регарда)."""
    m = re.search(r"\(([^)]+)\)", name)
    return m.group(1).strip() if m else None


def extract_brand(name):
    m = BRAND_PATTERN.search(name)
    if m:
        brand = m.group(1)
        # Нормализуем регистр
        brand_map = {
            "asus": "ASUS", "msi": "MSI", "gigabyte": "Gigabyte",
            "palit": "Palit", "sapphire": "Sapphire", "asrock": "ASRock",
            "afox": "AFOX", "ninja": "NINJA", "zotac": "Zotac",
            "evga": "EVGA", "colorful": "Colorful", "inno3d": "Inno3D",
            "pny": "PNY", "kfa2": "KFA2", "biostar": "Biostar",
            "powercolor": "PowerColor", "xfx": "XFX", "maxsun": "Maxsun",
            "arktek": "ARKTEK",
        }
        return brand_map.get(brand.lower(), brand)
    return None


def extract_gpu_chip(name):
    """Извлекает GPU чип, нормализует: 'GeForce RTX 5070' → 'RTX 5070'."""
    m = GPU_CHIP_PATTERN.search(name)
    if not m:
        return None
    chip = m.group(0).strip()
    # Убираем 'GeForce ' и 'Radeon ' для унификации
    chip = re.sub(r"^GeForce\s+", "", chip, flags=re.IGNORECASE)
    chip = re.sub(r"^Radeon\s+", "", chip, flags=re.IGNORECASE)
    # Нормализуем пробелы: "RTX5070" → "RTX 5070"
    chip = re.sub(r"(RTX|GTX|GT|RX|R[579]|ARC)\s*(\d)", r"\1 \2", chip, flags=re.IGNORECASE)
    return chip.upper().strip()


def extract_memory(name):
    """Извлекает объём памяти в ГБ."""
    m = MEMORY_PATTERN.search(name)
    return int(m.group(1)) if m else None


# ==================== МАТЧИНГ ====================

def run_matching():
    """Запускает матчинг товаров между магазинами.

    Tier 1 — код модели из скобок Регарда (citilink ↔ regard).
    Tier 2 — brand + chip + memory по всем магазинам.
    Tier 3 — chip + memory (без бренда) по всем магазинам — ловит
             случаи когда бренд распознан по-разному.
    """
    conn = db.get_connection()
    try:
        # Загружаем все товары с источниками (по одному offer на продукт)
        rows = conn.execute("""
            SELECT p.id, p.name, o.source
            FROM products p
            JOIN offers o ON o.product_id = p.id
            GROUP BY p.id
        """).fetchall()

        products = [dict(r) for r in rows]

        # Очищаем старые результаты матчинга
        conn.execute("UPDATE products SET canonical_id = NULL")

        # Заполняем атрибуты для всех товаров
        for p in products:
            brand = extract_brand(p["name"])
            chip = extract_gpu_chip(p["name"])
            fields, params = [], []
            if brand:
                fields.append("brand = ?")
                params.append(brand)
            if chip:
                fields.append("model = ?")
                params.append(chip)
            if fields:
                params.append(p["id"])
                conn.execute(
                    f"UPDATE products SET {', '.join(fields)} WHERE id = ?",
                    params,
                )

        # -------- Tier 1: код модели из скобок Регарда (citilink ↔ regard) --------
        citilink = [p for p in products if p["source"] == "citilink"]
        regard   = [p for p in products if p["source"] == "regard"]

        print(f"Ситилинк: {len(citilink)} | Регард: {len(regard)}")
        print(f"Всего источников: {len(set(p['source'] for p in products))}")

        matched_ids = set()   # id товаров, уже включённых в группу (не canonical)
        matched_tier1 = 0
        matched_tier2 = 0
        matched_tier3 = 0

        regard_codes = {}
        for r in regard:
            code = extract_model_code(r["name"])
            if code:
                regard_codes[r["id"]] = (code, r)

        for c in citilink:
            c_name_upper = c["name"].upper()
            for r_id, (code, r) in regard_codes.items():
                if r_id in matched_ids:
                    continue
                if code.upper() in c_name_upper:
                    canonical_id = min(c["id"], r["id"])
                    other_id    = max(c["id"], r["id"])
                    conn.execute(
                        "UPDATE products SET canonical_id = ? WHERE id = ?",
                        (canonical_id, other_id),
                    )
                    matched_ids.add(other_id)
                    matched_tier1 += 1
                    break

        # -------- Tier 2: brand + chip + memory — все магазины --------
        # Группируем по сигнатуре: (BRAND_UPPER, CHIP_UPPER, memory_gb)
        sig_map: dict = {}
        for p in products:
            brand = extract_brand(p["name"])
            chip  = extract_gpu_chip(p["name"])
            mem   = extract_memory(p["name"])
            if brand and chip and mem:
                sig = (brand.upper(), chip.upper(), int(mem))
                sig_map.setdefault(sig, []).append(p)

        for sig, group in sig_map.items():
            # Берём только товары из разных источников, ещё не сматченных
            by_source: dict = {}
            for p in group:
                if p["id"] not in matched_ids:
                    by_source.setdefault(p["source"], []).append(p)

            if len(by_source) < 2:
                continue  # один магазин — нечего объединять

            # Выбираем canonical (наименьший id из первых представителей каждого источника)
            representatives = [prods[0] for prods in by_source.values()]
            canonical_id = min(p["id"] for p in representatives)

            for p in representatives:
                if p["id"] != canonical_id:
                    conn.execute(
                        "UPDATE products SET canonical_id = ? WHERE id = ?",
                        (canonical_id, p["id"]),
                    )
                    matched_ids.add(p["id"])
                    matched_tier2 += 1

        # -------- Tier 3: chip + memory (без бренда) — подбираем оставшихся --------
        sig_map2: dict = {}
        for p in products:
            if p["id"] in matched_ids:
                continue
            chip = extract_gpu_chip(p["name"])
            mem  = extract_memory(p["name"])
            if chip and mem:
                sig = (chip.upper(), int(mem))
                sig_map2.setdefault(sig, []).append(p)

        for sig, group in sig_map2.items():
            by_source2: dict = {}
            for p in group:
                if p["id"] not in matched_ids:
                    by_source2.setdefault(p["source"], []).append(p)

            if len(by_source2) < 2:
                continue

            representatives = [prods[0] for prods in by_source2.values()]
            canonical_id = min(p["id"] for p in representatives)

            for p in representatives:
                if p["id"] != canonical_id:
                    conn.execute(
                        "UPDATE products SET canonical_id = ? WHERE id = ?",
                        (canonical_id, p["id"]),
                    )
                    matched_ids.add(p["id"])
                    matched_tier3 += 1

        conn.commit()

        total = matched_tier1 + matched_tier2 + matched_tier3
        print(f"\nРезультаты матчинга:")
        print(f"  Tier 1 (код Регарда):       {matched_tier1} совпадений")
        print(f"  Tier 2 (brand+chip+memory): {matched_tier2} совпадений")
        print(f"  Tier 3 (chip+memory):       {matched_tier3} совпадений")
        print(f"  Итого:                      {total} совпадений")

        return total
    finally:
        conn.close()


if __name__ == "__main__":
    db.init_db()
    run_matching()
