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
    """Запускает матчинг товаров между магазинами."""
    conn = db.get_connection()
    try:
        # Загружаем все товары с источниками
        rows = conn.execute("""
            SELECT p.id, p.name, o.source
            FROM products p
            JOIN offers o ON o.product_id = p.id
        """).fetchall()

        products = [dict(r) for r in rows]

        # Очищаем старые результаты матчинга
        conn.execute("UPDATE products SET canonical_id = NULL")

        # Разделяем по источникам
        citilink = [p for p in products if p["source"] == "citilink"]
        regard = [p for p in products if p["source"] == "regard"]

        print(f"Ситилинк: {len(citilink)} товаров")
        print(f"Регард: {len(regard)} товаров")

        # Сначала заполняем атрибуты для всех товаров
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

        # Tier 1: код модели из скобок Регарда
        matched_tier1 = 0
        matched_tier2 = 0
        matched_citilink_ids = set()
        matched_regard_ids = set()

        regard_codes = {}
        for r in regard:
            code = extract_model_code(r["name"])
            if code:
                regard_codes[r["id"]] = (code, r)

        for c in citilink:
            c_name_upper = c["name"].upper()
            for r_id, (code, r) in regard_codes.items():
                if r_id in matched_regard_ids:
                    continue
                if code.upper() in c_name_upper:
                    # Совпадение! Регард → canonical = Ситилинк (у кого id меньше)
                    canonical_id = min(c["id"], r["id"])
                    other_id = max(c["id"], r["id"])
                    conn.execute(
                        "UPDATE products SET canonical_id = ? WHERE id = ?",
                        (canonical_id, other_id),
                    )
                    matched_citilink_ids.add(c["id"])
                    matched_regard_ids.add(r_id)
                    matched_tier1 += 1
                    break

        # Tier 2: brand + chip + memory для оставшихся
        unmatched_citilink = [c for c in citilink if c["id"] not in matched_citilink_ids]
        unmatched_regard = [r for r in regard if r["id"] not in matched_regard_ids]

        for c in unmatched_citilink:
            c_brand = extract_brand(c["name"])
            c_chip = extract_gpu_chip(c["name"])
            c_mem = extract_memory(c["name"])
            if not (c_brand and c_chip):
                continue

            for r in unmatched_regard:
                if r["id"] in matched_regard_ids:
                    continue
                r_brand = extract_brand(r["name"])
                r_chip = extract_gpu_chip(r["name"])
                r_mem = extract_memory(r["name"])

                if c_brand == r_brand and c_chip == r_chip and c_mem and c_mem == r_mem:
                    canonical_id = min(c["id"], r["id"])
                    other_id = max(c["id"], r["id"])
                    conn.execute(
                        "UPDATE products SET canonical_id = ? WHERE id = ?",
                        (canonical_id, other_id),
                    )
                    matched_regard_ids.add(r["id"])
                    matched_tier2 += 1
                    break

        conn.commit()

        print(f"\nРезультаты матчинга:")
        print(f"  Tier 1 (код модели): {matched_tier1} совпадений")
        print(f"  Tier 2 (атрибуты):   {matched_tier2} совпадений")
        print(f"  Итого:               {matched_tier1 + matched_tier2} совпадений")
        print(f"  Без пары (Ситилинк): {len(citilink) - matched_tier1 - matched_tier2}")
        print(f"  Без пары (Регард):   {len(regard) - matched_tier1 - matched_tier2}")

        return matched_tier1 + matched_tier2
    finally:
        conn.close()


if __name__ == "__main__":
    db.init_db()
    run_matching()
