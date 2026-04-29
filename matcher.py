"""
Матчинг товаров между магазинами без LLM.

GPU Tier 1 — Код модели (~85%):
  Регард хранит код в скобках: "... MSI OC 12GB (RTX 5070 12G SHADOW 3X OC)"
  Ищем этот код в названии Ситилинка.

GPU Tier 2 — brand + chip + memory по всем магазинам.
GPU Tier 3 — chip + memory (без бренда).

CPU    — cpu_brand + model_code (Core i9-14900K / Ryzen 9 7950X).
MB     — gpu_brand + chipset + form_factor.
RAM    — ddr_type + speed_mhz + capacity_gb.
SSD    — storage_brand + capacity_gb + interface.
HDD    — storage_brand + capacity_gb.
PSU    — wattage_w + cert_tier.

Запуск:
    python matcher.py          — запустить матчинг и показать статистику
"""

import re
from collections import defaultdict

import database as db


# ==================== GPU ====================

GPU_BRAND_PATTERN = re.compile(
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

MEMORY_GB_PATTERN = re.compile(r"(\d+)\s*(?:ГБ|GB|G\b)", re.IGNORECASE)


def extract_model_code(name):
    """Извлекает код модели из скобок (формат Регарда)."""
    m = re.search(r"\(([^)]+)\)", name)
    return m.group(1).strip() if m else None


def extract_gpu_brand(name):
    m = GPU_BRAND_PATTERN.search(name)
    if m:
        brand = m.group(1)
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


# Обратная совместимость с кодом, который мог импортировать extract_brand
extract_brand = extract_gpu_brand


def extract_gpu_chip(name):
    """Извлекает GPU чип, нормализует: 'GeForce RTX 5070' → 'RTX 5070'."""
    m = GPU_CHIP_PATTERN.search(name)
    if not m:
        return None
    chip = m.group(0).strip()
    chip = re.sub(r"^GeForce\s+", "", chip, flags=re.IGNORECASE)
    chip = re.sub(r"^Radeon\s+", "", chip, flags=re.IGNORECASE)
    chip = re.sub(r"(RTX|GTX|GT|RX|R[579]|ARC)\s*(\d)", r"\1 \2", chip, flags=re.IGNORECASE)
    return chip.upper().strip()


def extract_memory(name):
    """Извлекает объём памяти в ГБ."""
    m = MEMORY_GB_PATTERN.search(name)
    return int(m.group(1)) if m else None


# ==================== CPU ====================

CPU_BRAND_PATTERN = re.compile(r"\b(Intel|AMD)\b", re.IGNORECASE)

CPU_MODEL_PATTERN = re.compile(
    r"Core\s+(?:Ultra\s+)?(?:i[3579]|HX|U|P)\s*[-]?\d{3,5}[A-Z0-9]*|"
    r"Ryzen\s+(?:Threadripper\s+(?:PRO\s+)?)?\d+\s+\d{4,5}[A-Z0-9]*|"
    r"Celeron\s+[A-Z]?\d{4,5}[A-Z]?|"
    r"Pentium\s+(?:Gold\s+)?[A-Z]?\d{4,5}[A-Z]?|"
    r"EPYC\s+\d{4,5}[A-Z]?",
    re.IGNORECASE,
)


def extract_cpu_key(name):
    """CPU: cpu_brand + model_code → ('INTEL', 'CORE I9-14900K')."""
    brand_m = CPU_BRAND_PATTERN.search(name)
    model_m = CPU_MODEL_PATTERN.search(name)
    if brand_m and model_m:
        brand = brand_m.group(1).upper()
        model = re.sub(r"\s+", " ", model_m.group(0)).upper().strip()
        return (brand, model)
    return None


# ==================== MB ====================

MB_CHIPSET_PATTERN = re.compile(
    r"\b([BZXH]\d{3}[EIM]?|TRX\d{2}|WRX\d{2}|X\d{3}[EIM]?)\b",
    re.IGNORECASE,
)

MB_FORMFACTOR_PATTERN = re.compile(
    r"\b(E-?ATX|[Mm]icro-?ATX|m-?ATX|Mini-?ITX|Mini-?DTX|ATX|ITX)\b",
    re.IGNORECASE,
)

_FF_NORM = {
    "EATX": "EATX", "E-ATX": "EATX",
    "MICROATX": "MATX", "MICRO-ATX": "MATX", "MATX": "MATX", "M-ATX": "MATX",
    "MINIITX": "ITX", "MINI-ITX": "ITX", "MINIDTX": "ITX", "MINI-DTX": "ITX",
    "ATX": "ATX", "ITX": "ITX",
}


def extract_mb_key(name):
    """MB: gpu_brand + chipset + form_factor → ('ASUS', 'B650', 'ATX')."""
    brand = extract_gpu_brand(name)
    chipset_m = MB_CHIPSET_PATTERN.search(name)
    ff_m = MB_FORMFACTOR_PATTERN.search(name)
    if brand and chipset_m:
        chipset = chipset_m.group(1).upper()
        ff_raw = ff_m.group(1).upper().replace(" ", "").replace("-", "") if ff_m else ""
        ff = _FF_NORM.get(ff_raw, ff_raw)
        return (brand.upper(), chipset, ff)
    return None


# ==================== RAM ====================

RAM_TYPE_PATTERN = re.compile(r"\b(DDR[2345])\b", re.IGNORECASE)
# Частота: ищем 4-5 цифр после дефиса или пробела (3200, 6000, 5600 и т.д.)
RAM_SPEED_PATTERN = re.compile(r"[-\s](\d{4,5})(?:\s*(?:MHz|МГц))?\b")
RAM_CAPACITY_PATTERN = re.compile(r"(\d+)\s*(?:ГБ|GB|G\b)", re.IGNORECASE)
# Кол-во планок: 2x16, 4x8 и т.д.
RAM_STICKS_PATTERN = re.compile(r"(\d+)\s*[xXхХ]\s*(\d+)\s*(?:ГБ|GB|G\b)", re.IGNORECASE)


def extract_ram_key(name):
    """RAM: ddr_type + speed_mhz + total_capacity_gb → ('DDR5', 6000, 32)."""
    type_m = RAM_TYPE_PATTERN.search(name)
    cap_m = RAM_CAPACITY_PATTERN.search(name)
    if not type_m or not cap_m:
        return None
    ddr = type_m.group(1).upper()
    # Суммарный объём: если "2x16GB" → 32, иначе берём первое число
    sticks_m = RAM_STICKS_PATTERN.search(name)
    if sticks_m:
        total_gb = int(sticks_m.group(1)) * int(sticks_m.group(2))
    else:
        total_gb = int(cap_m.group(1))
    # Скорость: ищем после типа DDR
    text_after_type = name[type_m.end():]
    speed_m = RAM_SPEED_PATTERN.search(text_after_type)
    speed = int(speed_m.group(1)) if speed_m else 0
    if total_gb < 2 or total_gb > 512:
        return None
    return (ddr, speed, total_gb)


# ==================== SSD ====================

STORAGE_BRAND_PATTERN = re.compile(
    r"\b(Samsung|Kingston|Crucial|WD|Western Digital|Seagate|Toshiba|"
    r"Transcend|Plextor|A-?Data|ADATA|Lexar|Hikvision|Gigabyte|Corsair|"
    r"Silicon Power|SP|Patriot|Team|TeamGroup|Apacer|GOODRAM|"
    r"Netac|Verbatim|PNY|MSI|ASUS|Sabrent|Inland|Addlink|XPG|"
    r"KIOXIA|Micron|SK\s*Hynix|Intel|Seagate|Sabrent|Solidigm)\b",
    re.IGNORECASE,
)

SSD_CAPACITY_PATTERN = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(ТБ|TB|ГБ|GB)\b",
    re.IGNORECASE,
)

SSD_INTERFACE_PATTERN = re.compile(
    r"\b(NVMe|PCIe|SATA|M\.2|mSATA)\b",
    re.IGNORECASE,
)


def _parse_capacity_gb(val_str, unit):
    """Конвертирует объём в ГБ."""
    val = float(val_str.replace(",", "."))
    if unit.upper() in ("ТБ", "TB"):
        return round(val * 1000 / 256) * 256  # округляем до стандартных значений
    return int(val)


def extract_storage_brand(name):
    m = STORAGE_BRAND_PATTERN.search(name)
    return m.group(1).upper().replace(" ", "").replace("-", "") if m else None


def extract_ssd_key(name):
    """SSD: storage_brand + capacity_gb + interface → ('SAMSUNG', 1000, 'NVME')."""
    brand = extract_storage_brand(name)
    cap_m = SSD_CAPACITY_PATTERN.search(name)
    if not brand or not cap_m:
        return None
    cap_gb = _parse_capacity_gb(cap_m.group(1), cap_m.group(2))
    if cap_gb < 16 or cap_gb > 64000:
        return None
    iface_m = SSD_INTERFACE_PATTERN.search(name)
    iface = iface_m.group(1).upper().replace(".", "") if iface_m else ""
    return (brand, cap_gb, iface)


# ==================== HDD ====================

HDD_CAPACITY_PATTERN = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(ТБ|TB|ГБ|GB)\b",
    re.IGNORECASE,
)


def extract_hdd_key(name):
    """HDD: storage_brand + capacity_gb → ('SEAGATE', 2000)."""
    brand = extract_storage_brand(name)
    cap_m = HDD_CAPACITY_PATTERN.search(name)
    if not brand or not cap_m:
        return None
    cap_gb = _parse_capacity_gb(cap_m.group(1), cap_m.group(2))
    if cap_gb < 100 or cap_gb > 200000:
        return None
    return (brand, cap_gb)


# ==================== PSU ====================

PSU_WATTAGE_PATTERN = re.compile(r"(\d{3,4})\s*(?:Вт|W)\b", re.IGNORECASE)
PSU_CERT_PATTERN = re.compile(
    r"\b(Titanium|Platinum|Gold|Silver|Bronze|80\s*[+Plus]+)\b",
    re.IGNORECASE,
)

_CERT_NORM = {
    "TITANIUM": "TITANIUM", "PLATINUM": "PLATINUM",
    "GOLD": "GOLD", "SILVER": "SILVER", "BRONZE": "BRONZE",
}


def extract_psu_key(name):
    """PSU: wattage_w + cert_tier → (850, 'GOLD')."""
    watt_m = PSU_WATTAGE_PATTERN.search(name)
    if not watt_m:
        return None
    watt = int(watt_m.group(1))
    if watt < 200 or watt > 2000:
        return None
    # Округляем до 50 Вт для допуска
    watt_rounded = round(watt / 50) * 50
    cert_m = PSU_CERT_PATTERN.search(name)
    cert = ""
    if cert_m:
        raw = cert_m.group(1).upper().replace(" ", "")
        cert = _CERT_NORM.get(raw, raw)
    return (watt_rounded, cert)


# ==================== GENERIC GROUPER ====================

def _match_category_products(products, key_fn, matched_ids, conn):
    """Группирует products по key_fn(name), записывает canonical_id.

    Возвращает количество объединённых товаров.
    """
    sig_map = defaultdict(list)
    for p in products:
        if p["id"] in matched_ids:
            continue
        key = key_fn(p["name"])
        if key:
            sig_map[key].append(p)

    count = 0
    for _key, group in sig_map.items():
        by_source = {}
        for p in group:
            if p["id"] not in matched_ids:
                by_source.setdefault(p["source"], []).append(p)

        if len(by_source) < 2:
            continue

        representatives = [prods[0] for prods in by_source.values()]
        canonical_id = min(p["id"] for p in representatives)

        for p in representatives:
            if p["id"] != canonical_id:
                conn.execute(
                    "UPDATE products SET canonical_id = ? WHERE id = ?",
                    (canonical_id, p["id"]),
                )
                matched_ids.add(p["id"])
                count += 1

    return count


# ==================== МАТЧИНГ ====================

def run_matching():
    """Запускает матчинг товаров между магазинами.

    GPU Tier 1 — код модели из скобок Регарда (citilink ↔ regard).
    GPU Tier 2 — brand + chip + memory по всем магазинам.
    GPU Tier 3 — chip + memory (без бренда) по всем магазинам.
    CPU        — brand + model_code.
    MB         — brand + chipset + form_factor.
    RAM        — ddr_type + speed_mhz + capacity_gb.
    SSD        — storage_brand + capacity_gb + interface.
    HDD        — storage_brand + capacity_gb.
    PSU        — wattage_w + cert_tier.
    """
    conn = db.get_connection()
    try:
        # Загружаем все товары с категорией и источником
        rows = conn.execute("""
            SELECT p.id, p.name, p.category, o.source
            FROM products p
            JOIN offers o ON o.product_id = p.id
            GROUP BY p.id
        """).fetchall()

        products = [dict(r) for r in rows]

        # Очищаем старые результаты матчинга только для товаров с офферами
        if products:
            placeholders = ",".join("?" * len(products))
            ids = [p["id"] for p in products]
            conn.execute(
                f"UPDATE products SET canonical_id = NULL WHERE id IN ({placeholders})",
                ids,
            )

        # Обновляем brand/model из названий для GPU
        for p in products:
            brand = extract_gpu_brand(p["name"])
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

        matched_ids: set = set()

        # -------- GPU Tier 1: код модели из скобок Регарда --------
        gpu_products = [p for p in products if not p["category"] or p["category"] == "GPU"]
        citilink = [p for p in gpu_products if p["source"] == "citilink"]
        regard   = [p for p in gpu_products if p["source"] == "regard"]

        print(f"Всего товаров: {len(products)}")
        print(f"Ситилинк GPU: {len(citilink)} | Регард GPU: {len(regard)}")
        print(f"Источников: {len(set(p['source'] for p in products))}")

        matched_tier1 = 0
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

        # -------- GPU Tier 2: brand + chip + memory — все магазины --------
        def gpu_key_t2(name):
            brand = extract_gpu_brand(name)
            chip  = extract_gpu_chip(name)
            mem   = extract_memory(name)
            if brand and chip and mem:
                return (brand.upper(), chip.upper(), int(mem))
            return None

        matched_tier2 = _match_category_products(
            [p for p in products if not p["category"] or p["category"] == "GPU"],
            gpu_key_t2, matched_ids, conn,
        )

        # -------- GPU Tier 3: chip + memory (без бренда) --------
        def gpu_key_t3(name):
            chip = extract_gpu_chip(name)
            mem  = extract_memory(name)
            if chip and mem:
                return (chip.upper(), int(mem))
            return None

        matched_tier3 = _match_category_products(
            [p for p in products if not p["category"] or p["category"] == "GPU"],
            gpu_key_t3, matched_ids, conn,
        )

        # -------- CPU --------
        cpu_products = [p for p in products if p["category"] == "CPU"]
        matched_cpu = _match_category_products(cpu_products, extract_cpu_key, matched_ids, conn)

        # -------- MB --------
        mb_products = [p for p in products if p["category"] == "MB"]
        matched_mb = _match_category_products(mb_products, extract_mb_key, matched_ids, conn)

        # -------- RAM --------
        ram_products = [p for p in products if p["category"] == "RAM"]
        matched_ram = _match_category_products(ram_products, extract_ram_key, matched_ids, conn)

        # -------- SSD --------
        ssd_products = [p for p in products if p["category"] == "SSD"]
        matched_ssd = _match_category_products(ssd_products, extract_ssd_key, matched_ids, conn)

        # -------- HDD --------
        hdd_products = [p for p in products if p["category"] == "HDD"]
        matched_hdd = _match_category_products(hdd_products, extract_hdd_key, matched_ids, conn)

        # -------- PSU --------
        psu_products = [p for p in products if p["category"] == "PSU"]
        matched_psu = _match_category_products(psu_products, extract_psu_key, matched_ids, conn)

        conn.commit()

        total = matched_tier1 + matched_tier2 + matched_tier3 + matched_cpu + matched_mb + matched_ram + matched_ssd + matched_hdd + matched_psu
        print(f"\nРезультаты матчинга:")
        print(f"  GPU Tier 1 (код Регарда):     {matched_tier1:>5} совпадений")
        print(f"  GPU Tier 2 (brand+chip+mem):  {matched_tier2:>5} совпадений")
        print(f"  GPU Tier 3 (chip+mem):        {matched_tier3:>5} совпадений")
        print(f"  CPU (brand+model):            {matched_cpu:>5} совпадений")
        print(f"  MB  (brand+chipset+ff):       {matched_mb:>5} совпадений")
        print(f"  RAM (type+speed+cap):         {matched_ram:>5} совпадений")
        print(f"  SSD (brand+cap+iface):        {matched_ssd:>5} совпадений")
        print(f"  HDD (brand+cap):              {matched_hdd:>5} совпадений")
        print(f"  PSU (watt+cert):              {matched_psu:>5} совпадений")
        print(f"  {'─'*34}")
        print(f"  ИТОГО:                        {total:>5} совпадений")

        return total
    finally:
        conn.close()


if __name__ == "__main__":
    db.init_db()
    run_matching()
