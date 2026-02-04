import os
import re
import sys
import traceback
import threading
import time
import urllib.parse
from fastapi import Request
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, time as dt_time
from typing import Any, Dict, List, Optional, Tuple
from starlette.requests import Request
from urllib.parse import parse_qs
import json
import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from fastapi import FastAPI, HTTPException, Query

# -----------------------------
# CONFIG
# -----------------------------
BITRIX_WEBHOOK = os.getenv(
    "BITRIX_WEBHOOK",
    "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote"
)

PG_HOST = os.getenv("PG_HOST", "194.33.40.197")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crm")
PG_USER = os.getenv("PG_USER", "crm")
PG_PASS = os.getenv("PG_PASS", "crm")

# Autoupdate
# По умолчанию включено (можно отключить через AUTO_SYNC_ENABLED=0)
# Когда Bitrix API разблокируют - синхронизация автоматически заработает
AUTO_SYNC_ENABLED = os.getenv("AUTO_SYNC_ENABLED", "0") == "1"
# Интервал авто-синка (по умолчанию 120 секунд)
AUTO_SYNC_INTERVAL_SEC = int(os.getenv("AUTO_SYNC_INTERVAL_SEC", "120"))

# Консервативные лимиты по умолчанию (вместо 0 = unlimited)
# Можно переопределить через переменные окружения для более агрессивной синхронизации
AUTO_SYNC_DEAL_LIMIT = int(os.getenv("AUTO_SYNC_DEAL_LIMIT", "50"))
AUTO_SYNC_SMART_LIMIT = int(os.getenv("AUTO_SYNC_SMART_LIMIT", "30"))
AUTO_SYNC_CONTACT_LIMIT = int(os.getenv("AUTO_SYNC_CONTACT_LIMIT", "30"))
AUTO_SYNC_LEAD_LIMIT = int(os.getenv("AUTO_SYNC_LEAD_LIMIT", "30"))

# Консервативное время работы синхронизации (10 секунд вместо 20)
# Helps avoid Bitrix operation time limit and API blocking
SYNC_TIME_BUDGET_SEC = int(os.getenv("SYNC_TIME_BUDGET_SEC", "10"))

# Консервативный интервал между запросами (1 секунда вместо 0.15)
# Helps avoid Bitrix rate limiting and API blocking
BITRIX_MIN_REQUEST_INTERVAL_SEC = float(os.getenv("BITRIX_MIN_REQUEST_INTERVAL_SEC", "1.0"))
BITRIX_MAX_RETRIES = int(os.getenv("BITRIX_MAX_RETRIES", "8"))
BITRIX_BACKOFF_BASE_SEC = float(os.getenv("BITRIX_BACKOFF_BASE_SEC", "0.7"))

# =====================================================================
# PDF HELPERS: "6 PDF по филиалам", и ВНУТРИ КАЖДОГО PDF — РАЗБИВКА ТАБЛИЦ
# =====================================================================
#
# ВАЖНО (почему я перенёс этот блок ВЫШЕ импорта api_data):
# api_data.py обычно импортирует build_branch_pdf / константы из main.py.
# Если main.py при этом импортит api_data.py — может быть circular import.
# Поэтому PDF-утилиты определены ДО строки `from api_data import router ...`,
# чтобы api_data мог безопасно импортировать их из main.py.
#
# =====================================================================

# reportlab (у тебя он уже стоит, раз pdf генеришь)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm


# ---- ПОЛЯ (оставляю те, что ты писал; если у тебя другие — поменяй тут) ----
STOCK_F_BRANCH = "ufCrm34_1749209523"       # Filiala (iblock element id или строка)
STOCK_F_LOC = "ufCrm34_1751116796"          # Locația (label/id)
STOCK_F_WAIT_SVC = "ufCrm34_1760623438126"  # In asteptare service (bool)

STOCK_F_FROMDT = "ufCrm34_1748962248"       # Data plecării
STOCK_F_TODT = "ufCrm34_1748962285"         # Data returnării

STOCK_F_CARNO = "ufCrm34_1748431574"        # Nr Auto
STOCK_F_BRAND = "ufCrm34_1748347910"        # Marca
STOCK_F_MODEL = "ufCrm34_1748431620"        # Model


# ---- НАЗВАНИЯ ЛОКАЦИЙ ----
DEFAULT_SERVICE_LOCS = {
    "Testare dupa service",
    "Vulcanizare Studentilor",
    "Spalatoria",
}
DEFAULT_SALE_LOC = "Parcarea de Vânzare"


def _to_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
    if isinstance(v, str):
        try:
            s = v.strip().replace("Z", "+00:00")
            d = datetime.fromisoformat(s)
            return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d
        except Exception:
            return None
    return None


def stock_classify_default(fields: Dict[str, Any], now: datetime) -> Tuple[str, Optional[str]]:
    """
    Возвращает (bucket, subkey)
      bucket: "CHIRIE" | "SERVICE" | "PARCARE" | "ALTE" | "FARA_STATUS"
      subkey: для ALTE — имя локации, чтобы делать отдельные таблицы по каждой локации
    """
    dt_from = _to_dt(fields.get(STOCK_F_FROMDT))
    dt_to = _to_dt(fields.get(STOCK_F_TODT))

    loc = fields.get(STOCK_F_LOC)
    loc_s = str(loc).strip() if loc is not None else ""

    wait_s = fields.get(STOCK_F_WAIT_SVC)
    wait_s_bool = str(wait_s).lower() in ("1", "true", "y", "yes", "да", "on")

    # 1) CHIRIE: есть даты и return >= now
    if dt_from and dt_to:
        try:
            if dt_to >= now:
                return ("CHIRIE", None)
        except Exception:
            return ("CHIRIE", None)

    # 2) SERVICE: флаг ожидания ИЛИ локация = сервисная
    if wait_s_bool or (loc_s and loc_s in DEFAULT_SERVICE_LOCS):
        return ("SERVICE", None)

    # 3) PARCARE (продажи)
    if loc_s and loc_s == DEFAULT_SALE_LOC:
        return ("PARCARE", None)

    # 4) ALTE: любое другое значение локации — отдельной таблицей по loc
    if loc_s:
        # ВАЖНО: сюда как раз попадёт "Prelungire" (если это значение Locația)
        return ("ALTE", loc_s)

    # 5) без статуса
    return ("FARA_STATUS", None)


def _make_table(title: str, rows: List[List[str]], styles, col_widths):
    out = []
    out.append(Paragraph(f"<b>{title}</b>", styles["Heading3"]))
    out.append(Spacer(1, 3 * mm))

    header = ["№ Auto", "Marca", "Model", "Locația", "De la", "Până la"]
    data = [header] + (rows if rows else [["", "", "", "", "", ""]])

    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    out.append(t)
    out.append(Spacer(1, 7 * mm))
    return out


def build_branch_pdf(
    branch_name: str,
    branch_items_fields: List[Dict[str, Any]],
    pdf_path: str,
    classify_fn=stock_classify_default,
) -> str:
    """
    Делает ОДИН PDF ДЛЯ ОДНОГО ФИЛИАЛА, но внутри:
      CHIRIE
      SERVICE
      PARCARE
      ALTE — отдельные таблицы по каждой "Locația" (включая Prelungire если он там)
      FARA_STATUS
    """
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm
    )

    now = datetime.now(timezone.utc)
    col_widths = [22 * mm, 28 * mm, 40 * mm, 38 * mm, 30 * mm, 30 * mm]

    buckets: Dict[str, Any] = {
        "CHIRIE": [],
        "SERVICE": [],
        "PARCARE": [],
        "ALTE": {},        # subkey (loc) -> rows
        "FARA_STATUS": [],
    }

    for f in branch_items_fields:
        bucket, subkey = classify_fn(f, now)

        car_no = f.get(STOCK_F_CARNO, "") or ""
        brand = f.get(STOCK_F_BRAND, "") or ""
        model = f.get(STOCK_F_MODEL, "") or ""
        loc = f.get(STOCK_F_LOC, "") or ""

        dt_from = _to_dt(f.get(STOCK_F_FROMDT))
        dt_to = _to_dt(f.get(STOCK_F_TODT))
        s_from = dt_from.strftime("%Y-%m-%d %H:%M") if dt_from else ""
        s_to = dt_to.strftime("%Y-%m-%d %H:%M") if dt_to else ""

        row = [str(car_no), str(brand), str(model), str(loc), s_from, s_to]

        if bucket == "ALTE":
            key = subkey or "Alt"
            buckets["ALTE"].setdefault(key, []).append(row)
        else:
            buckets[bucket].append(row)

    story = []
    story.append(Paragraph(f"<b>STOCK AUTO — {branch_name}</b>", styles["Title"]))
    story.append(Paragraph(datetime.now().strftime("%Y-%m-%d %H:%M"), styles["Normal"]))
    story.append(Spacer(1, 8 * mm))

    story += _make_table("CHIRIE", buckets["CHIRIE"], styles, col_widths)
    story += _make_table("SERVICE", buckets["SERVICE"], styles, col_widths)
    story += _make_table("PARCARE VÂNZARE", buckets["PARCARE"], styles, col_widths)

    # ALTE как много таблиц по локациям (сюда попадёт и Prelungire, если это Locația)
    for loc_name, rows in sorted(buckets["ALTE"].items(), key=lambda x: x[0]):
        story += _make_table(f"ALTE — {loc_name}", rows, styles, col_widths)

    story += _make_table("FĂRĂ STATUS", buckets["FARA_STATUS"], styles, col_widths)

    doc.build(story)
    return pdf_path


# -----------------------------
# FastAPI app + router
# -----------------------------
app = FastAPI(title="Bitrix24 Schema+Data Sync API")

# Import data API router
from api_data import router as data_router
app.include_router(data_router)

# Import processes-deals API router
from processes_deals_api import router as processes_deals_router
app.include_router(processes_deals_router)

# Import entity-meta-fields API router
from entity_meta_fields_api import router as entity_meta_fields_router
app.include_router(entity_meta_fields_router)

from entity_meta_data_api import router as entity_meta_data_router
app.include_router(entity_meta_data_router)

# -----------------------------
# Bitrix REST client
# -----------------------------
class BitrixClient:
    def __init__(self, webhook_base: str):
        self.base = webhook_base.rstrip("/")
        self._last_call_ts = 0.0

    def _throttle(self):
        now = time.time()
        dt = now - self._last_call_ts
        if dt < BITRIX_MIN_REQUEST_INTERVAL_SEC:
            time.sleep(BITRIX_MIN_REQUEST_INTERVAL_SEC - dt)
        self._last_call_ts = time.time()

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}/{method}.json"
        payload = params or {}

        last_err = None
        for attempt in range(BITRIX_MAX_RETRIES):
            try:
                self._throttle()
                r = requests.post(url, json=payload, timeout=60)

                # Bitrix can return 429 with JSON or plain text
                if r.status_code == 429:
                    wait = BITRIX_BACKOFF_BASE_SEC * (2 ** attempt)
                    time.sleep(wait)
                    last_err = f"Bitrix HTTP 429: {r.text}"
                    continue

                # Проверяем HTTP 401 - может быть OVERLOAD_LIMIT в JSON
                if r.status_code == 401:
                    try:
                        data = r.json()
                        if "error" in data and str(data.get("error")) == "OVERLOAD_LIMIT":
                            print(f"WARNING: b24.call: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
                            return {"error": "OVERLOAD_LIMIT", "result": []}
                    except:
                        pass  # Если не JSON, продолжим как обычно
                    # Если не OVERLOAD_LIMIT, падаем с ошибкой
                    raise HTTPException(status_code=502, detail=f"Bitrix HTTP {r.status_code}: {r.text}")

                if r.status_code != 200:
                    raise HTTPException(status_code=502, detail=f"Bitrix HTTP {r.status_code}: {r.text}")

                data = r.json()

                # Bitrix error in body
                if "error" in data:
                    err = str(data.get("error"))
                    # OVERLOAD_LIMIT - API заблокирован, возвращаем специальный ответ вместо исключения
                    if err == "OVERLOAD_LIMIT":
                        print(f"WARNING: b24.call: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
                        return {"error": "OVERLOAD_LIMIT", "result": []}
                    
                    # Typical: OPERATION_TIME_LIMIT
                    if err in ("OPERATION_TIME_LIMIT", "QUERY_LIMIT_EXCEEDED") or "LIMIT" in err:
                        wait = BITRIX_BACKOFF_BASE_SEC * (2 ** attempt)
                        time.sleep(wait)
                        last_err = f"Bitrix error: {data.get('error')} {data.get('error_description')}"
                        continue

                    raise HTTPException(
                        status_code=502,
                        detail=f"Bitrix error: {data.get('error')} {data.get('error_description')}"
                    )

                return data
            except requests.RequestException as e:
                wait = BITRIX_BACKOFF_BASE_SEC * (2 ** attempt)
                time.sleep(wait)
                last_err = repr(e)

        raise HTTPException(status_code=502, detail=f"Bitrix retry limit exceeded. Last error: {last_err}")


b24 = BitrixClient(BITRIX_WEBHOOK)

# -----------------------------
# Postgres helpers
# -----------------------------
def pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def ensure_meta_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_meta_entities (
            entity_key TEXT PRIMARY KEY,
            entity_kind TEXT NOT NULL,  -- deal | smart_process
            title TEXT,
            entity_type_id INT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # Create table (old installs may have it without new columns)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_meta_fields (
            entity_key TEXT NOT NULL,
            b24_field TEXT NOT NULL,
            column_name TEXT NOT NULL,
            b24_type TEXT,
            is_multiple BOOLEAN DEFAULT FALSE,
            is_required BOOLEAN DEFAULT FALSE,
            is_readonly BOOLEAN DEFAULT FALSE,
            settings JSONB,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (entity_key, b24_field)
        );
        """)

        # MIGRATION: add new columns if table already existed
        cur.execute('ALTER TABLE b24_meta_fields ADD COLUMN IF NOT EXISTS b24_title TEXT;')
        cur.execute('ALTER TABLE b24_meta_fields ADD COLUMN IF NOT EXISTS b24_labels JSONB;')

        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_sync_state (
            entity_key TEXT PRIMARY KEY,
            cursor TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        
        # Классификатор источников (sursa) для сделок
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_classifier_sources (
            source_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # Кэш пользователей Bitrix (id -> name) для отображения в API без вызова Bitrix
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_users (
            id BIGINT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # Воронки сделок (категории): id — ID категории, name — название
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_deal_categories (
            id TEXT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        # Стадии сделок: stage_id (например C12:NEW), category_id — воронка, name — название стадии
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_deal_stages (
            stage_id TEXT PRIMARY KEY,
            category_id TEXT,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        # Enum/списочные значения полей (в т.ч. UF_CRM_*): по entity_key + b24_field + value_id — value_title
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_field_enum (
            entity_key TEXT NOT NULL,
            b24_field TEXT NOT NULL,
            value_id TEXT NOT NULL,
            value_title TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (entity_key, b24_field, value_id)
        );
        """)
    conn.commit()

def get_sync_cursor(conn, entity_key: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT cursor FROM b24_sync_state WHERE entity_key=%s", (entity_key,))
        row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    try:
        return int(row[0])
    except Exception:
        return 0

def validate_sync_cursor(conn, entity_key: str, table: str) -> int:
    """
    Проверяет валидность курсора и сбрасывает его, если он слишком большой.
    Это защита от старых значений курсора (смещения), которые могут быть больше реальных ID.
    """
    last_id = get_sync_cursor(conn, entity_key)
    
    if last_id == 0:
        return 0  # Нормально - начинаем с начала
    
    # Проверяем, есть ли записи с ID больше last_id в базе
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX(id) FROM "{table}"')
            max_db_id = cur.fetchone()[0] or 0
        
        # Если last_id намного больше максимального ID в базе - это старый курсор (смещение)
        # Сбрасываем его для безопасности
        if last_id > max_db_id + 1000:  # Запас 1000 на случай новых записей в Bitrix
            print(f"WARNING: validate_sync_cursor: Cursor {last_id} is too large (max DB ID: {max_db_id}) for {entity_key}, resetting to 0", file=sys.stderr, flush=True)
            set_sync_cursor(conn, entity_key, 0)
            return 0
    except Exception as e:
        # Если таблица не существует или ошибка - просто возвращаем last_id
        print(f"WARNING: validate_sync_cursor: Could not validate cursor for {entity_key}: {e}", file=sys.stderr, flush=True)
        return last_id
    
    return last_id

def set_sync_cursor(conn, entity_key: str, cursor: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO b24_sync_state(entity_key, cursor)
            VALUES (%s, %s)
            ON CONFLICT (entity_key) DO UPDATE
            SET cursor = EXCLUDED.cursor,
                updated_at = now()
        """, (entity_key, str(int(cursor))))
    conn.commit()


def _upsert_b24_user(conn, user_id: int, name: Optional[str]) -> None:
    """Сохранить/обновить имя пользователя в b24_users (для API без вызова Bitrix)."""
    if name is None or not str(name).strip():
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO b24_users (id, name, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name, updated_at = now()
            """, (int(user_id), str(name).strip()))
        conn.commit()
    except Exception as e:
        print(f"WARNING: _upsert_b24_user({user_id}): {e}", file=sys.stderr, flush=True)


# -----------------------------
# Naming + type mapping
# -----------------------------
def sanitize_ident(name: str, max_len: int = 55) -> str:
    name = str(name).lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "field"
    if len(name) > max_len:
        name = name[:max_len].rstrip("_")
    if name[0].isdigit():
        name = f"f_{name}"
    return name

def unique_column_name(existing: set, base: str) -> str:
    if base not in existing:
        existing.add(base)
        return base
    i = 2
    while f"{base}_{i}" in existing:
        i += 1
    col = f"{base}_{i}"
    existing.add(col)
    return col

def map_b24_to_pg_type(b24_type: Optional[str], is_multiple: bool) -> str:
    if is_multiple:
        return "JSONB"
    t = (b24_type or "").lower()
    if t in ("integer", "int"):
        return "BIGINT"
    if t in ("double", "float", "number"):
        return "DOUBLE PRECISION"
    if t in ("boolean", "bool"):
        return "BOOLEAN"
    if t in ("datetime",):
        return "TIMESTAMPTZ"
    if t in ("date",):
        return "DATE"
    if t in ("string", "text", "char"):
        return "TEXT"
    return "TEXT"

def table_name_for_entity(entity_key: str) -> str:
    if entity_key == "deal":
        return "b24_crm_deal"
    if entity_key == "contact":
        return "b24_crm_contact"
    if entity_key == "lead":
        return "b24_crm_lead"
    if entity_key.startswith("sp:"):
        etid = entity_key.split(":", 1)[1]
        return f"b24_sp_{sanitize_ident(etid, max_len=20)}"
    return f"b24_{sanitize_ident(entity_key)}"

# -----------------------------
# Schema creation
# -----------------------------
def ensure_table_base(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGINT,
            raw JSONB,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{sanitize_ident(table, 40)}_id ON {table}(id);")
    conn.commit()

def ensure_columns(conn, table: str, columns: List[Tuple[str, str]]):
    with conn.cursor() as cur:
        for col, pgtype in columns:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{col}" {pgtype};')
    conn.commit()

def ensure_pk_index(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{sanitize_ident(table, 40)}_id ON {table}(id);')
    conn.commit()

def upsert_meta_entities(conn, items: List[Dict[str, Any]]):
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO b24_meta_entities (entity_key, entity_kind, title, entity_type_id)
            VALUES %s
            ON CONFLICT (entity_key) DO UPDATE
            SET entity_kind = EXCLUDED.entity_kind,
                title = EXCLUDED.title,
                entity_type_id = EXCLUDED.entity_type_id,
                updated_at = now()
            """,
            [
                (
                    i["entity_key"],
                    i["entity_kind"],
                    i.get("title"),
                    i.get("entity_type_id"),
                )
                for i in items
            ],
        )
    conn.commit()

def upsert_meta_fields(conn, entity_key: str, fields: Dict[str, Any], colmap: Dict[str, str]):
    """
    Сохраняем человеко-читаемое название поля из Bitrix в b24_title,
    и все варианты label-ов в b24_labels (JSONB).
    """
    def pick_title(meta: Dict[str, Any]) -> Optional[str]:
        for k in ("title", "formLabel", "listLabel", "filterLabel", "label", "name"):
            v = meta.get(k)
            s = _label_to_string(v)
            if s:
                return s
        return None

    rows = []
    for b24_field, meta in fields.items():
        settings_val = meta.get("settings")

        labels = {
            "title": meta.get("title"),
            "formLabel": meta.get("formLabel"),
            "listLabel": meta.get("listLabel"),
            "filterLabel": meta.get("filterLabel"),
            "label": meta.get("label"),
            "name": meta.get("name"),
        }

        rows.append((
            entity_key,
            b24_field,
            colmap[b24_field],
            meta.get("type"),
            bool(meta.get("isMultiple", False)),
            bool(meta.get("isRequired", False)),
            bool(meta.get("isReadOnly", False)),
            Json(settings_val) if settings_val is not None else None,
            pick_title(meta),
            Json(labels),
        ))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO b24_meta_fields
            (entity_key, b24_field, column_name, b24_type, is_multiple, is_required, is_readonly, settings, b24_title, b24_labels)
            VALUES %s
            ON CONFLICT (entity_key, b24_field) DO UPDATE
            SET column_name = EXCLUDED.column_name,
                b24_type = EXCLUDED.b24_type,
                is_multiple = EXCLUDED.is_multiple,
                is_required = EXCLUDED.is_required,
                is_readonly = EXCLUDED.is_readonly,
                settings = EXCLUDED.settings,
                b24_title = EXCLUDED.b24_title,
                b24_labels = EXCLUDED.b24_labels,
                updated_at = now()
            """,
            rows
        )
    conn.commit()

def sync_sources_classifier(conn):
    """
    Синхронизирует классификатор источников (sursa) из Bitrix API.
    Получает enum значения напрямую из crm.deal.userfield.list для поля UF_CRM_1749211409067.
    Это пользовательское поле типа enumeration в сделках.
    """
    from api_data import DEALS_F_SURSA
    
    # Нормализуем название поля: может быть uf_crm_... или UF_CRM_...
    DEAL_SOURCE_UF = "UF_CRM_1749211409067"  # Всегда в верхнем регистре для Bitrix API
    
    print(f"INFO: sync_sources_classifier: Starting sync from Bitrix API (crm.deal.userfield.list) for field {DEAL_SOURCE_UF}", file=sys.stderr, flush=True)
    
    try:
        # Шаг 1: Получаем список всех пользовательских полей сделок из Bitrix API
        print(f"INFO: sync_sources_classifier: Calling crm.deal.userfield.list", file=sys.stderr, flush=True)
        data = b24.call("crm.deal.userfield.list", {})
        
        result = data.get("result")
        if not result:
            print(f"ERROR: sync_sources_classifier: No result in API response", file=sys.stderr, flush=True)
            return
        
        # Bitrix может вернуть result как dict с ключом userFields/fields/items, или как list
        user_fields = []
        if isinstance(result, dict):
            user_fields = result.get("userFields") or result.get("fields") or result.get("items") or []
        elif isinstance(result, list):
            user_fields = result
        else:
            print(f"ERROR: sync_sources_classifier: Unexpected result type: {type(result)}", file=sys.stderr, flush=True)
            return
        
        if not isinstance(user_fields, list):
            print(f"ERROR: sync_sources_classifier: user_fields is not a list: {type(user_fields)}", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Received {len(user_fields)} user fields from Bitrix API", file=sys.stderr, flush=True)
        
        if not user_fields:
            print(f"ERROR: sync_sources_classifier: user_fields is empty!", file=sys.stderr, flush=True)
            print(f"DEBUG: sync_sources_classifier: Full API response result: {result}", file=sys.stderr, flush=True)
            return
        
        # Шаг 2: Находим нужное поле UF_CRM_1749211409067
        uf = None
        all_field_names = []
        for u in user_fields:
            if not isinstance(u, dict):
                continue
            field_name = u.get("fieldName") or u.get("FIELD_NAME") or u.get("field_name") or u.get("FIELD") or u.get("field")
            all_field_names.append(field_name)
            if field_name == DEAL_SOURCE_UF or field_name == DEALS_F_SURSA or field_name == DEALS_F_SURSA.upper():
                uf = u
                print(f"DEBUG: sync_sources_classifier: Found matching field: {field_name}, full object keys: {list(u.keys())}", file=sys.stderr, flush=True)
                break
        
        if not uf:
            print(f"ERROR: sync_sources_classifier: Field {DEAL_SOURCE_UF} not found in user fields", file=sys.stderr, flush=True)
            # Логируем все названия полей для отладки
            print(f"DEBUG: sync_sources_classifier: All field names ({len(all_field_names)}): {all_field_names[:20]}", file=sys.stderr, flush=True)
            # Проверяем, есть ли поля, содержащие нужный ID
            matching_fields = [u for u in user_fields if isinstance(u, dict) and ("1749211409067" in str(u.get("fieldName", "")) or "1749211409067" in str(u.get("FIELD_NAME", "")))]
            if matching_fields:
                print(f"DEBUG: sync_sources_classifier: Found fields containing '1749211409067': {[u.get('fieldName') or u.get('FIELD_NAME') for u in matching_fields]}", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Found field {DEAL_SOURCE_UF} in API response", file=sys.stderr, flush=True)
        print(f"DEBUG: sync_sources_classifier: Field object keys: {list(uf.keys())}", file=sys.stderr, flush=True)
        
        # Шаг 3: Извлекаем enum значения из поля
        # Пробуем разные ключи: items, values, ENUM, LIST
        items = (
            uf.get("items") or 
            uf.get("values") or 
            uf.get("ENUM") or 
            uf.get("LIST") or 
            uf.get("list") or
            []
        )
        
        if not isinstance(items, list):
            print(f"ERROR: sync_sources_classifier: Items is not a list: {type(items)}, value: {items}", file=sys.stderr, flush=True)
            # Логируем все ключи объекта для отладки
            print(f"DEBUG: sync_sources_classifier: All field keys and their types: {[(k, type(v).__name__) for k, v in uf.items()]}", file=sys.stderr, flush=True)
            return
        
        if not items:
            print(f"WARNING: sync_sources_classifier: No enum items found in field {DEAL_SOURCE_UF}", file=sys.stderr, flush=True)
            print(f"DEBUG: sync_sources_classifier: Field object (first 500 chars): {str(uf)[:500]}", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Found {len(items)} enum items in field", file=sys.stderr, flush=True)
        # Логируем первый элемент для отладки
        if items and len(items) > 0:
            print(f"DEBUG: sync_sources_classifier: First item example: {items[0]}", file=sys.stderr, flush=True)
        
        # Шаг 4: Формируем список для вставки в классификатор
        # Используем правильную логику извлечения: ID для source_id, VALUE или NAME для source_name
        rows = []
        for opt in items:
            if not isinstance(opt, dict):
                continue
            
            # Правильная логика извлечения (как в примере ChatGPT):
            # vid (source_id) = ID или VALUE (если это число)
            # name (source_name) = VALUE или NAME (VALUE может быть текстом!)
            vid = opt.get("ID") or opt.get("VALUE") or opt.get("value")
            name = opt.get("VALUE") or opt.get("NAME") or opt.get("value") or opt.get("name")
            
            # Пропускаем пустые значения
            if vid is None or vid == "" or str(vid).strip() == "":
                if len(rows) < 3:  # Логируем только первые 3 пропущенных для отладки
                    print(f"DEBUG: sync_sources_classifier: Skipping item with empty ID/VALUE: {opt}", file=sys.stderr, flush=True)
                continue
            
            # Если name отсутствует, пропускаем (не используем vid как название)
            if name is None or name == "" or str(name).strip() == "":
                print(f"WARNING: sync_sources_classifier: Skipping item with empty NAME (vid={vid}): {opt}", file=sys.stderr, flush=True)
                continue
            
            # Убеждаемся, что vid - это ID, а name - это текст
            rows.append((str(vid), str(name)))
        
        if not rows:
            print(f"ERROR: sync_sources_classifier: No valid sources to insert. Processed {len(items)} items but got 0 valid rows", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Prepared {len(rows)} rows for insertion", file=sys.stderr, flush=True)
        
        # Шаг 5: Вставляем/обновляем классификатор
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO b24_classifier_sources (source_id, source_name)
                VALUES %s
                ON CONFLICT (source_id) DO UPDATE
                SET source_name = EXCLUDED.source_name,
                    updated_at = now()
                """,
                rows,
                page_size=100
            )
            
            conn.commit()
            print(f"INFO: sync_sources_classifier: Successfully synced {len(rows)} sources to classifier", file=sys.stderr, flush=True)
    except Exception as e:
        conn.rollback()
        print(f"ERROR: sync_sources_classifier: Failed to sync sources from Bitrix API: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise


def sync_deal_categories(conn) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Заполняет b24_deal_categories. Возвращает (rows, debug_notes)."""
    rows: List[Tuple[str, str]] = []
    debug_notes: List[str] = []
    for method, params in [
        ("crm.category.list", {"entityTypeId": 2}),
        ("crm.category.list", {}),
        ("crm.dealcategory.list", {}),
    ]:
        try:
            data = b24.call(method, params)
            result = data.get("result")
            if not result:
                debug_notes.append(f"{method}: result empty")
                continue
            raw = result.get("categories") or result.get("items") if isinstance(result, dict) else result
            if isinstance(raw, dict):
                items = list(raw.values())
            elif isinstance(raw, list):
                items = raw
            else:
                items = []
            if isinstance(result, dict) and not items and result.get("result") is not None:
                r2 = result.get("result")
                items = list(r2.values()) if isinstance(r2, dict) else (r2 if isinstance(r2, list) else [])
            debug_notes.append(f"{method}: got {len(items)} items (first type: {type(items[0]).__name__ if items else 'n/a'})")
            for cat in items:
                cid = None
                name = ""
                if isinstance(cat, dict):
                    cid = cat.get("id") or cat.get("ID") or cat.get("Id") or cat.get("entityTypeId") or cat.get("categoryId")
                    name = cat.get("name") or cat.get("NAME") or cat.get("title") or cat.get("TITLE") or ""
                elif cat is not None and not isinstance(cat, (dict, list)):
                    cid = cat
                    name = str(cat)
                if cid is None:
                    continue
                rows.append((str(cid), (name or str(cid)).strip()))
            if rows:
                break
        except Exception as e:
            debug_notes.append(f"{method}: {type(e).__name__}: {e}")
            print(f"DEBUG: sync_deal_categories {method}: {e}", file=sys.stderr, flush=True)
            continue
    if rows:
        # Убираем дубликаты по id (ON CONFLICT не допускает два обновления одной строки в одной команде)
        seen_cat: Dict[str, Tuple[str, str]] = {}
        for cid, name in rows:
            seen_cat[cid] = (cid, name)
        rows = list(seen_cat.values())
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_deal_categories (id, name)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
                    """,
                    rows,
                    page_size=100,
                )
            conn.commit()
            print(f"INFO: sync_deal_categories: synced {len(rows)} categories", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            debug_notes.append(f"insert error: {e}")
            print(f"WARNING: sync_deal_categories insert: {e}", file=sys.stderr, flush=True)
    else:
        debug_notes.append("no categories parsed (check result.categories / dealcategory list format)")
    return rows, debug_notes


def sync_sources_from_status(conn) -> int:
    """
    Синхронизирует стандартные источники (SOURCE) из crm.status.list ENTITY_ID=SOURCE
    в b24_classifier_sources и в b24_field_enum (deal/lead source_id). Так значение «Источник»
    в сделках/лидах будет показывать название вместо кода (UC_Z315Y5 и т.д.).
    """
    try:
        data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": "SOURCE"}})
        result = data.get("result")
        if not result:
            return 0
        items = result if isinstance(result, list) else (
            result.get("result") or result.get("statuses") or result.get("items")
            or result.get("SOURCE") or result.get("source") or []
        )
        if not isinstance(items, list) or not items:
            return 0
        rows: List[Tuple[str, str]] = []
        for st in items:
            sid = None
            name = ""
            if isinstance(st, dict):
                sid = (
                    st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                    or st.get("VALUE") or st.get("value") or st.get("SYMBOL_CODE")
                )
                name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
            if sid is None:
                continue
            sid_str = str(sid).strip()
            name_str = (name or sid_str).strip()
            rows.append((sid_str, name_str))
        if not rows:
            return 0
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO b24_classifier_sources (source_id, source_name)
                VALUES %s
                ON CONFLICT (source_id) DO UPDATE SET source_name = EXCLUDED.source_name, updated_at = now()
                """,
                rows,
                page_size=200,
            )
            enum_rows: List[Tuple[str, str, str, str]] = []
            for sid_str, name_str in rows:
                enum_rows.append(("deal", "source_id", sid_str, name_str))
                enum_rows.append(("lead", "source_id", sid_str, name_str))
            if enum_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_field_enum (entity_key, b24_field, value_id, value_title)
                    VALUES %s
                    ON CONFLICT (entity_key, b24_field, value_id) DO UPDATE SET value_title = EXCLUDED.value_title, updated_at = now()
                    """,
                    enum_rows,
                    page_size=200,
                )
        conn.commit()
        print(f"INFO: sync_sources_from_status: synced {len(rows)} standard SOURCE statuses", file=sys.stderr, flush=True)
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"WARNING: sync_sources_from_status: {e}", file=sys.stderr, flush=True)
        return 0


def sync_deal_stages(conn) -> Tuple[List[Tuple[str, str, str]], List[str]]:
    """Заполняет b24_deal_stages из crm.status.list. Возвращает (rows, debug_notes)."""
    debug_notes: List[str] = []
    entity_ids = ["DEAL_STAGE"]
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM b24_deal_categories ORDER BY id")
        for row in cur.fetchall() or []:
            entity_ids.append(f"DEAL_STAGE_{row[0]}")
    if len(entity_ids) == 1:
        entity_ids = ["DEAL_STAGE", "DEAL_STAGE_0", "DEAL_STAGE_1", "DEAL_STAGE_2", "DEAL_STAGE_12", "DEAL_STAGE_20"]
    rows: List[Tuple[str, str, str]] = []
    for entity_id in entity_ids:
        try:
            data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": entity_id}})
            result = data.get("result")
            if not result:
                continue
            items = result if isinstance(result, list) else (result.get("result") or result.get("statuses") or result.get("items") or [])
            if not isinstance(items, list):
                items = []
            if items and len(debug_notes) == 0:
                debug_notes.append(f"status.list {entity_id}: {len(items)} items, first type={type(items[0]).__name__}")
            cat_id = entity_id.replace("DEAL_STAGE_", "") if entity_id != "DEAL_STAGE" else "0"
            for st in items:
                sid = None
                name = ""
                if isinstance(st, dict):
                    sid = st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                    name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                elif st is not None and not isinstance(st, (dict, list)):
                    sid = st
                    name = str(st)
                if sid is None:
                    continue
                rows.append((str(sid), cat_id, (name or str(sid)).strip()))
        except Exception as e:
            debug_notes.append(f"status.list {entity_id}: {e}")
            print(f"WARNING: sync_deal_stages {entity_id}: {e}", file=sys.stderr, flush=True)
    if rows:
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_deal_stages (stage_id, category_id, name)
                    VALUES %s
                    ON CONFLICT (stage_id) DO UPDATE SET category_id = EXCLUDED.category_id, name = EXCLUDED.name, updated_at = now()
                    """,
                    rows,
                    page_size=200,
                )
            conn.commit()
            print(f"INFO: sync_deal_stages: synced {len(rows)} stages", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            debug_notes.append(f"stages insert: {e}")
            print(f"WARNING: sync_deal_stages insert: {e}", file=sys.stderr, flush=True)
    return rows, debug_notes


def sync_deal_types(conn) -> None:
    """Заполняет b24_field_enum типами сделок (TYPE_ID) из crm.status.list ENTITY_ID=DEAL_TYPE."""
    rows: List[Tuple[str, str, str, str]] = []
    for entity_id in ("DEAL_TYPE", "TYPE"):
        try:
            data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": entity_id}})
            result = data.get("result")
            if not result:
                continue
            items: List[Any] = []
            if isinstance(result, list):
                items = result
            elif isinstance(result, dict):
                items = result.get("result") or result.get("statuses") or result.get("items") or []
                if not isinstance(items, list):
                    items = list(result.values()) if result else []
                if not items and result:
                    # формат { "UC_XXX": {"NAME": "..."}, ... }
                    for sid, st in result.items():
                        if isinstance(st, dict):
                            name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                            items.append({"STATUS_ID": sid, "NAME": name or sid})
                        elif st is not None and not isinstance(st, (dict, list)):
                            items.append({"STATUS_ID": sid, "NAME": str(st)})
            if not isinstance(items, list):
                items = []
            for st in items:
                sid = None
                name = ""
                if isinstance(st, dict):
                    sid = st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                    name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                elif st is not None and not isinstance(st, (dict, list)):
                    sid = st
                    name = str(st)
                if sid is None:
                    continue
                rows.append(("deal", "TYPE_ID", str(sid), (name or str(sid)).strip()))
            if rows:
                break
        except Exception as e:
            print(f"DEBUG: sync_deal_types {entity_id}: {e}", file=sys.stderr, flush=True)
            continue
    if not rows:
        return
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO b24_field_enum (entity_key, b24_field, value_id, value_title)
                VALUES %s
                ON CONFLICT (entity_key, b24_field, value_id) DO UPDATE SET value_title = EXCLUDED.value_title, updated_at = now()
                """,
                rows,
                page_size=100,
            )
        conn.commit()
        print(f"INFO: sync_deal_types: synced {len(rows)} deal types", file=sys.stderr, flush=True)
    except Exception as e:
        conn.rollback()
        print(f"WARNING: sync_deal_types: {e}", file=sys.stderr, flush=True)


def _userfield_items_to_enum_rows(entity_key: str, field_name: str, items: List[Any]) -> List[Tuple[str, str, str, str]]:
    rows = []
    for opt in items or []:
        if not isinstance(opt, dict):
            continue
        vid = opt.get("ID") or opt.get("id") or opt.get("VALUE") or opt.get("value")
        title = opt.get("VALUE") or opt.get("value") or opt.get("NAME") or opt.get("name") or opt.get("TITLE") or opt.get("title")
        if vid is None or (isinstance(vid, str) and not vid.strip()):
            continue
        if title is None:
            title = str(vid)
        rows.append((entity_key, field_name, str(vid).strip(), str(title).strip()))
    return rows


def sync_field_enums(conn, entity_key: str) -> Tuple[int, List[str]]:
    """Синхронизирует enum/списочные значения полей в b24_field_enum. Возвращает (n_inserted, debug_notes)."""
    debug_notes: List[str] = []
    if entity_key == "deal":
        method = "crm.deal.userfield.list"
    elif entity_key == "contact":
        method = "crm.contact.userfield.list"
    elif entity_key == "lead":
        method = "crm.lead.userfield.list"
    else:
        return 0, []
    try:
        data = b24.call(method, {})
        result = data.get("result")
        if not result:
            debug_notes.append(f"{entity_key} userfield.list: result empty")
            return 0, debug_notes
        field_list: List[Tuple[str, Dict[str, Any]]] = []
        if isinstance(result, list):
            for uf in result:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
            debug_notes.append(f"{entity_key}: result is list, {len(field_list)} fields")
        elif isinstance(result, dict):
            if result.get("userFields") and isinstance(result["userFields"], list):
                for uf in result["userFields"]:
                    if isinstance(uf, dict):
                        fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                        if fn:
                            field_list.append((fn, uf))
            elif result.get("fields") and isinstance(result["fields"], dict):
                for fn, uf in result["fields"].items():
                    if isinstance(uf, dict) and fn:
                        field_list.append((str(fn), uf))
            elif result.get("fields") and isinstance(result["fields"], list):
                for uf in result["fields"]:
                    if isinstance(uf, dict):
                        fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                        if fn:
                            field_list.append((fn, uf))
            else:
                for fn, uf in result.items():
                    if isinstance(uf, dict) and fn and not fn.startswith("_"):
                        field_list.append((str(fn), uf))
            debug_notes.append(f"{entity_key}: result is dict, {len(field_list)} fields")
        all_rows = []
        fields_with_items = 0
        for field_name, uf in field_list:
            raw_items = uf.get("items") or uf.get("values") or uf.get("ENUM") or uf.get("LIST") or uf.get("list") or []
            if isinstance(raw_items, dict):
                items = raw_items.get("items") or raw_items.get("values") or list(raw_items.values())
            else:
                items = raw_items if isinstance(raw_items, list) else []
            if not items:
                continue
            fields_with_items += 1
            all_rows.extend(_userfield_items_to_enum_rows(entity_key, field_name, items))
        debug_notes.append(f"{entity_key}: {fields_with_items} fields with enum items, {len(all_rows)} total values")
        if all_rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_field_enum (entity_key, b24_field, value_id, value_title)
                    VALUES %s
                    ON CONFLICT (entity_key, b24_field, value_id) DO UPDATE SET value_title = EXCLUDED.value_title, updated_at = now()
                    """,
                    all_rows,
                    page_size=200,
                )
            conn.commit()
            print(f"INFO: sync_field_enums({entity_key}): synced {len(all_rows)} enum values", file=sys.stderr, flush=True)
        return len(all_rows), debug_notes
    except Exception as e:
        conn.rollback()
        debug_notes.append(f"{entity_key}: {type(e).__name__}: {e}")
        print(f"WARNING: sync_field_enums({entity_key}): {e}", file=sys.stderr, flush=True)
        return 0, debug_notes


def _label_to_string(val: Any) -> Optional[str]:
    """Извлекает строку из label: строка как есть, dict — берём ru/en/first."""
    if val is None:
        return None
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        for k in ("ru", "en", "de", "ua", "first"):
            v = val.get(k) if k != "first" else (next(iter(val.values()), None) if val else None)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in val.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _userfield_list_to_field_titles(entity_key: str, result: Any) -> List[Tuple[str, str]]:
    """
    Парсит ответ crm.*.userfield.list и возвращает [(b24_field, human_title), ...].
    «Название поля» из админки Битрикс (как на скрине) обычно приходит в listLabel / editFormLabel / formLabel.
    """
    field_list: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(result, list):
        for uf in result:
            if isinstance(uf, dict):
                fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                if fn:
                    field_list.append((fn, uf))
    elif isinstance(result, dict):
        if result.get("userFields") and isinstance(result["userFields"], list):
            for uf in result["userFields"]:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
        elif result.get("items") and isinstance(result["items"], list):
            for uf in result["items"]:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
        elif result.get("fields") and isinstance(result["fields"], dict):
            for fn, uf in result["fields"].items():
                if isinstance(uf, dict) and fn:
                    field_list.append((str(fn), uf))
        elif result.get("fields") and isinstance(result["fields"], list):
            for uf in result["fields"]:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
        else:
            for fn, uf in result.items():
                if isinstance(uf, dict) and fn and not str(fn).startswith("_"):
                    field_list.append((str(fn), uf))
    # Ключи, в которых Битрикс может вернуть «Название поля» (как в админке: Код поля / Название поля)
    _title_keys = (
        "listLabel", "editFormLabel", "formLabel", "filterLabel",
        "title", "label", "name", "fieldLabel", "displayLabel", "caption", "header",
        "LIST_LABEL", "EDIT_FORM_LABEL", "FORM_LABEL", "TITLE", "LABEL", "NAME",
    )
    out: List[Tuple[str, str]] = []
    for field_name, uf in field_list:
        title = None
        for k in _title_keys:
            title = _label_to_string(uf.get(k))
            if title:
                break
        if title:
            out.append((field_name, title))
    return out


def _fields_response_to_title_pairs(fields_result: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Из ответа crm.*.fields извлекает [(b24_field, human_title), ...] для полей UF_CRM_*."""
    if not isinstance(fields_result, dict):
        return []
    out: List[Tuple[str, str]] = []
    for fn, meta in fields_result.items():
        if not fn or not isinstance(meta, dict):
            continue
        fn_str = str(fn).strip()
        if not (fn_str.upper().startswith("UF_CRM_") or fn_str.startswith("ufCrm")):
            continue
        title = None
        for k in ("listLabel", "editFormLabel", "formLabel", "filterLabel", "title", "label", "name"):
            title = _label_to_string(meta.get(k))
            if title:
                break
        if title:
            out.append((fn_str, title))
    return out


def sync_userfield_titles(conn, entity_key: str) -> int:
    """Обновляет b24_title в b24_meta_fields из crm.*.userfield.list (или crm.*.fields) для человекочитаемых названий полей."""
    if entity_key == "deal":
        method = "crm.deal.userfield.list"
        method_fallback = "crm.deal.fields"
    elif entity_key == "contact":
        method = "crm.contact.userfield.list"
        method_fallback = "crm.contact.fields"
    elif entity_key == "lead":
        method = "crm.lead.userfield.list"
        method_fallback = "crm.lead.fields"
    else:
        return 0
    updated = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT b24_field FROM b24_meta_fields WHERE entity_key = %s",
                (entity_key,),
            )
            existing = [row[0] for row in cur.fetchall() if row and row[0]]
        upper_to_field: Dict[str, str] = {str(f).upper(): f for f in existing}

        def apply_pairs(pairs: List[Tuple[str, str]]) -> int:
            n = 0
            with conn.cursor() as cur:
                for api_field, title in pairs:
                    canonical = upper_to_field.get(str(api_field).upper()) if api_field else None
                    if not canonical:
                        continue
                    cur.execute("""
                        UPDATE b24_meta_fields SET b24_title = %s, updated_at = now()
                        WHERE entity_key = %s AND b24_field = %s
                    """, (title, entity_key, canonical))
                    if cur.rowcount:
                        n += 1
            return n

        data = b24.call(method, {})
        result = data.get("result")
        if result:
            pairs = _userfield_list_to_field_titles(entity_key, result)
            if pairs:
                updated = apply_pairs(pairs)
        if updated == 0 and method_fallback:
            fallback_data = b24.call(method_fallback, {})
            fallback_result = fallback_data.get("result")
            if isinstance(fallback_result, dict):
                pairs = _fields_response_to_title_pairs(fallback_result)
                if pairs:
                    updated = apply_pairs(pairs)
                    print(f"INFO: sync_userfield_titles({entity_key}): used {method_fallback} fallback, updated {updated}", file=sys.stderr, flush=True)
        conn.commit()
        if updated:
            print(f"INFO: sync_userfield_titles({entity_key}): updated {updated} field titles", file=sys.stderr, flush=True)
        return updated
    except Exception as e:
        conn.rollback()
        print(f"WARNING: sync_userfield_titles({entity_key}): {e}", file=sys.stderr, flush=True)
        return 0


def load_entity_colmap(conn, entity_key: str) -> Dict[str, Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b24_field, column_name, b24_type, is_multiple
            FROM b24_meta_fields
            WHERE entity_key = %s
        """, (entity_key,))
        rows = cur.fetchall()

    m: Dict[str, Dict[str, Any]] = {}
    for b24_field, column_name, b24_type, is_multiple in rows:
        m[b24_field] = {
            "column_name": column_name,
            "b24_type": b24_type,
            "is_multiple": bool(is_multiple),
        }
    return m

def normalize_value(v: Any, b24_type: Optional[str] = None, is_multiple: bool = False):
    """
    Нормализует значение для вставки в PostgreSQL.
    - Пустые строки для дат/времени/чисел преобразуются в None
    - Для is_multiple полей значения оборачиваются в Json (даже boolean)
    - FIX: если колонка numeric/double/integer, а Bitrix прислал boolean -> приводим к 0/1
    - Дополнительно: мягкий парс "Y/N", "true/false", "1/0" для boolean
    """
    # Если поле multiple, колонка имеет тип JSONB - всегда оборачиваем в Json
    if is_multiple:
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return Json(v)
        return Json([v])

    # Для не-multiple: dict/list -> Json
    if isinstance(v, (dict, list)):
        return Json(v)

    bt = (b24_type or "").lower().strip()

    # Пустые строки -> None (кроме явного string/text)
    if isinstance(v, str) and not v.strip():
        if bt not in ("string", "text", "char"):
            return None
        return v

    # Нормализация boolean строк
    if bt in ("boolean", "bool"):
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("y", "yes", "true", "1", "on"):
                return True
            if s in ("n", "no", "false", "0", "off"):
                return False

    # FIX: numeric типы, но прилетел bool -> 0/1
    if isinstance(v, bool) and bt in (
        "double", "double precision", "float", "float8", "number", "numeric", "integer", "int", "int4", "int8"
    ):
        # для double лучше float
        if bt in ("double", "double precision", "float", "float8", "number", "numeric"):
            return 1.0 if v else 0.0
        return 1 if v else 0

    # (опционально) мягкий парс чисел из строк, если Bitrix прислал "123" или "12.5"
    if isinstance(v, str) and bt in ("double", "double precision", "float", "float8", "number", "numeric", "integer", "int", "int4", "int8"):
        s = v.strip().replace(",", ".")
        try:
            if bt in ("integer", "int", "int4", "int8"):
                return int(float(s))
            return float(s)
        except Exception:
            return v  # оставляем как есть

    return v

def upsert_rows(conn, table: str, columns: List[str], rows: List[List[Any]]):
    """
    Upsert rows into table by 'id'. Uses execute_values for speed.
    FIX: updated_at исключаем из set_cols, иначе получается 2 раза:
         updated_at = EXCLUDED.updated_at, updated_at = now()
    """
    if not rows:
        return

    # на всякий случай — убираем дубликаты колонок, сохраняя порядок
    seen = set()
    col_order = []
    for c in columns:
        if c not in seen:
            seen.add(c)
            col_order.append(c)

    cols_sql = ", ".join([f'"{c}"' for c in col_order])
    tmpl = "(" + ",".join(["%s"] * len(col_order)) + ")"

    # Важно: исключаем updated_at из set_cols
    set_cols = [c for c in col_order if c not in ("id", "created_at", "updated_at")]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in set_cols])

    # updated_at всегда обновляем
    if set_sql:
        set_sql = set_sql + ', "updated_at" = now()'
    else:
        set_sql = '"updated_at" = now()'

    sql = f"""
    INSERT INTO {table} ({cols_sql})
    VALUES %s
    ON CONFLICT ("id") DO UPDATE
    SET {set_sql}
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=tmpl, page_size=500)
    conn.commit()

def day_start_utc(tz_name: str = "Europe/Chisinau") -> datetime:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_local.astimezone(timezone.utc)

def normalize_value(v: Any, b24_type: Optional[str] = None, is_multiple: bool = False):
    """
    Нормализует значение для вставки в PostgreSQL.
    - Пустые строки для дат/времени/чисел преобразуются в None
    - Для is_multiple полей значения оборачиваются в Json (даже boolean)
    """
    # Если поле multiple, колонка имеет тип JSONB - всегда оборачиваем в Json
    if is_multiple:
        if v is None:
            return None
        # Если уже dict/list - оборачиваем в Json
        if isinstance(v, (dict, list)):
            return Json(v)
        # Если scalar (boolean, int, str) - оборачиваем в Json как массив
        return Json([v])
    
    # Для не-multiple полей: если значение dict/list, оборачиваем в Json
    if isinstance(v, (dict, list)):
        return Json(v)
    
    # Для пустых строк: преобразуем в None для всех типов, кроме TEXT
    if isinstance(v, str) and not v.strip():
        # Всегда преобразуем пустые строки в None (PostgreSQL не любит пустые строки в DATE/NUMERIC)
        # Исключение: если это явно TEXT поле
        if not b24_type or b24_type.lower() not in ("string", "text", "char"):
            return None
    
    return v

# -----------------------------
# Normalize Bitrix list response
# -----------------------------
def normalize_list_result(resp: Any) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Поддерживаем варианты:
      - crm.deal.list: {"result":[...], "next":50}
      - crm.item.list: {"result":{"items":[...]},"next":50}   (ВАЖНО: next часто сверху)
      - crm.item.list: {"result":{"items":[...], "next":50}}  (иногда next внутри)
      - иногда list напрямую
    Возвращаем (items, next_start)
    """
    if resp is None:
        return [], None

    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)], None

    if not isinstance(resp, dict):
        return [], None

    top_next = resp.get("next")

    # crm.deal.list: result = list
    if isinstance(resp.get("result"), list):
        items = [x for x in resp["result"] if isinstance(x, dict)]
        return items, (int(top_next) if top_next is not None else None)

    # crm.item.list: result = dict
    if isinstance(resp.get("result"), dict):
        inner = resp["result"]
        items = inner.get("items") or inner.get("result") or []
        if not isinstance(items, list):
            items = []
        items = [x for x in items if isinstance(x, dict)]

        inner_next = inner.get("next")
        nxt = inner_next if inner_next is not None else top_next
        return items, (int(nxt) if nxt is not None else None)

    return [], None

# -----------------------------
# Fetch entity fields from Bitrix
# -----------------------------
def fetch_deal_fields() -> Dict[str, Any]:
    data = b24.call("crm.deal.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}

def fetch_contact_fields() -> Dict[str, Any]:
    data = b24.call("crm.contact.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}

def fetch_lead_fields() -> Dict[str, Any]:
    data = b24.call("crm.lead.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}

def fetch_smart_process_types() -> List[Dict[str, Any]]:
    data = b24.call("crm.type.list", {"select": ["id", "title", "entityTypeId"]})
    res = data.get("result", {})

    if isinstance(res, dict):
        items = res.get("types")
        if isinstance(items, list):
            return items
    if isinstance(res, list):
        return res
    return []

def fetch_smart_fields(entity_type_id: int) -> Dict[str, Any]:
    data = b24.call("crm.item.fields", {"entityTypeId": entity_type_id})
    return data.get("result", {}).get("fields", {})

# -----------------------------
# Bitrix list data (for insert)
# -----------------------------
def b24_list_deals(
    start_id: int = 0,
    start_offset: int = 0,
    filter_params: Optional[Dict[str, Any]] = None,
    uf_fields: Optional[List[str]] = None,
    order: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Два режима:
      1) Инкремент по ID: start_id>0, start_offset=0, order={"ID":"ASC"}, filter {">ID":start_id}
      2) Today-pass по DATE_MODIFY: start_id=0, start_offset=next, order={"DATE_MODIFY":"ASC","ID":"ASC"}, filter {">=DATE_MODIFY": "..."}
    """

    select_list = ["*"]

    # Пытаемся получить имя ответственного (если Bitrix вернет)
    for x in ("ASSIGNED_BY_NAME", "assigned_by_name", "ASSIGNED_BY", "ASSIGNED_BY.*"):
        if x not in select_list:
            select_list.append(x)

    if uf_fields:
        select_list.extend(uf_fields)
    else:
        # может работать/не работать — но у тебя есть явный список UF из meta_fields
        select_list.append("UF_*")

    params = {
        "select": select_list,
        "start": int(start_offset),
        "order": order or {"ID": "ASC"},
    }

    filter_dict = {}
    if start_id > 0:
        filter_dict[">ID"] = start_id
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict

    resp = b24.call("crm.deal.list", params)

    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_deals: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}

    return resp

def b24_list_smart_items(entity_type_id: int, last_id: int = 0) -> Dict[str, Any]:
    """
    Получает смарт-процессы из Bitrix с оптимизацией (start=-1 + фильтр >ID).
    Использует рекомендацию Bitrix24 для оптимизации производительности.
    """
    params = {
        "entityTypeId": entity_type_id,
        "select": ["*"],
        "start": last_id,
        "order": {"id": "ASC"}
    }
    if last_id > 0:
        params["filter"] = {">id": last_id}
    print(f"DEBUG: b24_list_smart_items: Request params: entityTypeId={entity_type_id}, start={params['start']}, filter={params.get('filter')}", file=sys.stderr, flush=True)
    
    resp = b24.call("crm.item.list", params)
    
    # Проверяем, не заблокирован ли API
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_smart_items: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    
    return resp

def b24_list_contacts(start: int = 0, filter_params: Optional[Dict[str, Any]] = None, uf_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Получает контакты из Bitrix с оптимизацией (start=-1 + фильтр >ID).
    """
    select_list = ["*"]
    if uf_fields:
        select_list.extend(uf_fields)
    
    params = {
        "select": select_list,
        "start": start,
        "order": {"ID": "ASC"}
    }
    
    filter_dict = {}
    if start > 0:
        filter_dict[">ID"] = start
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict
    
    print(f"DEBUG: b24_list_contacts: Request params: select={select_list[:5]}..., start={params['start']}, filter={params.get('filter')}", file=sys.stderr, flush=True)
    
    resp = b24.call("crm.contact.list", params)
    
    # Проверяем, не заблокирован ли API
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_contacts: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    
    return resp

def b24_list_leads(start: int = 0, filter_params: Optional[Dict[str, Any]] = None, uf_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Получает лиды из Bitrix с оптимизацией (start=-1 + фильтр >ID).
    """
    select_list = ["*"]
    if uf_fields:
        select_list.extend(uf_fields)
    
    params = {
        "select": select_list,
        "start": start,
        "order": {"ID": "ASC"}
    }
    
    filter_dict = {}
    if start > 0:
        filter_dict[">ID"] = start
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict
    
    print(f"DEBUG: b24_list_leads: Request params: select={select_list[:5]}..., start={params['start']}, filter={params.get('filter')}", file=sys.stderr, flush=True)
    
    resp = b24.call("crm.lead.list", params)
    
    # Проверяем, не заблокирован ли API
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_leads: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    
    return resp

# -----------------------------
# Main schema sync
# -----------------------------
def sync_schema() -> Dict[str, Any]:
    conn = pg_conn()
    try:
        ensure_meta_tables(conn)

        deal_fields = fetch_deal_fields()
        deal_entity = {"entity_key": "deal", "entity_kind": "deal", "title": "CRM Deal", "entity_type_id": None}
        upsert_meta_entities(conn, [deal_entity])

        existing_cols = set(["id", "raw", "created_at", "updated_at"])
        deal_colmap: Dict[str, str] = {}
        for f in deal_fields.keys():
            base = sanitize_ident(f)
            deal_colmap[f] = unique_column_name(existing_cols, base)

        deal_table = table_name_for_entity("deal")
        ensure_table_base(conn, deal_table)

        deal_columns: List[Tuple[str, str]] = []
        for b24_field, meta in deal_fields.items():
            pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
            deal_columns.append((deal_colmap[b24_field], pgtype))

        ensure_columns(conn, deal_table, deal_columns)
        upsert_meta_fields(conn, "deal", deal_fields, deal_colmap)
        sync_userfield_titles(conn, "deal")

        # Синхронизируем контакты
        contact_fields = fetch_contact_fields()
        contact_entity = {"entity_key": "contact", "entity_kind": "contact", "title": "CRM Contact", "entity_type_id": None}
        upsert_meta_entities(conn, [contact_entity])
        
        existing_cols_contact = set(["id", "raw", "created_at", "updated_at"])
        contact_colmap: Dict[str, str] = {}
        for f in contact_fields.keys():
            base = sanitize_ident(f)
            contact_colmap[f] = unique_column_name(existing_cols_contact, base)
        
        contact_table = table_name_for_entity("contact")
        ensure_table_base(conn, contact_table)
        
        contact_columns: List[Tuple[str, str]] = []
        for b24_field, meta in contact_fields.items():
            pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
            contact_columns.append((contact_colmap[b24_field], pgtype))
        
        ensure_columns(conn, contact_table, contact_columns)
        upsert_meta_fields(conn, "contact", contact_fields, contact_colmap)
        sync_userfield_titles(conn, "contact")

        # Синхронизируем лиды
        lead_fields = fetch_lead_fields()
        lead_entity = {"entity_key": "lead", "entity_kind": "lead", "title": "CRM Lead", "entity_type_id": None}
        upsert_meta_entities(conn, [lead_entity])
        
        existing_cols_lead = set(["id", "raw", "created_at", "updated_at"])
        lead_colmap: Dict[str, str] = {}
        for f in lead_fields.keys():
            base = sanitize_ident(f)
            lead_colmap[f] = unique_column_name(existing_cols_lead, base)
        
        lead_table = table_name_for_entity("lead")
        ensure_table_base(conn, lead_table)
        
        lead_columns: List[Tuple[str, str]] = []
        for b24_field, meta in lead_fields.items():
            pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
            lead_columns.append((lead_colmap[b24_field], pgtype))
        
        ensure_columns(conn, lead_table, lead_columns)
        upsert_meta_fields(conn, "lead", lead_fields, lead_colmap)
        sync_userfield_titles(conn, "lead")

        # Синхронизируем классификатор источников: стандартные SOURCE + пользовательское поле Sursa
        sync_sources_from_status(conn)
        sync_sources_classifier(conn)

        types = fetch_smart_process_types()
        smart_entities: List[Dict[str, Any]] = []
        smart_results: List[Dict[str, Any]] = []

        for t in types:
            etid = t.get("entityTypeId") or t.get("ENTITY_TYPE_ID") or t.get("entity_type_id")
            if not etid:
                continue
            etid = int(etid)
            entity_key = f"sp:{etid}"
            smart_entities.append({
                "entity_key": entity_key,
                "entity_kind": "smart_process",
                "title": t.get("title") or t.get("TITLE") or f"SmartProcess {etid}",
                "entity_type_id": etid
            })

        if smart_entities:
            upsert_meta_entities(conn, smart_entities)

        for e in smart_entities:
            entity_key = e["entity_key"]
            etid = e["entity_type_id"]
            fields = fetch_smart_fields(etid)

            existing_cols = set(["id", "raw", "created_at", "updated_at"])
            colmap: Dict[str, str] = {}
            for f in fields.keys():
                base = sanitize_ident(f)
                colmap[f] = unique_column_name(existing_cols, base)

            table = table_name_for_entity(entity_key)
            ensure_table_base(conn, table)

            cols: List[Tuple[str, str]] = []
            for b24_field, meta in fields.items():
                pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
                cols.append((colmap[b24_field], pgtype))

            ensure_columns(conn, table, cols)
            upsert_meta_fields(conn, entity_key, fields, colmap)

            smart_results.append({"entity_key": entity_key, "table": table, "fields_count": len(fields)})

        return {
            "ok": True,
            "deal": {"table": deal_table, "fields_count": len(deal_fields)},
            "contact": {"table": contact_table, "fields_count": len(contact_fields)},
            "lead": {"table": lead_table, "fields_count": len(lead_fields)},
            "smart_processes": {"count": len(smart_entities), "items": smart_results[:50]}
        }
    finally:
        conn.close()

# -----------------------------
# Data sync (UPSERT) with cursor + time budget
# -----------------------------
def _is_unlimited(limit: int) -> bool:
    return limit <= 0

# Кэш для имен пользователей (чтобы не делать повторные запросы к Bitrix)
_user_name_cache: Dict[str, str] = {}

def sync_entity_data_deal(conn, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    entity_key = "deal"
    table = table_name_for_entity(entity_key)

    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})

    # Проверяем, есть ли колонка assigned_by_name в таблице (опционально)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = 'assigned_by_name'
        """, (table,))
        has_assigned_by_name_col = cur.fetchone() is not None
    if has_assigned_by_name_col and "assigned_by_name" not in col_order:
        col_order.append("assigned_by_name")

    # Загружаем UF поля из меты (лучше чем UF_*)
    uf_fields: List[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b24_field
                FROM b24_meta_fields
                WHERE entity_key = %s
                  AND b24_field ILIKE 'uf_%%'
            """, (entity_key,))
            uf_fields = [str(r[0]) for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        print(f"WARNING: sync_entity_data_deal: Failed to load UF fields: {e}", file=sys.stderr, flush=True)
        uf_fields = []

    # -------- helpers: собрать row (общая логика) --------
    def build_row_from_item(it: Dict[str, Any]) -> Optional[List[Any]]:
        deal_id = it.get("ID") or it.get("id")
        if not deal_id:
            return None

        row = {c: None for c in col_order}
        row["id"] = int(deal_id)
        row["raw"] = Json(it)

        # обычные поля по colmap
        for b24_field, meta in colmap.items():
            col = meta["column_name"]
            b24_type = meta.get("b24_type")
            is_multiple = meta.get("is_multiple", False)

            value = None
            if b24_field in it:
                value = it[b24_field]
            elif isinstance(it.get("fields"), dict) and b24_field in it["fields"]:
                value = it["fields"][b24_field]
            elif b24_field.upper() in it:
                value = it[b24_field.upper()]
            elif b24_field.lower() in it:
                value = it[b24_field.lower()]
            elif isinstance(it.get("fields"), dict) and b24_field.upper() in it["fields"]:
                value = it["fields"][b24_field.upper()]
            elif isinstance(it.get("fields"), dict) and b24_field.lower() in it["fields"]:
                value = it["fields"][b24_field.lower()]

            if value is not None:
                row[col] = normalize_value(value, b24_type, is_multiple)

        # assigned_by_name — берём только если Bitrix прислал (не долбим user.get лишний раз)
        if has_assigned_by_name_col and "assigned_by_name" in col_order:
            v = None
            if "ASSIGNED_BY_NAME" in it:
                v = it.get("ASSIGNED_BY_NAME")
            elif "assigned_by_name" in it:
                v = it.get("assigned_by_name")
            # иногда ASSIGNED_BY объект
            elif isinstance(it.get("ASSIGNED_BY"), dict):
                u = it["ASSIGNED_BY"]
                n = (u.get("NAME") or "").strip()
                ln = (u.get("LAST_NAME") or "").strip()
                v = (f"{n} {ln}".strip() or u.get("FULL_NAME") or None)

            row["assigned_by_name"] = (str(v).strip() if v else None)

        return [row.get(c) for c in col_order]

    # -------- 1) Инкремент: новые сделки по >ID (100% новых) --------
    total = 0
    last_id = validate_sync_cursor(conn, entity_key, table)
    started = time.time()

    while True:
        if time.time() - started >= time_budget_sec:
            break

        resp = b24_list_deals(
            start_id=last_id,
            start_offset=0,
            filter_params=None,
            uf_fields=uf_fields,
            order={"ID": "ASC"}
        )
        items, _ = normalize_list_result(resp)

        if not items:
            set_sync_cursor(conn, entity_key, last_id if last_id > 0 else 0)
            break

        batch_rows: List[List[Any]] = []
        max_seen = last_id

        for it in items:
            r = build_row_from_item(it)
            if not r:
                continue
            batch_rows.append(r)
            deal_id_val = r[col_order.index("id")]
            if deal_id_val and int(deal_id_val) > int(max_seen):
                max_seen = int(deal_id_val)

        if batch_rows:
            upsert_rows(conn, table, col_order, batch_rows)
            total += len(batch_rows)

        last_id = int(max_seen) if max_seen else last_id
        set_sync_cursor(conn, entity_key, last_id)

        if (not _is_unlimited(limit)) and total >= limit:
            break

        # Если Bitrix вернул “короткую” пачку — вероятно, новых больше нет, выходим
        if len(items) < 50:
            break

    # -------- 2) Today-pass: все сделки изменённые сегодня (100% актуальность дня) --------
    # Экономим запросы: ограничиваем число страниц за один запуск (если сегодня изменили очень много)
    tz_name = os.getenv("B24_TZ", "Europe/Chisinau")
    dt_from_utc = day_start_utc(tz_name)
    dt_from_str = dt_from_utc.isoformat()

    max_pages = int(os.getenv("DEAL_TODAY_MAX_PAGES", "10"))  # безопасный лимит
    page = 0
    offset = 0

    while True:
        if time.time() - started >= time_budget_sec:
            break
        if page >= max_pages:
            print(f"INFO: sync_entity_data_deal: today-pass reached max_pages={max_pages}, stop early to protect API", file=sys.stderr, flush=True)
            break

        resp2 = b24_list_deals(
            start_id=0,
            start_offset=offset,
            filter_params={">=DATE_MODIFY": dt_from_str},
            uf_fields=uf_fields,
            order={"DATE_MODIFY": "ASC", "ID": "ASC"},
        )
        items2, nxt2 = normalize_list_result(resp2)

        if not items2:
            break

        batch_rows2: List[List[Any]] = []
        for it in items2:
            r = build_row_from_item(it)
            if r:
                batch_rows2.append(r)

        if batch_rows2:
            upsert_rows(conn, table, col_order, batch_rows2)
            total += len(batch_rows2)

        page += 1
        if nxt2 is None:
            break
        offset = int(nxt2)

    return {"entity": "deal", "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}

def sync_entity_data_contact(conn, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    """Синхронизация данных контактов из Bitrix"""
    entity_key = "contact"
    table = table_name_for_entity(entity_key)
    
    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})
    
    # Получаем список UF полей
    uf_fields = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b24_field
                FROM b24_meta_fields
                WHERE entity_key = %s 
                  AND b24_field ILIKE 'uf_%%'
            """, (entity_key,))
            rows = cur.fetchall()
            for row in rows:
                if row and len(row) > 0:
                    uf_fields.append(str(row[0]))
    except Exception as e:
        print(f"WARNING: Failed to load UF fields for contacts: {e}", file=sys.stderr, flush=True)
        uf_fields = []
    
    total = 0
    # Валидируем курсор (offset пагинация через start/next)
    last_offset = validate_sync_cursor(conn, entity_key, table)
    
    started = time.time()
    while True:
        if time.time() - started >= time_budget_sec:
            break
        
        resp = b24_list_contacts(start=last_offset, filter_params=None, uf_fields=uf_fields)
        items, nxt = normalize_list_result(resp)
        
        if not items:
            set_sync_cursor(conn, entity_key, last_offset if last_offset > 0 else 0)
            break
        
        rows = []
        for it in items:
            contact_id = it.get("ID") or it.get("id")
            if not contact_id:
                continue
            
            row = {c: None for c in col_order}
            row["id"] = int(contact_id)
            row["raw"] = Json(it)
            
            for b24_field, meta in colmap.items():
                col = meta["column_name"]
                b24_type = meta.get("b24_type")
                is_multiple = meta.get("is_multiple", False)
                
                value = None
                if b24_field in it:
                    value = it[b24_field]
                elif b24_field.upper() in it:
                    value = it[b24_field.upper()]
                elif b24_field.lower() in it:
                    value = it[b24_field.lower()]
                
                if value is not None:
                    row[col] = normalize_value(value, b24_type, is_multiple)
            
            row_values = [row.get(c) for c in col_order]
            rows.append(row_values)
        
        upsert_rows(conn, table, col_order, rows)
        total += len(rows)
        
        # Пагинация через start/next
        if nxt is not None:
            last_offset = nxt
            set_sync_cursor(conn, entity_key, last_offset)
        else:
            last_seen_id = rows[-1][col_order.index("id")] if rows else last_offset
            set_sync_cursor(conn, entity_key, last_seen_id if last_seen_id else last_offset)
            break
        
        if (not _is_unlimited(limit)) and total >= limit:
            break
    
    return {"entity": "contact", "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}

def sync_entity_data_lead(conn, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    """Синхронизация данных лидов из Bitrix"""
    entity_key = "lead"
    table = table_name_for_entity(entity_key)
    
    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})
    
    # Получаем список UF полей
    uf_fields = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b24_field
                FROM b24_meta_fields
                WHERE entity_key = %s 
                  AND b24_field ILIKE 'uf_%%'
            """, (entity_key,))
            rows = cur.fetchall()
            for row in rows:
                if row and len(row) > 0:
                    uf_fields.append(str(row[0]))
    except Exception as e:
        print(f"WARNING: Failed to load UF fields for leads: {e}", file=sys.stderr, flush=True)
        uf_fields = []
    
    total = 0
    # Валидируем курсор (offset пагинация через start/next)
    last_offset = validate_sync_cursor(conn, entity_key, table)
    
    started = time.time()
    while True:
        if time.time() - started >= time_budget_sec:
            break
        
        resp = b24_list_leads(start=last_offset, filter_params=None, uf_fields=uf_fields)
        items, nxt = normalize_list_result(resp)
        
        if not items:
            set_sync_cursor(conn, entity_key, last_offset if last_offset > 0 else 0)
            break
        
        rows = []
        for it in items:
            lead_id = it.get("ID") or it.get("id")
            if not lead_id:
                continue
            
            row = {c: None for c in col_order}
            row["id"] = int(lead_id)
            row["raw"] = Json(it)
            
            for b24_field, meta in colmap.items():
                col = meta["column_name"]
                b24_type = meta.get("b24_type")
                is_multiple = meta.get("is_multiple", False)
                
                value = None
                if b24_field in it:
                    value = it[b24_field]
                elif b24_field.upper() in it:
                    value = it[b24_field.upper()]
                elif b24_field.lower() in it:
                    value = it[b24_field.lower()]
                
                if value is not None:
                    row[col] = normalize_value(value, b24_type, is_multiple)
            
            row_values = [row.get(c) for c in col_order]
            rows.append(row_values)
        
        upsert_rows(conn, table, col_order, rows)
        total += len(rows)
        
        # Пагинация через start/next
        if nxt is not None:
            last_offset = nxt
            set_sync_cursor(conn, entity_key, last_offset)
        else:
            last_seen_id = rows[-1][col_order.index("id")] if rows else last_offset
            set_sync_cursor(conn, entity_key, last_seen_id if last_seen_id else last_offset)
            break
        
        if (not _is_unlimited(limit)) and total >= limit:
            break
    
    return {"entity": "lead", "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}

def sync_entity_data_smart(conn, entity_type_id: int, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    entity_key = f"sp:{entity_type_id}"
    table = table_name_for_entity(entity_key)

    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})

    total = 0
    # Валидируем курсор (offset пагинация через start/next)
    last_offset = validate_sync_cursor(conn, entity_key, table)

    started = time.time()
    while True:
        if time.time() - started >= time_budget_sec:
            break

        resp = b24_list_smart_items(entity_type_id, last_id=last_offset)
        items, nxt = normalize_list_result(resp)

        if not items:
            set_sync_cursor(conn, entity_key, last_offset if last_offset > 0 else 0)
            break

        rows = []
        for it in items:
            item_id = it.get("id") or it.get("ID")
            if not item_id:
                continue
            
            row = {c: None for c in col_order}
            row["id"] = int(item_id)
            row["raw"] = Json(it)

            for b24_field, meta in colmap.items():
                col = meta["column_name"]
                b24_type = meta.get("b24_type")
                is_multiple = meta.get("is_multiple", False)
                
                # Просто берем значение из Bitrix и сохраняем в базу
                value = None
                
                # Проверяем в разных местах и регистрах
                if b24_field in it:
                    value = it[b24_field]
                elif isinstance(it.get("fields"), dict) and b24_field in it["fields"]:
                    value = it["fields"][b24_field]
                elif b24_field.upper() in it:
                    value = it[b24_field.upper()]
                elif b24_field.lower() in it:
                    value = it[b24_field.lower()]
                elif isinstance(it.get("fields"), dict) and b24_field.upper() in it["fields"]:
                    value = it["fields"][b24_field.upper()]
                elif isinstance(it.get("fields"), dict) and b24_field.lower() in it["fields"]:
                    value = it["fields"][b24_field.lower()]
                
                if value is not None:
                    row[col] = normalize_value(value, b24_type, is_multiple)

            rows.append([row[c] for c in col_order])

        upsert_rows(conn, table, col_order, rows)
        total += len(rows)

        # Пагинация через start/next
        if nxt is not None:
            last_offset = nxt
            set_sync_cursor(conn, entity_key, last_offset)
        else:
            last_seen_id = rows[-1][col_order.index("id")] if rows else last_offset
            set_sync_cursor(conn, entity_key, last_seen_id if last_seen_id else last_offset)
            break
        
        if (not _is_unlimited(limit)) and total >= limit:
            break
    
    return {"entity": entity_key, "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}



def sync_data(deal_limit: int, smart_limit: int, time_budget_sec: int, contact_limit: int = 0, lead_limit: int = 0) -> Dict[str, Any]:
    conn = pg_conn()
    try:
        ensure_meta_tables(conn)
        
        # Проверяем, существуют ли таблицы для контактов и лидов
        # Если нет - автоматически создаем их через sync_schema()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name IN ('b24_crm_contact', 'b24_crm_lead')
            """)
            existing_tables = {row[0] for row in cur.fetchall()}
            
            if 'b24_crm_contact' not in existing_tables or 'b24_crm_lead' not in existing_tables:
                print("INFO: sync_data: Tables for contacts/leads not found, running sync_schema() automatically...", file=sys.stderr, flush=True)
                try:
                    sync_schema()
                    print("INFO: sync_data: sync_schema() completed successfully", file=sys.stderr, flush=True)
                except Exception as e:
                    print(f"WARNING: sync_data: sync_schema() failed: {e}", file=sys.stderr, flush=True)
                    traceback.print_exc()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT entity_type_id
                FROM b24_meta_entities
                WHERE entity_kind = 'smart_process'
                ORDER BY entity_type_id
            """)
            smart_ids = [r[0] for r in cur.fetchall() if r[0] is not None]

        # split time budget: deals get 30%, contacts and leads get 20% each, smart share the rest (30%)
        t0 = max(1, int(time_budget_sec * 0.3))  # deals
        t1 = max(1, int(time_budget_sec * 0.2))  # contacts
        t2 = max(1, int(time_budget_sec * 0.2))  # leads
        t_rest = max(1, time_budget_sec - t0 - t1 - t2)  # smart processes
        per_smart = max(1, t_rest // max(1, len(smart_ids)))

        # Синхронизируем ВСЕ сделки (без фильтрации по статусу)
        deal_res = sync_entity_data_deal(conn, limit=deal_limit, time_budget_sec=t0)
        
        # Синхронизируем контакты
        contact_res = sync_entity_data_contact(conn, limit=contact_limit, time_budget_sec=t1)
        
        # Синхронизируем лиды
        lead_res = sync_entity_data_lead(conn, limit=lead_limit, time_budget_sec=t2)
        
        # Синхронизируем смарт-процессы
        smart_res = [sync_entity_data_smart(conn, int(etid), limit=smart_limit, time_budget_sec=per_smart) for etid in smart_ids]
        
        # Автоматически обновляем классификатор источников после синхронизации сделок
        # Это дополняет классификатор новыми источниками из сделок
        try:
            sync_sources_classifier(conn)
            print("INFO: sync_data: Sources classifier updated automatically", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: sync_data: Failed to update sources classifier: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()

        return {
            "ok": True, 
            "deal": deal_res, 
            "contact": contact_res,
            "lead": lead_res,
            "smart_processes": smart_res
        }
    finally:
        conn.close()

# -----------------------------
# Background auto-sync every 30 seconds
# -----------------------------
_sync_lock = threading.Lock()
_last_full_update_time = 0
FULL_UPDATE_INTERVAL_SEC = int(os.getenv("FULL_UPDATE_INTERVAL_SEC", "3600"))  # 1 час по умолчанию

def background_loop():
    global _last_full_update_time
    while True:
        if AUTO_SYNC_ENABLED:
            # Проверяем время - не синхронизируем с 00:00 до 06:00
            current_time_obj = datetime.now()
            current_hour = current_time_obj.hour
            # Пропускаем синхронизацию с 00:00 до 06:00 (включительно)
            if 0 <= current_hour < 6:
                print(f"INFO: background_loop: Skipping sync (night time: {current_hour:02d}:00)", file=sys.stderr, flush=True)
                time.sleep(AUTO_SYNC_INTERVAL_SEC)
                continue
            
            if _sync_lock.acquire(blocking=False):
                try:
                    # Обычная инкрементальная синхронизация
                    res_data = sync_data(
                        deal_limit=AUTO_SYNC_DEAL_LIMIT,
                        smart_limit=AUTO_SYNC_SMART_LIMIT,
                        time_budget_sec=SYNC_TIME_BUDGET_SEC,
                        contact_limit=AUTO_SYNC_CONTACT_LIMIT,
                        lead_limit=AUTO_SYNC_LEAD_LIMIT
                    )
                    print(
                        "AUTO SYNC OK:",
                        {
                            "deal": res_data.get("deal"),
                            "smart_sample": (res_data.get("smart_processes") or [])[:2],
                        },
                        flush=True
                    )
                    
                    # Периодически (раз в FULL_UPDATE_INTERVAL_SEC) обновляем assigned_by_name и справочники
                    current_time = time.time()
                    if current_time - _last_full_update_time >= FULL_UPDATE_INTERVAL_SEC:
                        # Справочники (воронки, стадии, enum) — подтягиваем новые значения из Bitrix
                        try:
                            print("INFO: background_loop: Running periodic sync reference data...", file=sys.stderr, flush=True)
                            run_sync_reference_data()
                        except Exception as e:
                            print(f"WARNING: background_loop: reference data sync failed: {e}", file=sys.stderr, flush=True)
                        print("INFO: background_loop: Starting periodic update of assigned_by_name", file=sys.stderr, flush=True)
                        try:
                            conn = pg_conn()
                            try:
                                table = table_name_for_entity("deal")
                                global _user_name_cache
                                _user_name_cache.clear()
                                
                                # Получаем список сделок без assigned_by_name (ограничиваем до 500 за раз)
                                with conn.cursor() as cur:
                                    cur.execute(f"""
                                        SELECT id, assigned_by_id
                                        FROM {table}
                                        WHERE assigned_by_id IS NOT NULL
                                          AND assigned_by_name IS NULL
                                        ORDER BY id DESC
                                        LIMIT 500
                                    """)
                                    deals_to_update = cur.fetchall()
                                
                                if deals_to_update:
                                    updated = 0
                                    for deal_id, assigned_by_id in deals_to_update:
                                        user_id_str = str(assigned_by_id).strip()
                                        
                                        if user_id_str in _user_name_cache:
                                            assigned_by_name = _user_name_cache[user_id_str]
                                        else:
                                            try:
                                                user_resp = b24.call("user.get", {"ID": user_id_str})
                                                if user_resp and "result" in user_resp and len(user_resp["result"]) > 0:
                                                    user = user_resp["result"][0]
                                                    name = user.get("NAME", "").strip()
                                                    last_name = user.get("LAST_NAME", "").strip()
                                                    if name and last_name:
                                                        assigned_by_name = f"{name} {last_name}"
                                                    elif name:
                                                        assigned_by_name = name
                                                    elif last_name:
                                                        assigned_by_name = last_name
                                                    elif user.get("FULL_NAME"):
                                                        assigned_by_name = str(user.get("FULL_NAME")).strip()
                                                    elif user.get("LOGIN"):
                                                        assigned_by_name = str(user.get("LOGIN")).strip()
                                                    else:
                                                        assigned_by_name = None
                                                    _user_name_cache[user_id_str] = assigned_by_name or user_id_str
                                                else:
                                                    assigned_by_name = None
                                                    _user_name_cache[user_id_str] = user_id_str
                                            except Exception as e:
                                                print(f"WARNING: Failed to get user name for deal {deal_id}: {e}", file=sys.stderr, flush=True)
                                                assigned_by_name = None
                                                _user_name_cache[user_id_str] = user_id_str
                                        
                                        if assigned_by_name and assigned_by_name != user_id_str:
                                            with conn.cursor() as cur:
                                                cur.execute(f"""
                                                    UPDATE {table}
                                                    SET assigned_by_name = %s
                                                    WHERE id = %s
                                                """, (assigned_by_name, deal_id))
                                                conn.commit()
                                                updated += 1
                                            try:
                                                _upsert_b24_user(conn, int(assigned_by_id), assigned_by_name)
                                            except Exception:
                                                pass
                                    
                                    print(f"INFO: background_loop: Updated {updated} deals with assigned_by_name", file=sys.stderr, flush=True)
                                else:
                                    print("INFO: background_loop: No deals need assigned_by_name update", file=sys.stderr, flush=True)
                                
                                _last_full_update_time = current_time
                            finally:
                                conn.close()
                        except Exception as e:
                            print(f"ERROR: background_loop: Failed to update assigned_by_name: {e}", file=sys.stderr, flush=True)
                            traceback.print_exc()
                except Exception:
                    traceback.print_exc()
                finally:
                    _sync_lock.release()
        time.sleep(AUTO_SYNC_INTERVAL_SEC)

def _initial_sync_thread():
    """Запускает начальную синхронизацию в отдельном потоке, чтобы не блокировать старт сервиса."""
    # Небольшая задержка, чтобы сервис успел запуститься
    time.sleep(2)
    print("INFO: _initial_sync_thread: Starting initial sync from Bitrix...", file=sys.stderr, flush=True)
    try:
        # Сначала синхронизируем схему (создаем таблицы и метаданные для всех сущностей)
        print("INFO: _initial_sync_thread: Running sync_schema() first to ensure all tables exist...", file=sys.stderr, flush=True)
        try:
            schema_result = sync_schema()
            print(f"INFO: _initial_sync_thread: sync_schema() completed: {schema_result}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: _initial_sync_thread: sync_schema() failed (will continue anyway): {e}", file=sys.stderr, flush=True)
            traceback.print_exc()

        # Справочники (воронки, стадии, enum) — чтобы entity-meta-data сразу показывал названия
        try:
            print("INFO: _initial_sync_thread: Running sync reference data (categories, stages, field enums)...", file=sys.stderr, flush=True)
            run_sync_reference_data()
            print("INFO: _initial_sync_thread: Reference data sync completed", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: _initial_sync_thread: reference data sync failed (will continue): {e}", file=sys.stderr, flush=True)
        
        # Затем синхронизируем данные с увеличенным time_budget для начальной загрузки
        initial_sync_result = sync_data(
            deal_limit=0,  # Без ограничений для начальной синхронизации
            smart_limit=0,
            time_budget_sec=300,  # 5 минут для начальной синхронизации
            contact_limit=0,  # Без ограничений для начальной синхронизации
            lead_limit=0  # Без ограничений для начальной синхронизации
        )
        print(f"INFO: _initial_sync_thread: Initial sync completed: {initial_sync_result}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"WARNING: _initial_sync_thread: Initial sync failed: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()


# -----------------------------
# WEBHOOK-ONLY MODE (outbound Bitrix events)
# -----------------------------
WEBHOOK_ONLY = os.getenv("WEBHOOK_ONLY", "0") == "1"

def logi(msg: str):
    print(msg, file=sys.stderr, flush=True)

def ensure_webhook_queue_schema() -> None:
    """
    Ensure queue table exists and has the columns we need.
    Matches your current schema (received_at, processed_at, etc.) and adds missing columns safely.
    """
    conn = None
    try:
        conn = pg_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.b24_webhook_queue (
            id bigserial PRIMARY KEY,
            entity_key text NOT NULL,
            entity_id bigint NOT NULL,
            event text,
            received_at timestamptz NOT NULL DEFAULT now(),
            processed_at timestamptz,
            status text NOT NULL DEFAULT 'new',
            attempts int NOT NULL DEFAULT 0,
            last_error text,
            event_name text,
            payload jsonb,
            next_run_at timestamptz DEFAULT now(),
            created_at timestamptz DEFAULT now()
        );
        """)
        # add columns if table already existed
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS created_at timestamptz;")
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS event_name text;")
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS payload jsonb;")
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS next_run_at timestamptz;")
        # defaults (safe)
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN received_at SET DEFAULT now();")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN created_at SET DEFAULT now();")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN next_run_at SET DEFAULT now();")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN status SET DEFAULT 'new';")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN attempts SET DEFAULT 0;")
        cur.close()
    except Exception as e:
        logi(f"ERROR: ensure_webhook_queue_schema: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

def _extract_int(v) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None

def _guess_entity_from_event(event_name: str, payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], str]:
    """
    Returns (entity_key, entity_id, normalized_event_name)
    entity_key examples: 'deal', 'contact', 'lead', 'sp:1164'
    """
    en = (event_name or payload.get("event") or payload.get("event_name") or payload.get("EVENT_NAME") or "").strip()
    en_up = en.upper()

    # try find ID
    def pick_id() -> Optional[int]:
        # direct fields
        for k in ("id", "ID", "entity_id", "ENTITY_ID"):
            x = _extract_int(payload.get(k))
            if x:
                return x
        # common Bitrix outbound shape: data / FIELDS
        data = payload.get("data") or payload.get("DATA")
        if isinstance(data, dict):
            fields = data.get("FIELDS") or data.get("fields") or data
            if isinstance(fields, dict):
                x = _extract_int(fields.get("ID") or fields.get("id"))
                if x:
                    return x
        fields = payload.get("FIELDS") or payload.get("fields")
        if isinstance(fields, dict):
            x = _extract_int(fields.get("ID") or fields.get("id"))
            if x:
                return x
        return None

    entity_id = pick_id()

    # smart process entityTypeId
    entity_type_id = None
    for k in ("entityTypeId", "ENTITY_TYPE_ID", "entity_type_id", "ENTITYTYPEID"):
        entity_type_id = _extract_int(payload.get(k))
        if entity_type_id:
            break
    if not entity_type_id:
        data = payload.get("data") or payload.get("DATA")
        if isinstance(data, dict):
            entity_type_id = _extract_int(data.get("entityTypeId") or data.get("ENTITY_TYPE_ID") or data.get("ENTITYTYPEID"))

    if "DEAL" in en_up:
        return ("deal", entity_id, en)
    if "CONTACT" in en_up:
        return ("contact", entity_id, en)
    if "LEAD" in en_up:
        return ("lead", entity_id, en)
    if entity_type_id:
        return (f"sp:{int(entity_type_id)}", entity_id, en)

    ek = payload.get("entity_key")
    if isinstance(ek, str) and ek.strip():
        return (ek.strip(), entity_id, en)

    return (None, entity_id, en)

def _enqueue_webhook_event(entity_key: str, entity_id: int, event_name: str, payload: Dict[str, Any]) -> None:
    conn = None
    try:
        conn = pg_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.b24_webhook_queue (entity_key, entity_id, event_name, event, payload, status, attempts, next_run_at, received_at, created_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, 'new', 0, now(), now(), now())
            """, (entity_key, entity_id, event_name, event_name, json.dumps(payload, ensure_ascii=False)))
    except Exception as e:
        logi(f"ERROR: _enqueue_webhook_event: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

def _bitrix_get_one(entity_key: str, entity_id: int) -> Optional[Dict[str, Any]]:
    try:
        if entity_key == "deal":
            resp = b24.call("crm.deal.get", {"id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            return (resp.get("result") if isinstance(resp, dict) else None)

        if entity_key == "contact":
            resp = b24.call("crm.contact.get", {"id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            return (resp.get("result") if isinstance(resp, dict) else None)

        if entity_key == "lead":
            resp = b24.call("crm.lead.get", {"id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            return (resp.get("result") if isinstance(resp, dict) else None)

        if entity_key.startswith("sp:"):
            etid = int(entity_key.split(":", 1)[1])
            resp = b24.call("crm.item.get", {"entityTypeId": etid, "id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            if not isinstance(resp, dict):
                return None
            r = resp.get("result") or {}
            if isinstance(r, dict):
                return r.get("item") or None
            return None
    except Exception as e:
        logi(f"ERROR: _bitrix_get_one({entity_key},{entity_id}): {e}")
        traceback.print_exc()
    return None

def _upsert_single_item(conn, entity_key: str, item: Dict[str, Any]) -> bool:
    table = table_name_for_entity(entity_key)
    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    if not colmap:
        logi(f"WARNING: webhook upsert: no colmap for {entity_key} (run schema sync once)")
        return False

    # Determine id field
    raw_id = item.get("ID") if "ID" in item else item.get("id")
    entity_id = _extract_int(raw_id)
    if not entity_id:
        return False

    # Build column order
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})
    # keep updated_at
    if "updated_at" not in col_order:
        col_order.append("updated_at")

    row = {c: None for c in col_order}
    row["id"] = int(entity_id)
    row["raw"] = Json(item)
    row["updated_at"] = datetime.now(timezone.utc)

    for b24_field, meta in colmap.items():
        col = meta["column_name"]
        b24_type = meta.get("b24_type")
        is_multiple = meta.get("is_multiple", False)

        value = None
        if b24_field in item:
            value = item[b24_field]
        elif b24_field.upper() in item:
            value = item[b24_field.upper()]
        elif b24_field.lower() in item:
            value = item[b24_field.lower()]
        elif isinstance(item.get("fields"), dict) and b24_field in item["fields"]:
            value = item["fields"][b24_field]

        if value is not None:
            row[col] = normalize_value(value, b24_type, is_multiple)

    upsert_rows(conn, table, col_order, [[row.get(c) for c in col_order]])
    return True

def webhook_queue_worker(stop_event: threading.Event) -> None:
    logi("INFO: webhook_queue_worker started")
    while not stop_event.is_set():
        try:
            conn = pg_conn()
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, entity_key, entity_id,
                           COALESCE(event_name, event) AS event_name,
                           attempts
                    FROM public.b24_webhook_queue
                    WHERE status IN ('new','retry','pending')
                      AND (next_run_at IS NULL OR next_run_at <= now())
                    ORDER BY id
                    LIMIT 10
                """)
                jobs = cur.fetchall() or []
            conn.close()

            if not jobs:
                time.sleep(1.0)
                continue

            for job in jobs:
                if stop_event.is_set():
                    break
                qid = int(job["id"])
                ek = str(job["entity_key"])
                eid = int(job["entity_id"])
                ev = str(job.get("event_name") or "")
                attempts = int(job.get("attempts") or 0)

                # mark processing
                connp = pg_conn(); connp.autocommit = True
                with connp.cursor() as curp:
                    curp.execute("UPDATE public.b24_webhook_queue SET status='processing' WHERE id=%s", (qid,))
                connp.close()

                # delete event -> just mark done
                if "DELETE" in ev.upper() or "REMOVE" in ev.upper():
                    connm = pg_conn(); connm.autocommit=True
                    with connm.cursor() as curm:
                        curm.execute("UPDATE public.b24_webhook_queue SET status='done', processed_at=now(), last_error=NULL WHERE id=%s", (qid,))
                    connm.close()
                    continue

                item = _bitrix_get_one(ek, eid)
                if not item:
                    backoff = min(300, 5 * (attempts + 1))
                    connr = pg_conn(); connr.autocommit=True
                    with connr.cursor() as curr:
                        curr.execute("""
                            UPDATE public.b24_webhook_queue
                            SET status='retry', attempts=attempts+1,
                                last_error=%s,
                                next_run_at=now() + (%s || ' seconds')::interval
                            WHERE id=%s
                        """, ("bitrix blocked / empty", backoff, qid))
                    connr.close()
                    continue

                connu = pg_conn(); connu.autocommit=True
                ok = False
                try:
                    ok = _upsert_single_item(connu, ek, item)
                finally:
                    connu.close()

                connf = pg_conn(); connf.autocommit=True
                with connf.cursor() as curf:
                    if ok:
                        curf.execute("UPDATE public.b24_webhook_queue SET status='done', processed_at=now(), last_error=NULL WHERE id=%s", (qid,))
                    else:
                        backoff = min(300, 5 * (attempts + 1))
                        curf.execute("""
                            UPDATE public.b24_webhook_queue
                            SET status='retry', attempts=attempts+1,
                                last_error=%s,
                                next_run_at=now() + (%s || ' seconds')::interval
                            WHERE id=%s
                        """, ("upsert failed", backoff, qid))
                connf.close()

        except Exception as e:
            logi(f"ERROR: webhook_queue_worker: {e}")
            traceback.print_exc()
            time.sleep(2.0)

    logi("INFO: webhook_queue_worker stopped")

WEBHOOK_WORKER_STOP = threading.Event()
WEBHOOK_WORKER_THREAD: Optional[threading.Thread] = None

@app.post("/webhooks/b24/dynamic-item-update")
async def b24_dynamic_item_update(request: Request):
    """
    Receiver for Bitrix outbound webhooks (create/update/delete) for deals/leads/contacts/smart-process.
    Works WITHOUT python-multipart:
      - application/json -> request.json()
      - application/x-www-form-urlencoded -> parse raw body via urllib.parse
    Supports Bitrix keys like data[FIELDS][ID], data[FIELDS][ENTITY_TYPE_ID].
    """
    try:
        import urllib.parse

        ct = (request.headers.get("content-type") or "").lower()

        # --- 1) payload parsing (NO python-multipart needed) ---
        payload: dict = {}

        if "application/json" in ct:
            payload = await request.json()
        else:
            # Bitrix чаще всего шлёт x-www-form-urlencoded
            raw = await request.body()
            s = raw.decode("utf-8", errors="ignore")
            qs = urllib.parse.parse_qs(s, keep_blank_values=True)

            # превратим qs: {k:[v]} -> {k:v}
            payload = {k: (v[0] if isinstance(v, list) and v else "") for k, v in qs.items()}

        # --- 2) helpers to read common Bitrix keys ---
        def _get_first(*keys: str) -> str:
            for k in keys:
                v = payload.get(k)
                if v is None:
                    continue
                v = str(v).strip()
                if v != "":
                    return v
            return ""

        event_name = _get_first("event", "event_name", "EVENT_NAME")

        # Bitrix формат:
        #  - deal/lead/contact: data[FIELDS][ID]
        #  - dynamic items: data[FIELDS][ID] + data[FIELDS][ENTITY_TYPE_ID]
        entity_id_str = _get_first("data[FIELDS][ID]", "data[ID]", "ID", "id")
        entity_type_id_str = _get_first("data[FIELDS][ENTITY_TYPE_ID]", "data[ENTITY_TYPE_ID]", "ENTITY_TYPE_ID")

        # --- 3) detect entity_key from event ---
        # deal/lead/contact events
        ev = (event_name or "").upper()

        entity_key = ""
        norm_event = event_name or ""

        if "ONCRMDEAL" in ev:
            entity_key = "deal"
        elif "ONCRMLEAD" in ev:
            entity_key = "lead"
        elif "ONCRMCONTACT" in ev:
            entity_key = "contact"
        elif "ONCRMCOMPANY" in ev:
            entity_key = "company"
        elif "ONCRMDYNAMICITEM" in ev or "DYNAMIC" in ev:
            # smart-process / dynamic items
            if not entity_type_id_str:
                logi(
                    f"WARNING: dynamic webhook without ENTITY_TYPE_ID "
                    f"(id={entity_id_str}, event={event_name}). Skipping."
                )
                return {"ok": True, "queued": False}

            # ВАЖНО: сохраняем entity_key как sp:<ENTITY_TYPE_ID>
            entity_key = f"sp:{int(entity_type_id_str)}"

        # --- 4) validate entity_id ---
        if not entity_key or not entity_id_str:
            logi(
                f"WARNING: webhook parse failed. "
                f"content-type={ct}, len={len(str(payload))}, keys={list(payload.keys())[:30]}"
            )
            return {"ok": True, "queued": False}

        try:
            entity_id = int(entity_id_str)
        except Exception:
            logi(f"WARNING: webhook parse failed: cannot int(entity_id) from '{entity_id_str}'")
            return {"ok": True, "queued": False}

        # --- 5) enqueue ---
        _enqueue_webhook_event(entity_key, entity_id, norm_event, payload)
        return {"ok": True, "queued": True, "entity_key": entity_key, "entity_id": entity_id}

    except Exception as e:
        logi(f"ERROR: webhook endpoint: {e}")
        traceback.print_exc()
        return {"ok": False}


@app.on_event("startup")
def on_startup():

    # WEBHOOK ONLY: do not poll Bitrix, process only outbound events
    if WEBHOOK_ONLY:
        ensure_webhook_queue_schema()
        global WEBHOOK_WORKER_THREAD
        if WEBHOOK_WORKER_THREAD is None or not WEBHOOK_WORKER_THREAD.is_alive():
            WEBHOOK_WORKER_THREAD = threading.Thread(target=webhook_queue_worker, args=(WEBHOOK_WORKER_STOP,), daemon=True)
            WEBHOOK_WORKER_THREAD.start()
        print("WEBHOOK ONLY MODE: polling is disabled; waiting for outbound Bitrix events...", flush=True)
        return
    # Синхронизируем данные из Bitrix сразу при старте сервиса (в отдельном потоке, чтобы не блокировать запуск)
    # Это обеспечивает: БИТРИКС -> БАЗА -> PDF
    initial_sync_thread = threading.Thread(target=_initial_sync_thread, daemon=True)
    initial_sync_thread.start()
    
    # Запускаем фоновую синхронизацию каждые 30 секунд
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    print(
        "AUTO SYNC STARTED:",
        {
            "enabled": AUTO_SYNC_ENABLED,
            "interval_sec": AUTO_SYNC_INTERVAL_SEC,
            "deal_limit": AUTO_SYNC_DEAL_LIMIT,
            "smart_limit": AUTO_SYNC_SMART_LIMIT,
            "time_budget_sec": SYNC_TIME_BUDGET_SEC,
        },
        flush=True
    )

# -----------------------------
# API endpoints
# -----------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "auto_sync": {
            "enabled": AUTO_SYNC_ENABLED,
            "interval_sec": AUTO_SYNC_INTERVAL_SEC,
            "deal_limit": AUTO_SYNC_DEAL_LIMIT,
            "smart_limit": AUTO_SYNC_SMART_LIMIT,
            "time_budget_sec": SYNC_TIME_BUDGET_SEC,
        }
    }

@app.post("/sync/schema")
def sync_schema_endpoint():
    try:
        return sync_schema()
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))

@app.post("/sync/sources-classifier")
def sync_sources_classifier_endpoint():
    """
    Ручной запуск синхронизации классификатора источников.
    Заполняет b24_classifier_sources из enum значений поля источника сделок.
    """
    conn = pg_conn()
    try:
        print(f"INFO: sync_sources_classifier_endpoint: Starting manual sync", file=sys.stderr, flush=True)
        sync_sources_classifier(conn)
        # Проверяем, сколько записей в классификаторе
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM b24_classifier_sources")
            count = cur.fetchone()[0]
        print(f"INFO: sync_sources_classifier_endpoint: Completed. Total sources in classifier: {count}", file=sys.stderr, flush=True)
        return {
            "ok": True,
            "message": f"Sources classifier synced. Total sources: {count}",
            "count": count
        }
    except Exception as e:
        print(f"ERROR: sync_sources_classifier_endpoint: Exception: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


def _debug_bitrix_calls() -> Dict[str, Any]:
    """Пробует вызвать Bitrix API и возвращает ответы/ошибки для отладки (без записи в БД)."""
    out: Dict[str, Any] = {"categories": {}, "stages": {}, "deal_userfields": {}}
    # 1) Категории
    for method, params in [("crm.category.list", {"entityTypeId": 2}), ("crm.dealcategory.list", {})]:
        try:
            data = b24.call(method, params)
            err = data.get("error")
            result = data.get("result")
            if err:
                out["categories"][method] = {"error": err, "error_description": data.get("error_description", "")}
            else:
                rtype = type(result).__name__
                if isinstance(result, dict):
                    keys = list(result.keys())[:20]
                    out["categories"][method] = {"result_type": rtype, "result_keys": keys, "result_empty": not result}
                elif isinstance(result, list):
                    out["categories"][method] = {"result_type": rtype, "count": len(result), "result_empty": not result}
                else:
                    out["categories"][method] = {"result_type": rtype, "result_empty": not result}
        except Exception as e:
            out["categories"][method] = {"exception": str(e), "exception_type": type(e).__name__}
    # 2) Стадии
    try:
        data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_STAGE"}})
        err = data.get("error")
        result = data.get("result")
        if err:
            out["stages"] = {"error": err, "error_description": data.get("error_description", "")}
        else:
            rtype = type(result).__name__
            if isinstance(result, list):
                out["stages"] = {"result_type": rtype, "count": len(result), "result_empty": not result}
            elif isinstance(result, dict):
                out["stages"] = {"result_type": rtype, "result_keys": list(result.keys())[:20], "result_empty": not result}
            else:
                out["stages"] = {"result_type": rtype, "result_empty": not result}
    except Exception as e:
        out["stages"] = {"exception": str(e), "exception_type": type(e).__name__}
    # 3) Поля сделок (userfield.list) — дамп одного поля, чтобы увидеть ключи (listLabel и т.д.)
    try:
        data = b24.call("crm.deal.userfield.list", {})
        err = data.get("error")
        result = data.get("result")
        if err:
            out["deal_userfields"] = {"error": err, "error_description": data.get("error_description", "")}
        else:
            rtype = type(result).__name__
            sample: Dict[str, Any] = {}
            if isinstance(result, dict):
                keys = list(result.keys())[:30]
                sample["result_type"] = rtype
                sample["result_keys"] = keys
                sample["result_empty"] = not result
                # Полный объект одного поля (например UF_CRM_1733346976), чтобы увидеть listLabel/editFormLabel
                for k in ("UF_CRM_1733346976", "UF_CRM_1749211409067") + tuple(keys[:2]):
                    v = result.get(k) if isinstance(result, dict) else None
                    if isinstance(v, dict):
                        sample["sample_field_key"] = k
                        sample["sample_field"] = v
                        break
                if not sample.get("sample_field") and result and isinstance(result, dict):
                    first_key = next(iter(result.keys()), None)
                    if first_key:
                        sample["sample_field_key"] = first_key
                        sample["sample_field"] = result.get(first_key)
            elif isinstance(result, list) and result:
                sample["result_type"] = rtype
                sample["count"] = len(result)
                sample["result_empty"] = not result
                first = result[0] if isinstance(result[0], dict) else None
                if first:
                    sample["sample_field_key"] = first.get("fieldName") or first.get("FIELD_NAME") or "?"
                    sample["sample_field"] = first
            else:
                sample["result_type"] = rtype
                sample["result_empty"] = not result
            out["deal_userfields"] = sample
    except Exception as e:
        out["deal_userfields"] = {"exception": str(e), "exception_type": type(e).__name__}
    # 4) Типы сделок (DEAL_TYPE)
    try:
        data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_TYPE"}})
        err = data.get("error")
        result = data.get("result")
        if err:
            out["deal_types"] = {"error": err, "error_description": data.get("error_description", "")}
        else:
            rtype = type(result).__name__
            if isinstance(result, list):
                out["deal_types"] = {"result_type": rtype, "count": len(result), "result_empty": not result}
            elif isinstance(result, dict):
                out["deal_types"] = {"result_type": rtype, "result_keys": list(result.keys())[:20], "result_empty": not result}
            else:
                out["deal_types"] = {"result_type": rtype, "result_empty": not result}
    except Exception as e:
        out["deal_types"] = {"exception": str(e), "exception_type": type(e).__name__}
    return out


def run_sync_reference_data() -> None:
    """Запускает синхронизацию справочников (воронки, стадии, типы сделок, enum, названия полей UF_CRM_*) в БД."""
    conn = pg_conn()
    try:
        sync_deal_categories(conn)
        sync_deal_stages(conn)
        sync_deal_types(conn)
        sync_field_enums(conn, "deal")
        sync_field_enums(conn, "contact")
        sync_field_enums(conn, "lead")
        sync_userfield_titles(conn, "deal")
        sync_userfield_titles(conn, "contact")
        sync_userfield_titles(conn, "lead")
    except Exception as e:
        print(f"WARNING: run_sync_reference_data: {e}", file=sys.stderr, flush=True)
    finally:
        conn.close()


@app.post("/sync/reference-data")
def sync_reference_data_endpoint(debug: Optional[str] = Query(None, description="1 = только отладка Bitrix, без записи в БД")):
    """
    Синхронизирует справочники: воронки (b24_deal_categories), стадии (b24_deal_stages),
    enum-значения полей UF_CRM_* и др. (b24_field_enum) из Bitrix API.
    Вызывать после /sync/schema и при необходимости для обновления подписей.
    Параметр ?debug=1 — в ответ добавится debug с сырыми ответами/ошибками Bitrix (без записи в БД).
    """
    if debug == "1":
        try:
            debug_info = _debug_bitrix_calls()
            return {"ok": True, "message": "Debug only (no sync)", "debug": debug_info}
        except Exception as e:
            traceback.print_exc()
            return {"ok": False, "message": str(e), "debug_exception": repr(e)}
    conn = pg_conn()
    all_notes: List[str] = []
    try:
        cat_rows, cat_notes = sync_deal_categories(conn)
        all_notes.extend([f"categories: {n}" for n in cat_notes])
        stage_rows, stage_notes = sync_deal_stages(conn)
        all_notes.extend([f"stages: {n}" for n in stage_notes])
        sync_deal_types(conn)
        sync_sources_from_status(conn)
        sync_sources_classifier(conn)
        enum_deal_n, enum_deal_notes = sync_field_enums(conn, "deal")
        all_notes.extend(enum_deal_notes)
        sync_field_enums(conn, "contact")
        sync_field_enums(conn, "lead")
        titles_deal = sync_userfield_titles(conn, "deal")
        titles_contact = sync_userfield_titles(conn, "contact")
        titles_lead = sync_userfield_titles(conn, "lead")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM b24_deal_categories")
            cat_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_deal_stages")
            stage_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_field_enum")
            enum_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_classifier_sources")
            sources_count = cur.fetchone()[0]
        out = {
            "ok": True,
            "message": "Reference data synced",
            "categories": cat_count,
            "stages": stage_count,
            "sources": sources_count,
            "field_enum_values": enum_count,
            "userfield_titles_updated": {"deal": titles_deal, "contact": titles_contact, "lead": titles_lead},
        }
        if cat_count == 0 or stage_count == 0 or enum_count == 0:
            out["debug_notes"] = all_notes
        return out
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


@app.get("/api/data/sources-classifier")
def get_sources_classifier():
    """
    Возвращает классификатор источников (sursa) из базы данных.
    Используется для получения mapping ID -> название источника.
    """
    conn = pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT source_id, source_name
                FROM b24_classifier_sources
                ORDER BY source_id
            """)
            rows = cur.fetchall()
        
        # Формируем словарь для удобства использования
        classifier = {}
        for row in rows:
            classifier[str(row["source_id"])] = str(row["source_name"])
        
        return {
            "ok": True,
            "count": len(classifier),
            "classifier": classifier,
            "sources": [{"id": row["source_id"], "name": row["source_name"]} for row in rows]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


    # Достаём entityTypeId и itemId из разных форматов payload Bitrix
    data = payload.get("data") or {}
    fields = data.get("FIELDS") or data.get("fields") or {}

    raw_entity_type_id = (
        payload.get("entityTypeId")
        or payload.get("entity_type_id")
        or data.get("entityTypeId")
        or data.get("entity_type_id")
    )
    raw_item_id = (
        payload.get("id")
        or payload.get("item_id")
        or fields.get("ID")
        or fields.get("id")
    )

    try:
        entity_type_id = int(raw_entity_type_id)
        item_id = int(raw_item_id)
    except Exception:
        raise HTTPException(status_code=400, detail="entityTypeId or item ID is missing/invalid")

    print(f"INFO: webhook_b24_dynamic_item_update: entityTypeId={entity_type_id}, id={item_id}", file=sys.stderr, flush=True)

    resp = b24.call("crm.item.get", {"entityTypeId": entity_type_id, "id": item_id})
    if not isinstance(resp, dict):
        raise HTTPException(status_code=502, detail="crm.item.get returned invalid response")

    result = resp.get("result") if isinstance(resp.get("result"), dict) else resp.get("result")
    item = None
    if isinstance(result, dict) and "item" in result:
        item = result["item"]
    elif isinstance(result, dict):
        item = result

    if not isinstance(item, dict):
        raise HTTPException(status_code=502, detail="crm.item.get returned no item")

    return upsert_single_smart_item(entity_type_id, item)

@app.post("/sync/data")
def sync_data_endpoint(deal_limit: int = 0, smart_limit: int = 0, time_budget_sec: int = SYNC_TIME_BUDGET_SEC, contact_limit: int = 0, lead_limit: int = 0):
    """
    deal_limit=0 / smart_limit=0 => unlimited (eventually), but still time_budget_sec applies to avoid 429.
    """
    try:
        return sync_data(deal_limit=deal_limit, smart_limit=smart_limit, time_budget_sec=time_budget_sec, contact_limit=contact_limit, lead_limit=lead_limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))

@app.post("/sync/data/full")
def sync_data_full_endpoint():
    """
    Принудительная полная синхронизация всех сделок и smart processes.
    Запускается в фоновом потоке, чтобы не блокировать ответ.
    Возвращает сразу, синхронизация продолжается в фоне.
    """
    def _full_sync():
        try:
            print("INFO: sync_data_full_endpoint: Starting full sync in background...", file=sys.stderr, flush=True)
            # Полная синхронизация без ограничений по времени и количеству
            result = sync_data(
                deal_limit=0,  # Без ограничений
                smart_limit=0,  # Без ограничений
                time_budget_sec=3600,  # 1 час на синхронизацию
                contact_limit=0,  # Без ограничений
                lead_limit=0  # Без ограничений
            )
            print(f"INFO: sync_data_full_endpoint: Full sync completed: {result}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"ERROR: sync_data_full_endpoint: Full sync failed: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()
    
    # Запускаем в отдельном потоке
    t = threading.Thread(target=_full_sync, daemon=True)
    t.start()
    
    return {
        "ok": True,
        "message": "Full sync started in background. Check logs for progress.",
        "note": "This may take several minutes depending on the number of deals."
    }

@app.post("/sync/update-assigned-by-names")
def update_assigned_by_names_endpoint(limit: int = 1000, time_budget_sec: int = 60):
    """
    Принудительно обновляет assigned_by_name для всех сделок через Bitrix API.
    Обрабатывает сделки, у которых есть assigned_by_id, но нет assigned_by_name.
    """
    conn = pg_conn()
    try:
        table = table_name_for_entity("deal")
        global _user_name_cache
        _user_name_cache.clear()
        
        # Получаем список сделок, у которых есть assigned_by_id, но нет assigned_by_name
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, assigned_by_id
                FROM {table}
                WHERE assigned_by_id IS NOT NULL
                  AND assigned_by_name IS NULL
                ORDER BY id DESC
                LIMIT %s
            """, (limit,))
            deals_to_update = cur.fetchall()
        
        if not deals_to_update:
            return {"ok": True, "message": "No deals need updating", "updated": 0}
        
        updated = 0
        start_time = time.time()
        
        for deal_id, assigned_by_id in deals_to_update:
            # Проверяем time budget
            if time.time() - start_time >= time_budget_sec:
                print(f"INFO: update_assigned_by_names: Time budget exceeded, stopping. Updated {updated}/{len(deals_to_update)}", file=sys.stderr, flush=True)
                break
            
            user_id_str = str(assigned_by_id).strip()
            
            # Проверяем кэш
            if user_id_str in _user_name_cache:
                assigned_by_name = _user_name_cache[user_id_str]
            else:
                try:
                    user_resp = b24.call("user.get", {"ID": user_id_str})
                    if user_resp and "result" in user_resp and len(user_resp["result"]) > 0:
                        user = user_resp["result"][0]
                        name = user.get("NAME", "").strip()
                        last_name = user.get("LAST_NAME", "").strip()
                        if name and last_name:
                            assigned_by_name = f"{name} {last_name}"
                        elif name:
                            assigned_by_name = name
                        elif last_name:
                            assigned_by_name = last_name
                        elif user.get("FULL_NAME"):
                            assigned_by_name = str(user.get("FULL_NAME")).strip()
                        elif user.get("LOGIN"):
                            assigned_by_name = str(user.get("LOGIN")).strip()
                        else:
                            assigned_by_name = None
                        _user_name_cache[user_id_str] = assigned_by_name or user_id_str
                    else:
                        assigned_by_name = None
                        _user_name_cache[user_id_str] = user_id_str
                except Exception as e:
                    print(f"WARNING: Failed to get user name for deal {deal_id}, user_id {user_id_str}: {e}", file=sys.stderr, flush=True)
                    assigned_by_name = None
                    _user_name_cache[user_id_str] = user_id_str
            
            # Обновляем в базе
            if assigned_by_name and assigned_by_name != user_id_str:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {table}
                        SET assigned_by_name = %s
                        WHERE id = %s
                    """, (assigned_by_name, deal_id))
                    conn.commit()
                    updated += 1
                try:
                    _upsert_b24_user(conn, int(assigned_by_id), assigned_by_name)
                except Exception:
                    pass
        
        return {"ok": True, "updated": updated, "total": len(deals_to_update)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


def _collect_user_ids_from_tables(conn) -> List[int]:
    """Собрать все уникальные user ID из колонок deal/contact/lead (assigned_by_id, created_by_id и т.д.)."""
    user_cols = ["assigned_by_id", "created_by_id", "modified_by_id", "last_activity_by", "moved_by_id"]
    tables = [
        ("b24_crm_deal", user_cols),
        ("b24_crm_contact", user_cols),
        ("b24_crm_lead", user_cols),
    ]
    seen: set = set()
    with conn.cursor() as cur:
        for table, cols in tables:
            try:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = ANY(%s)
                """, (table, cols))
                existing = [r[0] for r in cur.fetchall() if r and r[0]]
                if not existing:
                    continue
                for col in existing:
                    cur.execute(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL')
                    for row in cur.fetchall() or []:
                        if row and row[0] is not None:
                            try:
                                uid = int(row[0])
                                if uid > 0:
                                    seen.add(uid)
                            except (TypeError, ValueError):
                                pass
            except Exception as e:
                print(f"WARNING: _collect_user_ids_from_tables {table}: {e}", file=sys.stderr, flush=True)
    return list(seen)


def _user_record_to_name(u: Dict[str, Any]) -> Optional[str]:
    """Из ответа user.get собрать имя пользователя."""
    name = u.get("NAME", "").strip()
    last = u.get("LAST_NAME", "").strip()
    if name and last:
        return f"{name} {last}"
    if name:
        return name
    if last:
        return last
    return (u.get("FULL_NAME") or u.get("LOGIN") or "").strip() or None


def sync_all_users_from_bitrix(conn, time_budget_sec: int = 600) -> Dict[str, Any]:
    """
    Загрузить всех пользователей из Bitrix user.get (пагинация start=0, 50, 100, ...)
    и заполнить b24_users. Один вызов — полный справочник пользователей.
    """
    started = time.time()
    synced = 0
    start = 0
    page_size = 50
    while time.time() - started < time_budget_sec:
        try:
            resp = b24.call("user.get", {"start": start})
            result = resp.get("result") if isinstance(resp, dict) else []
            if not isinstance(result, list):
                break
            if not result:
                break
            for u in result:
                uid = u.get("ID")
                if uid is None:
                    continue
                try:
                    uid_int = int(uid)
                except (TypeError, ValueError):
                    continue
                full = _user_record_to_name(u)
                if full:
                    _upsert_b24_user(conn, uid_int, full)
                    synced += 1
            if len(result) < page_size:
                break
            start += page_size
        except Exception as e:
            print(f"WARNING: sync_all_users_from_bitrix start={start}: {e}", file=sys.stderr, flush=True)
            break
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM b24_users")
        cached = cur.fetchone()[0] if cur.rowcount else 0
    return {"ok": True, "synced": synced, "total_in_cache": cached, "mode": "all"}


def sync_users_into_cache(conn, limit: int = 500, time_budget_sec: int = 120) -> Dict[str, Any]:
    """
    Заполнить b24_users из Bitrix user.get для тех user ID, которых ещё нет в кэше
    (только ID, встречающиеся в сделках/контактах/лидах).
    Для загрузки всех пользователей сразу используйте POST /sync/users?all=1
    """
    started = time.time()
    all_ids = _collect_user_ids_from_tables(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM b24_users")
        cached = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    missing = [uid for uid in all_ids if uid not in cached][:limit]
    if not missing:
        return {"ok": True, "synced": 0, "total_missing": 0, "cached": len(cached)}

    synced = 0
    batch_size = 50
    for i in range(0, len(missing), batch_size):
        if time.time() - started >= time_budget_sec:
            break
        batch = missing[i : i + batch_size]
        ids_str = ",".join(str(x) for x in batch)
        try:
            resp = b24.call("user.get", {"ID": ids_str})
            result = resp.get("result") if isinstance(resp, dict) else []
            if not isinstance(result, list):
                continue
            for u in result:
                uid = u.get("ID")
                if uid is None:
                    continue
                try:
                    uid_int = int(uid)
                except (TypeError, ValueError):
                    continue
                full = _user_record_to_name(u)
                if full:
                    _upsert_b24_user(conn, uid_int, full)
                    synced += 1
        except Exception as e:
            print(f"WARNING: sync_users_into_cache batch {ids_str}: {e}", file=sys.stderr, flush=True)
    return {"ok": True, "synced": synced, "total_missing": len(missing), "cached": len(cached)}


@app.post("/sync/users")
def sync_users_endpoint(
    all_users: bool = False,
    limit: int = 500,
    time_budget_sec: int = 120,
):
    """
    Заполнить b24_users (кэш имён пользователей) из Bitrix.

    - all_users=0 (по умолчанию): только те user ID, что есть в сделках/контактах/лидах и ещё не в кэше.
    - all_users=1: загрузить всех пользователей из Bitrix сразу (user.get с пагинацией).

    Пример: curl -X POST "http://127.0.0.1:7070/sync/users?all_users=1"
    """
    conn = pg_conn()
    try:
        if all_users:
            result = sync_all_users_from_bitrix(conn, time_budget_sec=min(time_budget_sec, 600))
        else:
            result = sync_users_into_cache(conn, limit=limit, time_budget_sec=time_budget_sec)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


