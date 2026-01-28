import os
import re
import sys
import traceback
import threading
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, time as dt_time
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from fastapi import FastAPI, HTTPException, Request

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

# If enabled, we do NOT poll Bitrix (no initial sync, no periodic sync).
# We only accept outbound Bitrix webhooks and fetch single entities by ID.
WEBHOOK_ONLY = os.getenv("WEBHOOK_ONLY", "0") == "1"

# Optional: verify webhook sender (Bitrix "application_token").
# If set, webhook requests with a different token are rejected.
B24_WEBHOOK_APPLICATION_TOKEN = os.getenv("B24_WEBHOOK_APPLICATION_TOKEN", "").strip()

# How many webhook queue items to process per tick
WEBHOOK_QUEUE_BATCH = int(os.getenv("WEBHOOK_QUEUE_BATCH", "30"))

# Seconds between queue processing ticks
WEBHOOK_QUEUE_POLL_SEC = float(os.getenv("WEBHOOK_QUEUE_POLL_SEC", "2"))

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


@app.post("/webhooks/b24/dynamic-item-update")
async def b24_dynamic_item_update(request: Request):
    """Bitrix outbound webhook receiver.

    Bitrix typically sends application/x-www-form-urlencoded, but we also accept JSON.
    We only enqueue the event here (fast response), processing happens in background.
    """
    payload: Dict[str, Any] = {}
    # 1) Try JSON
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            payload = {"_": payload}
    except Exception:
        payload = {}

    # 2) Fallback to form-urlencoded
    if not payload:
        try:
            body = (await request.body()).decode("utf-8", errors="ignore")
            # parse_qs returns dict[str, list[str]]
            from urllib.parse import parse_qs

            qs = parse_qs(body, keep_blank_values=True)
            payload = {k: (v[0] if len(v) == 1 else v) for k, v in qs.items()}
        except Exception:
            payload = {}

    # Optional security: validate Bitrix application_token if you set B24_WEBHOOK_TOKEN
    expected_token = os.getenv("B24_WEBHOOK_TOKEN", "").strip()
    if expected_token:
        auth = payload.get("auth") or {}
        token = None
        if isinstance(auth, dict):
            token = auth.get("application_token") or auth.get("APPLICATION_TOKEN")
        if not token:
            token = payload.get("auth[application_token]")
        if token != expected_token:
            return {"ok": False, "error": "bad_token"}

    entity_key, entity_id, is_delete = _parse_webhook_event(payload)
    if not entity_key or not entity_id:
        return {"ok": True, "skipped": True}

    try:
        with get_conn() as conn:
            ensure_meta_tables(conn)
            enqueue_webhook_event(conn, entity_key, entity_id, str(payload.get("event") or payload.get("EVENT") or ""))
    except Exception as e:
        # Still return 200 to Bitrix, but tell ourselves there was an issue
        return {"ok": False, "error": str(e)[:200]}

    return {"ok": True, "queued": True, "entity": entity_key, "id": entity_id, "delete": is_delete}

# Import data API router
from api_data import router as data_router
app.include_router(data_router)

# Import processes-deals API router
from processes_deals_api import router as processes_deals_router
app.include_router(processes_deals_router)

# Import entity-fields API router
from entity_fields_api import router as entity_fields_router
app.include_router(entity_fields_router)

# Import entity-data API router
from entity_data_api import router as entity_data_router
app.include_router(entity_data_router)

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

        # Queue for Bitrix outbound webhooks (so we can process in background
        # with rate limiting and retries instead of polling everything).
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_webhook_queue (
            id BIGSERIAL PRIMARY KEY,
            entity_key TEXT NOT NULL,
            entity_id BIGINT NOT NULL,
            event TEXT,
            received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            processed_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INT NOT NULL DEFAULT 0,
            last_error TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_b24_webhook_queue_pending ON b24_webhook_queue(status, received_at);")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_b24_webhook_queue_dedup ON b24_webhook_queue(entity_key, entity_id, status) WHERE status='pending';")
        
        # Классификатор источников (sursa) для сделок
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_classifier_sources (
            source_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
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
            if isinstance(v, str) and v.strip():
                return v.strip()
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

def upsert_rows(conn, table: str, col_order: List[str], rows: List[List[Any]]):
    if not rows:
        return

    cols_sql = ", ".join([f'"{c}"' for c in col_order])

    # обновляем все колонки кроме id (created_at не трогаем)
    set_cols = [c for c in col_order if c not in ("id", "created_at")]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in set_cols])

    # ВСЕГДА обновляем updated_at при апдейте
    if set_sql:
        set_sql = set_sql + ", updated_at = now()"
    else:
        set_sql = "updated_at = now()"

    sql = f"""
        INSERT INTO {table} ({cols_sql})
        VALUES %s
        ON CONFLICT (id) DO UPDATE
        SET {set_sql}
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
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
# Webhook ingestion (outbound Bitrix -> our API)
# -----------------------------

def _parse_webhook_event(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], bool]:
    """Return (entity_key, entity_id, is_delete). Supports deals/leads/contacts and dynamic items."""
    # Bitrix sometimes sends application/x-www-form-urlencoded with flat keys like
    # data[FIELDS][ID]. We support both nested JSON and flat dict payloads.
    event = str(payload.get("event") or payload.get("EVENT") or "").upper()

    data_obj = payload.get("data") or payload.get("DATA")
    fields = {}
    if isinstance(data_obj, dict):
        fields = (data_obj.get("FIELDS") or data_obj.get("fields") or {}) or {}
    elif isinstance(payload, dict):
        # flat form-like
        for k, v in payload.items():
            if not isinstance(k, str):
                continue
            if k.lower().endswith("[id]") and "fields" in k.lower():
                fields["ID"] = v
            if "entity_type_id" in k.lower() or k.lower().endswith("[entitytypeid]"):
                fields["ENTITY_TYPE_ID"] = v

    # ID can be str or int
    raw_id = fields.get("ID") or fields.get("id")
    try:
        entity_id = int(raw_id) if raw_id is not None else None
    except Exception:
        entity_id = None

    is_delete = event.endswith("DELETE")

    # Deal / Lead / Contact
    if "CRMDEAL" in event:
        return "deal", entity_id, is_delete
    if "CRMLEAD" in event:
        return "lead", entity_id, is_delete
    if "CRMCONTACT" in event:
        return "contact", entity_id, is_delete

    # Dynamic item (smart process)
    # Bitrix names: ONCRMDYNAMICITEMADD/UPDATE/DELETE
    if "CRMDYNAMICITEM" in event or "DYNAMICITEM" in event:
        et = (
            fields.get("ENTITY_TYPE_ID")
            or fields.get("ENTITYTYPEID")
            or fields.get("ENTITY_TYPE")
            or payload.get("entityTypeId")
            or payload.get("ENTITY_TYPE_ID")
        )
        try:
            entity_type_id = int(et) if et is not None else None
        except Exception:
            entity_type_id = None
        if entity_type_id is None:
            return None, entity_id, is_delete
        return f"sp:{entity_type_id}", entity_id, is_delete

    return None, entity_id, is_delete


def enqueue_webhook_event(conn, entity_key: str, entity_id: int, event: str, payload: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO b24_webhook_queue(entity_key, entity_id, event_name, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (entity_key, entity_id, event, json.dumps(payload, ensure_ascii=False))
        )
    conn.commit()


def _fetch_webhook_batch(conn, limit: int = 50) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, entity_key, entity_id, event_name
            FROM b24_webhook_queue
            WHERE processed_at IS NULL
            ORDER BY id ASC
            LIMIT %s
            """,
            (limit,)
        )
        rows = cur.fetchall() or []
    return rows


def _mark_webhook_processed(conn, queue_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE b24_webhook_queue SET processed_at = now(), status = 'ok' WHERE id = %s",
            (queue_id,)
        )
    conn.commit()


def _mark_webhook_failed(conn, queue_id: int, err: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE b24_webhook_queue
            SET last_error = %s,
                attempts = COALESCE(attempts,0)+1,
                status = 'error'
            WHERE id = %s
            """,
            (err[:500], queue_id)
        )
    conn.commit()


def _delete_entity_row(conn, entity_key: str, entity_id: int) -> None:
    table = table_name_for_entity(entity_key)
    if not table:
        return
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE id = %s", (entity_id,))
    conn.commit()


def _upsert_single_item(conn, entity_key: str, item: Dict[str, Any]) -> None:
    table = table_name_for_entity(entity_key)
    if not table:
        return

    colmap = load_colmap(conn, entity_key)
    if not colmap:
        # If we don't have schema mapping yet, skip to avoid breaking.
        return

    # Base columns
    row: Dict[str, Any] = {
        "id": int(item.get("ID") or item.get("id")),
        "raw": Json(item),
    }

    # Map fields -> DB columns
    for src_key, col_name in colmap.items():
        if src_key == "ID":
            continue
        if src_key in item:
            row[col_name] = item.get(src_key)

    # Optional convenience field
    if entity_key == "deal":
        assigned_id = item.get("ASSIGNED_BY_ID")
        if assigned_id is not None and "assigned_by_id" in [*row.keys(), *colmap.values()]:
            # If assigned_by_id exists as mapped column, it will already be set.
            pass

    cols = ["id", "raw"] + [c for c in row.keys() if c not in ("id", "raw")]
    vals = [row.get(c) for c in cols]

    placeholders = ",".join(["%s"] * len(cols))
    set_clause = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "id"])

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT (id) DO UPDATE SET {set_clause}",
            vals,
        )
    conn.commit()


def _b24_get_entity_by_id(entity_key: str, entity_id: int) -> Optional[Dict[str, Any]]:
    if entity_key == "deal":
        r = b24_call("crm.deal.get", {"id": entity_id})
        if r and r.get("error") == "OVERLOAD_LIMIT":
            return {"__overload": True}
        return (r or {}).get("result")
    if entity_key == "lead":
        r = b24_call("crm.lead.get", {"id": entity_id})
        if r and r.get("error") == "OVERLOAD_LIMIT":
            return {"__overload": True}
        return (r or {}).get("result")
    if entity_key == "contact":
        r = b24_call("crm.contact.get", {"id": entity_id})
        if r and r.get("error") == "OVERLOAD_LIMIT":
            return {"__overload": True}
        return (r or {}).get("result")

    # smart process
    if entity_key.startswith("sp:"):
        try:
            entity_type_id = int(entity_key.split(":", 1)[1])
        except Exception:
            return None
        r = b24_call("crm.item.get", {"entityTypeId": entity_type_id, "id": entity_id})
        if r and r.get("error") == "OVERLOAD_LIMIT":
            return {"__overload": True}
        return (r or {}).get("result", {}).get("item")
    return None


def webhook_queue_worker():
    """Background worker: processes queued webhook events with rate limiting."""
    while True:
        try:
            conn = pg_conn()
            ensure_meta_tables(conn)
            batch = _fetch_webhook_batch(conn, limit=30)
            if not batch:
                conn.close()
                time.sleep(1.0)
                continue

            for row in batch:
                qid = int(row["id"])
                entity_key = row["entity_key"]
                entity_id = int(row["entity_id"])
                event_name = str(row["event_name"] or "")

                try:
                    if event_name.upper().endswith("DELETE"):
                        _delete_entity_row(conn, entity_key, entity_id)
                        _mark_webhook_processed(conn, qid)
                        continue

                    item = _b24_get_entity_by_id(entity_key, entity_id)
                    if isinstance(item, dict) and item.get("__overload"):
                        # Bitrix API temporarily blocked (OVERLOAD_LIMIT). Leave it queued.
                        time.sleep(5.0)
                        break
                    if item is None:
                        _mark_webhook_failed(conn, qid, "empty result")
                        continue
                    _upsert_single_item(conn, entity_key, item)
                    _mark_webhook_processed(conn, qid)
                except Exception as e:
                    _mark_webhook_failed(conn, qid, str(e))

            conn.close()
        except Exception as e:
            print(f"ERROR: webhook_queue_worker: {e}", file=sys.stderr, flush=True)
            time.sleep(2.0)

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
        
        # Синхронизируем классификатор источников из enum значений поля источника сделок
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
                    
                    # Периодически (раз в час) обновляем assigned_by_name для всех сделок
                    current_time = time.time()
                    if current_time - _last_full_update_time >= FULL_UPDATE_INTERVAL_SEC:
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

@app.on_event("startup")
def on_startup():
    # Always start webhook queue worker (it is idle if nothing is queued).
    wt = threading.Thread(target=webhook_queue_worker, daemon=True)
    wt.start()

    if WEBHOOK_ONLY:
        # Webhook-only mode: do not poll Bitrix at all (prevents OVERLOAD_LIMIT bans).
        print(
            "WEBHOOK ONLY MODE: polling is disabled; waiting for outbound Bitrix events...",
            flush=True
        )
        return

    # Default behavior: do initial sync and periodic polling.
    # Это обеспечивает: БИТРИКС -> БАЗА -> PDF
    initial_sync_thread = threading.Thread(target=_initial_sync_thread, daemon=True)
    initial_sync_thread.start()

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
        
        return {"ok": True, "updated": updated, "total": len(deals_to_update)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


