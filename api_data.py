import base64
import errno
import os
import re
import sys
import json
from io import BytesIO
from contextvars import ContextVar
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException, Query
from reportlab.lib.pagesizes import A4, A3, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Frame, PageTemplate, KeepTogether
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

import requests  # Telegram + Bitrix user.get

# WeasyPrint для генерации PDF из HTML/CSS (поддержка CSS Grid)
try:
    from weasyprint import HTML, CSS
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False
    print("WARNING: weasyprint not installed. Install with: pip install weasyprint", file=sys.stderr, flush=True)

# ---- timezone helper (Moldova default) ----
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


router = APIRouter(prefix="/api/data", tags=["data"])


# ---------------- User name cache (для Responsabil) ----------------
_user_name_cache: Dict[str, str] = {}


def _get_user_name(user_id: Optional[str], bitrix_webhook: Optional[str] = None) -> str:
    """
    Получает имя пользователя по ID через Bitrix API.
    Использует кэш, чтобы не делать повторные запросы.
    """
    if not user_id:
        return ""

    user_id_str = str(user_id).strip()
    if not user_id_str:
        return ""

    # Проверяем кэш
    if user_id_str in _user_name_cache:
        return _user_name_cache[user_id_str]

    # Если нет webhook, возвращаем ID
    if not bitrix_webhook:
        return user_id_str

    try:
        url = f"{bitrix_webhook.rstrip('/')}/user.get.json"
        params = {"ID": user_id_str}

        response = requests.post(url, json=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if "result" in data and len(data["result"]) > 0:
                user = data["result"][0]
                name = (
                    user.get("NAME")
                    or user.get("LAST_NAME")
                    or user.get("FULL_NAME")
                    or user.get("LOGIN")
                    or ""
                )
                if name:
                    if user.get("LAST_NAME") and user.get("NAME"):
                        name = f"{user.get('NAME')} {user.get('LAST_NAME')}"
                    _user_name_cache[user_id_str] = name
                    return name
    except Exception:
        pass

    _user_name_cache[user_id_str] = user_id_str
    return user_id_str


# ---------------- CONFIG ----------------
TG_TOKEN = os.getenv("TG_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
BITRIX_WEBHOOK = os.getenv(
    "BITRIX_WEBHOOK",
    "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote",
).strip()
# Отдельный webhook и чат для отправки PDF в Bitrix от system.notifications@nobilauto.md
BITRIX_WEBHOOK_REPORTS = os.getenv(
    "BITRIX_WEBHOOK_REPORTS",
    "https://nobilauto.bitrix24.ru/rest/20532/grmoroz08bush0kp",
).strip()
BITRIX_REPORT_CHAT_ID = os.getenv("BITRIX_REPORT_CHAT_ID", "136188").strip()

# отчетный timezone (чтобы "сегодня" было по Молдове, а не UTC)
REPORT_TZ = os.getenv("REPORT_TZ", "Europe/Chisinau").strip() or "Europe/Chisinau"
DEALS_ONLY_TODAY = os.getenv("DEALS_ONLY_TODAY", "1").strip() in ("1", "true", "yes", "y", "on", "да")


def _get_report_tzinfo():
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(REPORT_TZ)
    except Exception:
        return timezone.utc


REPORT_TZINFO = _get_report_tzinfo()

# Если задан при вызове send (report_date=YYYY-MM-DD), отчёт строится за эту дату, а не за "сейчас"
_report_date_override: ContextVar[Optional[date]] = ContextVar("report_date_override", default=None)


def _today_in_report_tz(now_utc: Optional[datetime] = None) -> date:
    override = _report_date_override.get()
    if override is not None:
        return override
    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        return now_utc.astimezone(REPORT_TZINFO).date()
    except Exception:
        return now_utc.date()


# Smart process STOCK AUTO
STOCK_ENTITY_TYPE_ID = int(os.getenv("STOCK_ENTITY_TYPE_ID", "1114"))

# Branches format: "123:Centru,124:Botanica,..."
BRANCHES = os.getenv("BRANCHES", "").strip()

# ---------------- PDF Filters by Branch (assigned_by_id) ----------------
PDF_FILTERS_ASSIGNED_BY_IDS = {
    "Centru": [3238, 8136, 8138, 23796],
    "Buiucani": [8134, 1624],  # Ilie Gaina, Dan Soltan
    "Comrat": [1620],  # Igor Dudoglo
    "Cahul": [8142],  # Petru Dobrovolschi
    "Mezon": [19668],  # Denis Abramciuc
    "Balti": [20566],  # Veaceslav Ungureanu
    "Ungheni": [8144],  # Ivan Polschii
}

# Отображение assigned_by_id -> имя (для заполнения, если пусто в БД)
PDF_RESPONSIBLE_NAMES = {
    8134: "Ilie Gaina",
    1624: "Dan Soltan",
    20566: "Veaceslav Ungureanu",
    8142: "Petru Dobrovolschi",
    1620: "Igor Dudoglo",
    8144: "Ivan Polschii",
    19668: "Denis Abramciuc",
    8136: "Stefan Cerchez",
    8138: "Cristian Vacari",
    23796: "Dumitru Cosciug",
}

# Отображение assigned_by_id -> имя (для вывода в PDF, если в БД пусто)
PDF_RESPONSIBLE_NAMES = {
    8134: "Ilie Gaina",
    1624: "Dan Soltan",
    20566: "Veaceslav Ungureanu",
    8142: "Petru Dobrovolschi",
    1620: "Igor Dudoglo",
    8144: "Ivan Polschii",
    19668: "Denis Abramciuc",
    8136: "Stefan Cerchez",
    8138: "Cristian Vacari",
    23796: "Dumitru Cosciug",
}

# Филиалы, для которых показываются таблицы "Auto Date" и "Auto Primite"
# Если пусто - для всех филиалов. Если указаны - только для них.
DEALS_TABLES_BRANCHES = []  # Пусто = для всех филиалов. Или: ["Centru", "Buiucani", "Comrat"]

# Филиалы, для которых показывается таблица "Prelungire"
# Если пусто - для всех филиалов. Если указаны - только для них.
DEALS_THIRD_TABLE_BRANCHES = []  # Пусто = для всех филиалов. Или: ["Centru", "Buiucani"]

# PDF font override (rarely needed)
PDF_FONT_PATH = os.getenv("PDF_FONT_PATH", "").strip()

# ---------------- Postgres ----------------
PG_HOST = os.getenv("PG_HOST", "194.33.40.197")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crm")
PG_USER = os.getenv("PG_USER", "crm")
PG_PASS = os.getenv("PG_PASS", "crm")

# Optional category filter (categoryId) for STOCK AUTO
STOCK_CATEGORY_ID = os.getenv("STOCK_CATEGORY_ID", "").strip()

# ---------------- Deals (Auto Date) ----------------
DEALS_TABLE = os.getenv("DEALS_TABLE", "b24_crm_deal").strip()
DEALS_CATEGORY_ID = int(os.getenv("DEALS_CATEGORY_ID", "20"))

# !!! В ЭТОЙ ВЕРСИИ МЫ НЕ ФИЛЬТРУЕМ СДЕЛКИ ПО ФИЛИАЛУ
DEALS_F_BRANCH = os.getenv("DEALS_F_BRANCH", "uf_crm_1749158152036").strip()

DEALS_F_SURSA = os.getenv("DEALS_F_SURSA", "uf_crm_1749211409067").strip()
DEALS_F_CARNO = os.getenv("DEALS_F_CARNO", "uf_crm_1749550611").strip()
DEALS_F_BRAND = os.getenv("DEALS_F_BRAND", "uf_crm_1749556374").strip()
DEALS_F_MODEL = os.getenv("DEALS_F_MODEL", "uf_crm_1749550573").strip()
DEALS_F_FROMDT = os.getenv("DEALS_F_FROMDT", "uf_crm_1749728734").strip()
DEALS_F_TODT = os.getenv("DEALS_F_TODT", "uf_crm_1749728773").strip()

# Новые поля для второй таблицы
DEALS_F_GPS = os.getenv("DEALS_F_GPS", "uf_crm_1754124947425").strip()
DEALS_F_AMENDA = os.getenv("DEALS_F_AMENDA", "uf_crm_1749189180").strip()
DEALS_F_COM_AMENDA = os.getenv("DEALS_F_COM_AMENDA", "uf_crm_1750430038").strip()
DEALS_F_SUMA_RAMBURSARE = os.getenv("DEALS_F_SUMA_RAMBURSARE", "uf_crm_1750709202").strip()
DEALS_F_COM_REFUZ = os.getenv("DEALS_F_COM_REFUZ", "uf_crm_1750709546").strip()

# Поля для третьей таблицы (Prelungire)
DEALS_F_RETURN_DT = os.getenv("DEALS_F_RETURN_DT", "UF_CRM_1749189804").strip()  # Data - return din chirie (si cu prelungire)

# Поля для продления аренды (5 продлений с датами и ценами)
DEALS_F_PRELUNGIRE_1_DT = os.getenv("DEALS_F_PRELUNGIRE_1_DT", "UF_CRM_1751889187").strip()  # 1. Prima data chiria se prelungeste pana pe
DEALS_F_PRELUNGIRE_2_DT = os.getenv("DEALS_F_PRELUNGIRE_2_DT", "UF_CRM_1751894356").strip()  # 2. A doua oara chiria se prelungeste pana pe
DEALS_F_PRELUNGIRE_3_DT = os.getenv("DEALS_F_PRELUNGIRE_3_DT", "UF_CRM_1751894409").strip()  # 3. A treia oara chiria se prelungeste pana pe
DEALS_F_PRELUNGIRE_4_DT = os.getenv("DEALS_F_PRELUNGIRE_4_DT", "UF_CRM_1751894425").strip()  # 4. A patra oara chiria se prelungeste pana pe
DEALS_F_PRELUNGIRE_5_DT = os.getenv("DEALS_F_PRELUNGIRE_5_DT", "UF_CRM_1751894535").strip()  # 5. A cincea oara chiria se prelungeste pana pe

DEALS_F_PRELUNGIRE_1_PRET = os.getenv("DEALS_F_PRELUNGIRE_1_PRET", "UF_CRM_1751886604").strip()  # 1. Prețul primei prelungire chiriei
DEALS_F_PRELUNGIRE_2_PRET = os.getenv("DEALS_F_PRELUNGIRE_2_PRET", "UF_CRM_1751886635").strip()  # 2. Prețul a doua prelungire chiriei
DEALS_F_PRELUNGIRE_3_PRET = os.getenv("DEALS_F_PRELUNGIRE_3_PRET", "UF_CRM_1751888121").strip()  # 3. Prețul a treia prelungire chiriei
DEALS_F_PRELUNGIRE_4_PRET = os.getenv("DEALS_F_PRELUNGIRE_4_PRET", "UF_CRM_1751888928").strip()  # 4. Prețul a patra prelungire chiriei
DEALS_F_PRELUNGIRE_5_PRET = os.getenv("DEALS_F_PRELUNGIRE_5_PRET", "UF_CRM_1751889092").strip()  # 5. Prețul a cincea prelungire chiriei

# Фильтры для второй таблицы
DEALS_FILTER_STATUS_VALUES = ["Contract închis", "Сделка провалена"]
DEALS_FILTER_RESPONSABIL_NAMES = ["Stefan Cerchez", "Cristian Vacari", "Rafaell Vintu"]

# Фильтр стадии для таблицы "Auto Date"
DEALS_FILTER_STAGE_IN_CHIRIE = os.getenv("DEALS_FILTER_STAGE_IN_CHIRIE", "în chirie").strip()  # Стадия "în chirie"

# Фильтр стадии для таблицы "Auto Date"
DEALS_FILTER_STAGE_IN_CHIRIE = os.getenv("DEALS_FILTER_STAGE_IN_CHIRIE", "în chirie").strip()  # Стадия "în chirie"


def pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )
    try:
        conn.set_client_encoding("UTF8")
    except Exception as e:
        try:
            with conn.cursor() as cur:
                cur.execute("SET client_encoding TO 'UTF8'")
            conn.commit()
        except Exception:
            print(f"WARNING: pg_conn: could not set UTF8 encoding: {e}", file=sys.stderr, flush=True)
    return conn


def stock_table_name(entity_type_id: int) -> str:
    return f"b24_sp_f_{int(entity_type_id)}"


def meta_entity_key(entity_type_id: int) -> str:
    return f"sp:{int(entity_type_id)}"


# ---------------- ВАЖНО: ПОЛЯ (STOCK AUTO) ----------------
STOCK_F_BRANCH = os.getenv("STOCK_F_BRANCH", "ufCrm34_1749209523").strip()
STOCK_F_LOC = os.getenv("STOCK_F_LOC", "ufCrm34_1751116796").strip()
STOCK_F_WAIT_SVC = os.getenv("STOCK_F_WAIT_SVC", "ufCrm34_1760623438126").strip()

STOCK_F_FROMDT = os.getenv("STOCK_F_FROMDT", "ufCrm34_1748962248").strip()
STOCK_F_TODT = os.getenv("STOCK_F_TODT", "ufCrm34_1748962285").strip()

STOCK_F_CARNO = os.getenv("STOCK_F_CARNO", "ufCrm34_1748431574").strip()
STOCK_F_BRAND = os.getenv("STOCK_F_BRAND", "ufCrm34_1748347910").strip()
STOCK_F_MODEL = os.getenv("STOCK_F_MODEL", "ufCrm34_1748431620").strip()

DEFAULT_SERVICE_LOCS = {
    "Testare dupa service",
    "Vulcanizare Studentilor",
    "Spalatoria",
}
DEFAULT_SALE_LOC = "Parcarea de Vânzare"


# ---------------- Branches parsing ----------------
def parse_branches(raw: str) -> List[Tuple[str, str]]:
    if not raw:
        raise HTTPException(
            status_code=400,
            detail='BRANCHES is empty. Set env BRANCHES="123:Centru,124:Botanica,..."',
        )
    out: List[Tuple[str, str]] = []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for p in parts:
        if ":" not in p:
            print(f"WARNING: parse_branches: Skipping invalid part (no ':'): '{p}'", file=sys.stderr, flush=True)
            continue
        a, b = [x.strip() for x in p.split(":", 1)]
        if a.isdigit() and not b.isdigit():
            # Формат: "1670:Ungheni" -> (name="Ungheni", id="1670")
            out.append((b, a))
            print(f"DEBUG: parse_branches: Parsed '{p}' -> name='{b}', id='{a}'", file=sys.stderr, flush=True)
        else:
            # Формат: "Ungheni:1670" -> (name="Ungheni", id="1670")
            out.append((a, b))
            print(f"DEBUG: parse_branches: Parsed '{p}' -> name='{a}', id='{b}'", file=sys.stderr, flush=True)
    if not out:
        raise HTTPException(status_code=400, detail="BRANCHES parsed empty. Format is id:name,id:name,...")
    print(f"DEBUG: parse_branches: Total parsed branches: {len(out)}, branches: {out}", file=sys.stderr, flush=True)
    return out


def branches_id_to_name(branches: List[Tuple[str, str]]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for display, fid in branches:
        m[str(fid)] = str(display)
    return m


# ---------------- Fonts (Cyrillic) ----------------
def register_cyrillic_font() -> Tuple[str, str]:
    candidates: List[str] = []
    if PDF_FONT_PATH:
        candidates.append(PDF_FONT_PATH)
    candidates += [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]
    bold_candidates: List[str] = []
    if PDF_FONT_PATH:
        # Пытаемся найти жирную версию рядом с обычной
        base_path = os.path.dirname(PDF_FONT_PATH)
        bold_candidates.append(os.path.join(base_path, "DejaVuSans-Bold.ttf"))
    bold_candidates += [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    ]
    
    font_path = None
    for p in candidates:
        if p and os.path.isfile(p):
            pdfmetrics.registerFont(TTFont("DejaVuSans", p))
            font_path = p
            break
    
    if not font_path:
        raise RuntimeError(
            "Cyrillic font not found. Install: apt-get update && apt-get install -y fonts-dejavu-core "
            "or set PDF_FONT_PATH=/path/to/DejaVuSans.ttf"
        )
    
    # Регистрируем жирную версию шрифта, если найдена
    for p in bold_candidates:
        if p and os.path.isfile(p):
            pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", p))
            break
    
    return "DejaVuSans", font_path


# ---------------- JSON helpers from raw ----------------
def _extract_fields_from_raw(raw_obj: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(raw_obj.get("fields"), dict):
        return raw_obj["fields"]
    return raw_obj


# ---------------- Dates ----------------
def _to_dt(v: Any) -> Optional[datetime]:
    """
    FIX:
    - Если дата/время без timezone (naive) -> считаем REPORT_TZINFO (Europe/Chisinau),
      а не UTC. Это убирает “позавчера” из-за сдвига часов.
    """
    if not v:
        return None

    if isinstance(v, datetime):
        if v.tzinfo is None:
            try:
                return v.replace(tzinfo=REPORT_TZINFO)
            except Exception:
                return v.replace(tzinfo=timezone.utc)
        return v

    if isinstance(v, str):
        try:
            s = v.strip().replace("Z", "+00:00")
            d = datetime.fromisoformat(s)

            if d.tzinfo is None:
                try:
                    return d.replace(tzinfo=REPORT_TZINFO)
                except Exception:
                    return d.replace(tzinfo=timezone.utc)

            return d
        except Exception:
            return None

    return None


def _get_moved_time(raw_obj: Dict[str, Any], fields: Dict[str, Any]) -> Optional[datetime]:
    for k in ("movedTime", "MOVED_TIME", "moved_time"):
        if k in raw_obj:
            dt = _to_dt(raw_obj.get(k))
            if dt:
                return dt
        if k in fields:
            dt = _to_dt(fields.get(k))
            if dt:
                return dt
    return None


def _fmt_ddmmyyyy(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    try:
        return dt.astimezone(REPORT_TZINFO).strftime("%d/%m/%Y")
    except Exception:
        return dt.astimezone(timezone.utc).strftime("%d/%m/%Y")


def _fmt_ddmmyyyy_hhmm(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    try:
        return dt.astimezone(REPORT_TZINFO).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return dt.astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M")


def _days_since(dt: Optional[datetime], now_dt: datetime) -> str:
    if not dt:
        return ""
    try:
        return str((now_dt.date() - dt.date()).days)
    except Exception:
        return ""


def _is_dt_today_in_report_tz(dt: Optional[datetime], now_utc: Optional[datetime] = None) -> bool:
    """
    TODAY: True, если дата dt == СЕГОДНЯ в REPORT_TZ (Europe/Chisinau).
    """
    if not dt:
        return False

    now_utc = now_utc or datetime.now(timezone.utc)

    try:
        today_local = now_utc.astimezone(REPORT_TZINFO).date()
        return dt.astimezone(REPORT_TZINFO).date() == today_local
    except Exception:
        today_utc = now_utc.astimezone(timezone.utc).date()
        return dt.astimezone(timezone.utc).date() == today_utc


# ---------------- КЛАССИФИКАЦИЯ ДЛЯ STOCK AUTO ----------------
def stock_classify_default(fields: Dict[str, Any], now: datetime) -> Tuple[str, Optional[str]]:
    """
    Классифицирует элементы смарт-процесса по stage_name из словаря стадий.
    Приоритет: stage_name > старая логика (даты/локации)
    """
    # Приоритет: используем stage_name из словаря стадий
    stage_name = fields.get("_stage_name") or fields.get("stage_name") or fields.get("STAGE_NAME") or ""
    
    if stage_name:
        stage_name_upper = str(stage_name).strip().upper()
        
        # Маппинг stage_name → bucket согласно Excel
        if "SERVICE" in stage_name_upper or stage_name_upper == "IN SERVICE":
            return ("SERVICE", stage_name)
        elif "CHIRIE" in stage_name_upper or stage_name_upper == "IN CHIRIE":
            return ("CHIRIE", stage_name)
        elif "DISPONIBILE" in stage_name_upper:
            return ("PARCARE", stage_name)  # PARCARE используется для Disponibile
        elif "ALTELE" in stage_name_upper or "ALTE" in stage_name_upper:
            return ("ALTE", stage_name)
    
    # Fallback: старая логика (даты/локации) если stage_name не найден
    dt_from = _to_dt(fields.get(STOCK_F_FROMDT))
    dt_to = _to_dt(fields.get(STOCK_F_TODT))

    loc = fields.get(STOCK_F_LOC)
    loc_s = str(loc).strip() if loc is not None else ""

    wait_s = fields.get(STOCK_F_WAIT_SVC)
    wait_s_bool = str(wait_s).lower() in ("1", "true", "y", "yes", "да", "on")

    if dt_from and dt_to:
        try:
            if dt_to >= now:
                return ("CHIRIE", None)
        except Exception:
            return ("CHIRIE", None)

    if wait_s_bool or (loc_s and loc_s in DEFAULT_SERVICE_LOCS):
        return ("SERVICE", None)

    if loc_s and loc_s == DEFAULT_SALE_LOC:
        return ("PARCARE", None)

    if loc_s:
        return ("ALTE", loc_s)

    return ("FARA_STATUS", None)


# ---------------- Enum mapping from PG meta ----------------
def _extract_enum_map_from_settings(settings: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not settings:
        return out
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except Exception:
            return out
    if not isinstance(settings, dict):
        return out

    candidates: List[Any] = []
    for k in ("items", "values", "enum", "list", "options"):
        v = settings.get(k)
        if isinstance(v, list):
            candidates.append(v)

    for lst in candidates:
        for it in lst:
            if not isinstance(it, dict):
                continue
            _id = it.get("ID") or it.get("id") or it.get("VALUE_ID") or it.get("value_id")
            _val = (
                it.get("VALUE")
                or it.get("value")
                or it.get("NAME")
                or it.get("name")
                or it.get("TITLE")
                or it.get("title")
            )
            if _id is None or _val is None:
                continue
            out[str(_id)] = str(_val)
    return out


def pg_load_enum_map(conn, entity_key: str, b24_field: str) -> Dict[str, str]:
    if not b24_field:
        return {}

    if entity_key == "deal" and (
        b24_field == "SourceId"
        or b24_field == "source_id"
        or b24_field == "SOURCE_ID"
        or b24_field == DEALS_F_SURSA
    ):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT source_id, source_name
                FROM b24_classifier_sources
                ORDER BY source_id
                """
            )
            rows = cur.fetchall()
            enum_map = {}
            for row in rows:
                enum_map[str(row["source_id"])] = str(row["source_name"])
            return enum_map

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT settings
            FROM b24_meta_fields
            WHERE entity_key=%s AND b24_field=%s
            """,
            (entity_key, b24_field),
        )
        row = cur.fetchone()
        if not row:
            return {}
        return _extract_enum_map_from_settings(row.get("settings"))


def _enum_to_text(val: Any, enum_map: Dict[str, str]) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""

    if not enum_map:
        return s

    if s in enum_map:
        return enum_map[s]

    if "|" in s:
        first_part = s.split("|")[0].strip()
        if first_part and first_part in enum_map:
            return enum_map[first_part]
        return s

    return s


# ---------------- Filiala normalize (Stock auto) ----------------
def _normalize_branch_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float, str)):
        return str(v).strip()
    if isinstance(v, dict):
        return str(
            v.get("id")
            or v.get("ID")
            or v.get("value")
            or v.get("VALUE")
            or v.get("name")
            or v.get("NAME")
            or ""
        ).strip()
    if isinstance(v, list) and v:
        for x in v:
            s = _normalize_branch_value(x)
            if s:
                return s
        return ""
    return str(v).strip()


# ---------------- SAFE identifier ----------------
_ident_re = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_ident(name: str, what: str = "identifier") -> str:
    name = (name or "").strip()
    if not _ident_re.match(name):
        raise HTTPException(status_code=400, detail=f"Invalid {what}: {name!r}")
    return name


# ---------------- Assigned_by helpers (ID -> NAME mapping + filtering) ----------------
_ws_re = re.compile(r"\s+", re.UNICODE)


def _normalize_person_name(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    s = _ws_re.sub(" ", s)
    return s.lower()


def _build_allowed_assigned_names(assigned_ids_list: List[int]) -> List[str]:
    out: List[str] = []
    for uid in assigned_ids_list:
        try:
            name = _get_user_name(str(uid), BITRIX_WEBHOOK)
            n = _normalize_person_name(name)
            if n:
                out.append(n)
                print(f"DEBUG: _build_allowed_assigned_names: Successfully got name for ID {uid}: '{name}' -> normalized: '{n}'", file=sys.stderr, flush=True)
            else:
                print(f"WARNING: _build_allowed_assigned_names: Got empty name for ID {uid}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: _build_allowed_assigned_names: Failed to get name for ID {uid}: {e}", file=sys.stderr, flush=True)
            continue

    seen = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    print(f"DEBUG: _build_allowed_assigned_names: Final normalized names for IDs {assigned_ids_list}: {uniq}", file=sys.stderr, flush=True)
    return uniq


def _ensure_assigned_name(d: Dict[str, Any]):
    """
    Если assigned_by_name пусто, а assigned_by_id есть, подставляем из PDF_RESPONSIBLE_NAMES.
    """
    if not isinstance(d, dict):
        return
    if d.get("assigned_by_name"):
        return
    aid = d.get("assigned_by_id")
    if aid is None:
        return
    try:
        aid_int = int(aid)
        name = PDF_RESPONSIBLE_NAMES.get(aid_int)
        if name:
            d["assigned_by_name"] = name
    except Exception:
        pass


def _raw_get(raw_obj: Any, key: str) -> Any:
    if not raw_obj or not isinstance(raw_obj, dict):
        return None
    
    value = raw_obj.get(key)
    if value is not None:
        return value

    value = raw_obj.get(key.upper())
    if value is not None:
        return value

    value = raw_obj.get(key.lower())
    if value is not None:
        return value

    fields = raw_obj.get("fields")
    if isinstance(fields, dict):
        value = fields.get(key)
        if value is not None:
            return value
        value = fields.get(key.upper())
        if value is not None:
            return value
        value = fields.get(key.lower())
        if value is not None:
            return value

        return None
    

def _deal_assigned_name_from_row(d: Dict[str, Any]) -> str:
    raw = d.get("raw") if isinstance(d.get("raw"), dict) else None
    name = d.get("assigned_by_name") or ""
    if not name:
        name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
    return str(name or "").strip()


def _deal_matches_assigned_filter(
    d: Dict[str, Any],
    allowed_ids: List[int],
    allowed_names_norm: List[str],
) -> bool:
    raw = d.get("raw") if isinstance(d.get("raw"), dict) else None

    # 1) Проверяем assigned_by_id из строки или из raw
    aid = d.get("assigned_by_id")
    if aid is None and raw:
        aid = _raw_get(raw, "ASSIGNED_BY_ID") or _raw_get(raw, "assigned_by_id")
    if aid is not None and allowed_ids:
        try:
            if int(aid) in allowed_ids:
                return True
        except Exception:
            pass

    if allowed_names_norm:
        nm = _deal_assigned_name_from_row(d)
        nm_norm = _normalize_person_name(nm)
        if nm_norm and nm_norm in set(allowed_names_norm):
            return True

    return False


def _row_get_any(row: Dict[str, Any], raw: Optional[Dict[str, Any]], field: str) -> Any:
    if not field:
        return None

    if field in row and row[field] is not None:
        return row[field]
    if field.upper() in row and row[field.upper()] is not None:
        return row[field.upper()]
    if field.lower() in row and row[field.lower()] is not None:
        return row[field.lower()]

    if raw:
        v = _raw_get(raw, field)
        if v is not None:
            return v

        return None
    

def _deal_dt_from_any(d: Dict[str, Any]) -> Optional[datetime]:
    raw = d.get("raw") if isinstance(d.get("raw"), dict) else None
    return _to_dt(d.get("fromdt_val") or _row_get_any(d, raw, DEALS_F_FROMDT))


# ---------------- PG list STOCK AUTO items (raw) ----------------
def pg_load_stage_dict_from_table(conn, table: str) -> Dict[str, str]:
    """
    Загружает словарь стадий из таблицы b24_sp_f_1114.
    Возвращает словарь: stageid -> stage_name
    """
    stage_dict = {}
    try:
        table_safe = _safe_ident(table, "table")
        with conn.cursor() as cur:
            # Получаем уникальные stageid из таблицы
            cur.execute(f"""
                SELECT DISTINCT btrim(stageid) AS stageid
                FROM {table_safe}
                WHERE stageid IS NOT NULL AND btrim(stageid) != ''
                LIMIT 1000
            """)
            rows = cur.fetchall()
            
            # Создаем словарь с маппингом stageid -> stage_name
            for (stageid,) in rows:
                if not stageid:
                    continue
                stageid_clean = str(stageid).strip()
                
                # Маппинг согласно Excel:
                # DT1114_70:UC_8XCJ8D -> In service
                # DT1114_70:UC_J41FJW -> Altele
                # DT1114_70:PREPARATION -> In chirie
                # DT1114_70:NEW -> Disponibile
                if stageid_clean.endswith(':UC_8XCJ8D'):
                    stage_dict[stageid_clean] = 'In service'
                elif stageid_clean.endswith(':UC_J41FJW'):
                    stage_dict[stageid_clean] = 'Altele'
                elif stageid_clean.endswith(':PREPARATION'):
                    stage_dict[stageid_clean] = 'In chirie'
                elif stageid_clean.endswith(':NEW'):
                    stage_dict[stageid_clean] = 'Disponibile'
                else:
                    # Для неизвестных стадий оставляем пустым
                    stage_dict[stageid_clean] = ''
            
            print(f"DEBUG: pg_load_stage_dict_from_table: Loaded {len(stage_dict)} stage mappings from {table}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"WARNING: pg_load_stage_dict_from_table: Failed to load stage dict: {e}", file=sys.stderr, flush=True)
    
    return stage_dict


def pg_list_stock_raw(
    conn,
    table: str,
    branch_field: str,
    branch_value: Any,
    limit: int,
    category_id: Optional[int] = None,
    stage_dict: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    if not branch_field:
        raise HTTPException(status_code=400, detail="STOCK_F_BRANCH is empty")

    branch_value_str = str(branch_value).strip()
    
    # Детальное логирование для Centru с самого начала
    is_centru_input = branch_value_str == "1668" or "centru" in str(branch_value).lower() or "centru" in branch_value_str.lower()
    if is_centru_input:
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU FUNCTION CALLED *** branch_value={branch_value} (type={type(branch_value)}), branch_value_str={branch_value_str}, branch_field={branch_field}, table={table}", file=sys.stderr, flush=True)
    
    table_safe = _safe_ident(table, "table")
    # Для JSONB ключей нужно использовать строку в кавычках, а не параметр
    # Экранируем одинарные кавычки для безопасности от SQL injection
    branch_field_escaped = branch_field.replace("'", "''")
    
    # Загружаем словарь стадий, если не передан
    if stage_dict is None:
        stage_dict = pg_load_stage_dict_from_table(conn, table)
    
    # Строим CTE для словаря стадий, если он есть
    stage_dict_cte = ""
    stage_dict_join = ""
    if stage_dict and len(stage_dict) > 0:
        try:
            max_values = 1000
            stage_dict_items = list(stage_dict.items())[:max_values]
            
            stage_dict_values = []
            for stageid, stage_name in stage_dict_items:
                if not stageid:
                    continue
                # Экранируем кавычки
                stageid_escaped = str(stageid).replace("'", "''").replace("\\", "\\\\")
                stage_name_escaped = (str(stage_name) if stage_name else "").replace("'", "''").replace("\\", "\\\\")
                stage_dict_values.append(f"('{stageid_escaped}', '{stage_name_escaped}')")
            
            if stage_dict_values:
                stage_dict_cte = f"""
        , stage_dict AS (
            SELECT stageid, stage_name
            FROM (VALUES {', '.join(stage_dict_values)}) AS v(stageid, stage_name)
        )
                """
                stage_dict_join = """
        LEFT JOIN stage_dict ON btrim(t.stage_id) = btrim(stage_dict.stageid)
                """
                print(f"DEBUG: pg_list_stock_raw: Created stage_dict CTE with {len(stage_dict_values)} entries", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: pg_list_stock_raw: Failed to create stage_dict CTE: {e}", file=sys.stderr, flush=True)
            stage_dict_cte = ""
            stage_dict_join = ""

    # СПЕЦИАЛЬНАЯ ОБРАБОТКА ТОЛЬКО ДЛЯ CENTRU: упрощенный SQL, который проверяет только raw->field напрямую
    if is_centru_input:
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU USING SIMPLIFIED SQL *** Only checking raw->'{branch_field_escaped}' directly", file=sys.stderr, flush=True)
        sql = f"""
            WITH t AS (
                SELECT 
                    id, 
                    raw, 
                    -- Извлекаем stageId из raw JSON
                    COALESCE(raw->>'stageId', raw->'fields'->>'stageId', raw->>'STAGE_ID', raw->'fields'->>'STAGE_ID') AS stage_id
                FROM {table_safe}
            ){stage_dict_cte}
            SELECT 
                t.raw,
                t.stage_id{', COALESCE(stage_dict.stage_name, NULL) AS stage_name' if stage_dict_cte else ''}
            FROM t{stage_dict_join}
            WHERE (
                -- ТОЛЬКО проверка в raw->field напрямую (для Centru данные хранятся здесь)
                (t.raw->'{branch_field_escaped}' IS NOT NULL AND (
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'string' AND (t.raw->>'{branch_field_escaped}') = %s)
                    OR
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'number' AND (t.raw->>'{branch_field_escaped}') = %s)
                    OR
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'object' AND (
                        COALESCE((t.raw->'{branch_field_escaped}')->>'id', (t.raw->'{branch_field_escaped}')->>'ID', '') = %s
                        OR COALESCE((t.raw->'{branch_field_escaped}')->>'value', (t.raw->'{branch_field_escaped}')->>'VALUE', '') = %s
                        OR COALESCE((t.raw->'{branch_field_escaped}')->>'name', (t.raw->'{branch_field_escaped}')->>'NAME', '') = %s
                    ))
                    OR
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'array' AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(t.raw->'{branch_field_escaped}') e
                        WHERE
                            (jsonb_typeof(e) IN ('string','number') AND trim(both '"' from e::text) = %s)
                            OR
                            (jsonb_typeof(e) = 'object' AND (
                                COALESCE(e->>'id', e->>'ID', '') = %s
                                OR COALESCE(e->>'value', e->>'VALUE', '') = %s
                                OR COALESCE(e->>'name', e->>'NAME', '') = %s
                            ))
                    ))
                ))
            )
        """
        params: List[Any] = [
            branch_value_str,  # string
            branch_value_str,  # number
            branch_value_str,  # object->id
            branch_value_str,  # object->value
            branch_value_str,  # object->name
            branch_value_str,  # array->string/number
            branch_value_str,  # array->object->id
            branch_value_str,  # array->object->value
            branch_value_str,  # array->object->name
        ]
        # Итого для Centru: 9 параметров (только raw напрямую)
    else:
        # Обычный SQL для всех остальных филиалов
        sql = f"""
            WITH t AS (
                SELECT 
                    id, 
                    raw, 
                    -- Извлекаем stageId из raw JSON (полный stageid, например DT1114_92:NEW)
                    COALESCE(raw->>'stageId', raw->'fields'->>'stageId', raw->>'STAGE_ID', raw->'fields'->>'STAGE_ID') AS stage_id
                FROM {table_safe}
            ){stage_dict_cte}
            SELECT 
                t.raw,
                t.stage_id{', COALESCE(stage_dict.stage_name, NULL) AS stage_name' if stage_dict_cte else ''}
            FROM t{stage_dict_join}
            WHERE (
                -- Проверяем в raw->'fields'->field (для большинства филиалов)
                (t.raw->'fields'->'{branch_field_escaped}' IS NOT NULL AND (
                    (jsonb_typeof(t.raw->'fields'->'{branch_field_escaped}') = 'string' AND (t.raw->'fields'->>'{branch_field_escaped}') = %s)
                    OR
                    (jsonb_typeof(t.raw->'fields'->'{branch_field_escaped}') = 'number' AND (t.raw->'fields'->>'{branch_field_escaped}') = %s)
                    OR
                    (jsonb_typeof(t.raw->'fields'->'{branch_field_escaped}') = 'object' AND (
                        COALESCE((t.raw->'fields'->'{branch_field_escaped}')->>'id', (t.raw->'fields'->'{branch_field_escaped}')->>'ID', '') = %s
                        OR COALESCE((t.raw->'fields'->'{branch_field_escaped}')->>'value', (t.raw->'fields'->'{branch_field_escaped}')->>'VALUE', '') = %s
                        OR COALESCE((t.raw->'fields'->'{branch_field_escaped}')->>'name', (t.raw->'fields'->'{branch_field_escaped}')->>'NAME', '') = %s
                    ))
                    OR
                    (jsonb_typeof(t.raw->'fields'->'{branch_field_escaped}') = 'array' AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(t.raw->'fields'->'{branch_field_escaped}') e
                        WHERE
                            (jsonb_typeof(e) IN ('string','number') AND trim(both '"' from e::text) = %s)
                            OR
                            (jsonb_typeof(e) = 'object' AND (
                                COALESCE(e->>'id', e->>'ID', '') = %s
                                OR COALESCE(e->>'value', e->>'VALUE', '') = %s
                                OR COALESCE(e->>'name', e->>'NAME', '') = %s
                            ))
                    ))
                ))
                OR
                -- Проверяем в raw->field напрямую (fallback для других случаев)
                (t.raw->'{branch_field_escaped}' IS NOT NULL AND (
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'string' AND (t.raw->>'{branch_field_escaped}') = %s)
                    OR
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'number' AND (t.raw->>'{branch_field_escaped}') = %s)
                    OR
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'object' AND (
                        COALESCE((t.raw->'{branch_field_escaped}')->>'id', (t.raw->'{branch_field_escaped}')->>'ID', '') = %s
                        OR COALESCE((t.raw->'{branch_field_escaped}')->>'value', (t.raw->'{branch_field_escaped}')->>'VALUE', '') = %s
                        OR COALESCE((t.raw->'{branch_field_escaped}')->>'name', (t.raw->'{branch_field_escaped}')->>'NAME', '') = %s
                    ))
                    OR
                    (jsonb_typeof(t.raw->'{branch_field_escaped}') = 'array' AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(t.raw->'{branch_field_escaped}') e
                        WHERE
                            (jsonb_typeof(e) IN ('string','number') AND trim(both '"' from e::text) = %s)
                            OR
                            (jsonb_typeof(e) = 'object' AND (
                                COALESCE(e->>'id', e->>'ID', '') = %s
                                OR COALESCE(e->>'value', e->>'VALUE', '') = %s
                                OR COALESCE(e->>'name', e->>'NAME', '') = %s
                            ))
                    ))
                ))
            )
        """
        params: List[Any] = [
            # Проверка в raw->'fields'->field (для большинства филиалов)
            branch_value_str,  # string
            branch_value_str,  # number
            branch_value_str,  # object->id
            branch_value_str,  # object->value
            branch_value_str,  # object->name
            branch_value_str,  # array->string/number
            branch_value_str,  # array->object->id
            branch_value_str,  # array->object->value
            branch_value_str,  # array->object->name
            # Проверка в raw->field напрямую (fallback)
            branch_value_str,  # string
            branch_value_str,  # number
            branch_value_str,  # object->id
            branch_value_str,  # object->value
            branch_value_str,  # object->name
            branch_value_str,  # array->string/number
            branch_value_str,  # array->object->id
            branch_value_str,  # array->object->value
            branch_value_str,  # array->object->name
        ]
        # Итого для остальных филиалов: 18 параметров (9 для raw->'fields' + 9 для raw напрямую)

    if category_id is not None:
        sql += """
            AND (
                jsonb_extract_path_text(t.raw, 'categoryId') = %s
                OR jsonb_extract_path_text(t.raw, 'fields', 'categoryId') = %s
                OR jsonb_extract_path_text(t.raw, 'CATEGORY_ID') = %s
                OR jsonb_extract_path_text(t.raw, 'fields', 'CATEGORY_ID') = %s
                OR jsonb_extract_path_text(t.raw, 'category_id') = %s
                OR jsonb_extract_path_text(t.raw, 'fields', 'category_id') = %s
            )
        """
        cid = str(int(category_id))
        params += [cid, cid, cid, cid, cid, cid]

    sql += " ORDER BY t.id ASC NULLS LAST LIMIT %s"
    params.append(int(limit))

    # Логируем SQL запрос и параметры для отладки
    is_centru_query = branch_value_str == "1668" or "centru" in branch_value_str.lower()
    
    print(f"DEBUG: pg_list_stock_raw: table={table_safe}, branch_field={branch_field}, branch_value={branch_value_str}, category_id={category_id}, limit={limit}", file=sys.stderr, flush=True)
    print(f"DEBUG: pg_list_stock_raw: params count={len(params)}, params={params}", file=sys.stderr, flush=True)
    
    if is_centru_query:
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU QUERY DETECTED *** branch_value={branch_value_str}, branch_field={branch_field}", file=sys.stderr, flush=True)
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU SQL (first 2000 chars): {sql[:2000]}", file=sys.stderr, flush=True)
        if len(sql) > 2000:
            print(f"DEBUG: pg_list_stock_raw: *** CENTRU SQL (last 500 chars): ...{sql[-500:]}", file=sys.stderr, flush=True)
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU PARAMS ({len(params)} total): {params}", file=sys.stderr, flush=True)
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU SQL LENGTH: {len(sql)} chars", file=sys.stderr, flush=True)
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            print(f"DEBUG: pg_list_stock_raw: SQL executed successfully, fetched {len(rows)} rows", file=sys.stderr, flush=True)
            
            if is_centru_query:
                print(f"DEBUG: pg_list_stock_raw: *** CENTRU QUERY RESULT *** Fetched {len(rows)} rows for Centru", file=sys.stderr, flush=True)
                if len(rows) == 0:
                    print(f"WARNING: pg_list_stock_raw: *** CENTRU HAS NO DATA! *** Check if branch_value={branch_value_str} matches data in database", file=sys.stderr, flush=True)
                    print(f"WARNING: pg_list_stock_raw: *** CENTRU DEBUG *** branch_field={branch_field}, branch_value={branch_value_str}, table={table_safe}", file=sys.stderr, flush=True)
                    # ПРОСТОЙ ТЕСТОВЫЙ ЗАПРОС - проверяем напрямую raw->field
                    try:
                        test_sql = f"""
                            SELECT COUNT(*) as cnt
                            FROM {table_safe}
                            WHERE (raw->>'{branch_field_escaped}') = %s
                        """
                        cur.execute(test_sql, [branch_value_str])
                        test_result = cur.fetchone()
                        test_count = test_result[0] if test_result else 0
                        print(f"DEBUG: pg_list_stock_raw: *** CENTRU SIMPLE TEST *** Direct query raw->>'{branch_field_escaped}' = '{branch_value_str}' found {test_count} rows", file=sys.stderr, flush=True)
                        
                        # Если простой запрос нашел данные, но основной не нашел - значит проблема в основном запросе
                        if test_count > 0:
                            print(f"ERROR: pg_list_stock_raw: *** CENTRU DATA EXISTS BUT NOT FOUND BY MAIN QUERY! *** Simple query found {test_count} rows, but main query found 0. Check SQL logic!", file=sys.stderr, flush=True)
                            # Попробуем выполнить упрощенную версию основного запроса
                            simple_main_sql = f"""
                                SELECT id, raw
                                FROM {table_safe}
                                WHERE (raw->>'{branch_field_escaped}') = %s
                                LIMIT 5
                            """
                            cur.execute(simple_main_sql, [branch_value_str])
                            simple_rows = cur.fetchall()
                            print(f"DEBUG: pg_list_stock_raw: *** CENTRU SIMPLE MAIN QUERY *** Found {len(simple_rows)} rows with simplified query", file=sys.stderr, flush=True)
                    except Exception as test_err:
                        print(f"ERROR: pg_list_stock_raw: *** CENTRU SIMPLE TEST FAILED *** {test_err}", file=sys.stderr, flush=True)
                        import traceback
                        print(f"ERROR: pg_list_stock_raw: *** CENTRU TEST TRACEBACK ***\n{traceback.format_exc()}", file=sys.stderr, flush=True)
                    # Попробуем найти, какие значения есть в базе для этого поля
                    try:
                        # Обновленный debug запрос - проверяет оба места
                        debug_sql = f"""
                            SELECT DISTINCT 
                                COALESCE(
                                    jsonb_typeof(raw->'fields'->'{branch_field_escaped}'),
                                    jsonb_typeof(raw->'{branch_field_escaped}'),
                                    'null'
                                ) as field_type,
                                COALESCE(
                                    raw->'fields'->>'{branch_field_escaped}',
                                    raw->>'{branch_field_escaped}',
                                    'NULL'
                                ) as field_value_str,
                                COALESCE(
                                    (raw->'fields'->'{branch_field_escaped}')::text,
                                    (raw->'{branch_field_escaped}')::text,
                                    'NULL'
                                ) as field_value_raw,
                                COUNT(*) as count
                            FROM {table_safe}
                            WHERE (
                                raw->'fields'->'{branch_field_escaped}' IS NOT NULL
                                OR raw->'{branch_field_escaped}' IS NOT NULL
                            )
                            GROUP BY field_type, field_value_str, field_value_raw
                            ORDER BY count DESC
                            LIMIT 20
                        """
                        cur.execute(debug_sql)
                        debug_rows = cur.fetchall()
                        print(f"DEBUG: pg_list_stock_raw: *** CENTRU SAMPLE VALUES *** Found {len(debug_rows)} distinct values in database:", file=sys.stderr, flush=True)
                        for dbg_row in debug_rows[:10]:
                            print(f"DEBUG: pg_list_stock_raw: *** CENTRU SAMPLE *** type={dbg_row[0]}, value_str={dbg_row[1]}, value_raw={dbg_row[2]}, count={dbg_row[3]}", file=sys.stderr, flush=True)
                        
                        # Также проверим, есть ли значение "1668" в любом формате
                        check_sql = f"""
                            SELECT COUNT(*) as total_count
                            FROM {table_safe}
                            WHERE (
                                (jsonb_typeof(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') = 'string' AND (COALESCE(raw->'fields', raw)->>'{branch_field_escaped}') = '1668')
                                OR
                                (jsonb_typeof(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') = 'number' AND (COALESCE(raw->'fields', raw)->>'{branch_field_escaped}') = '1668')
                                OR
                                (jsonb_typeof(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') = 'object' AND (
                                    COALESCE((COALESCE(raw->'fields', raw)->'{branch_field_escaped}')->>'id', '') = '1668'
                                    OR COALESCE((COALESCE(raw->'fields', raw)->'{branch_field_escaped}')->>'value', '') = '1668'
                                    OR COALESCE((COALESCE(raw->'fields', raw)->'{branch_field_escaped}')->>'ID', '') = '1668'
                                    OR COALESCE((COALESCE(raw->'fields', raw)->'{branch_field_escaped}')->>'VALUE', '') = '1668'
                                ))
                                OR
                                (jsonb_typeof(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') = 'array' AND EXISTS (
                                    SELECT 1
                                    FROM jsonb_array_elements(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') e
                                    WHERE
                                        (jsonb_typeof(e) IN ('string','number') AND trim(both '"' from e::text) = '1668')
                                        OR
                                        (jsonb_typeof(e) = 'object' AND (
                                            COALESCE(e->>'id', e->>'ID', '') = '1668'
                                            OR COALESCE(e->>'value', e->>'VALUE', '') = '1668'
                                        ))
                                ))
                            )
                        """
                        cur.execute(check_sql)
                        check_result = cur.fetchone()
                        total_with_1668 = check_result[0] if check_result else 0
                        print(f"DEBUG: pg_list_stock_raw: *** CENTRU CHECK 1668 *** Total rows with value '1668' in any format: {total_with_1668}", file=sys.stderr, flush=True)
                        
                        # Также проверим, есть ли данные с именем "Centru" или "centru"
                        check_centru_name_sql = f"""
                            SELECT COUNT(*) as total_count
                            FROM {table_safe}
                            WHERE (
                                (jsonb_typeof(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') = 'string' AND LOWER(COALESCE(raw->'fields', raw)->>'{branch_field_escaped}') LIKE '%centru%')
                                OR
                                (jsonb_typeof(COALESCE(raw->'fields', raw)->'{branch_field_escaped}') = 'object' AND (
                                    LOWER(COALESCE((COALESCE(raw->'fields', raw)->'{branch_field_escaped}')->>'name', '')) LIKE '%centru%'
                                    OR LOWER(COALESCE((COALESCE(raw->'fields', raw)->'{branch_field_escaped}')->>'NAME', '')) LIKE '%centru%'
                                ))
                            )
                        """
                        cur.execute(check_centru_name_sql)
                        check_name_result = cur.fetchone()
                        total_with_centru_name = check_name_result[0] if check_name_result else 0
                        print(f"DEBUG: pg_list_stock_raw: *** CENTRU CHECK NAME *** Total rows with 'centru' in name: {total_with_centru_name}", file=sys.stderr, flush=True)
                        
                        # Также проверим, есть ли вообще данные в таблице для этого поля
                        total_count_sql = f"SELECT COUNT(*) FROM {table_safe}"
                        cur.execute(total_count_sql)
                        total_count_result = cur.fetchone()
                        total_count = total_count_result[0] if total_count_result else 0
                        print(f"DEBUG: pg_list_stock_raw: *** CENTRU TABLE INFO *** Total rows in table: {total_count}", file=sys.stderr, flush=True)
                        
                        # Проверим, есть ли вообще поле branch_field в таблице
                        field_exists_sql = f"""
                            SELECT COUNT(*) 
                            FROM {table_safe}
                            WHERE COALESCE(raw->'fields', raw)->'{branch_field_escaped}' IS NOT NULL
                        """
                        cur.execute(field_exists_sql)
                        field_exists_result = cur.fetchone()
                        field_exists_count = field_exists_result[0] if field_exists_result else 0
                        print(f"DEBUG: pg_list_stock_raw: *** CENTRU FIELD INFO *** Rows with field '{branch_field_escaped}': {field_exists_count}", file=sys.stderr, flush=True)
                    except Exception as debug_e:
                        print(f"WARNING: pg_list_stock_raw: *** CENTRU DEBUG QUERY FAILED *** {debug_e}", file=sys.stderr, flush=True)
                        import traceback
                        print(f"WARNING: pg_list_stock_raw: *** CENTRU DEBUG TRACEBACK ***\n{traceback.format_exc()}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"ERROR: pg_list_stock_raw: SQL execution failed: {e}", file=sys.stderr, flush=True)
        print(f"ERROR: pg_list_stock_raw: SQL query (first 1000 chars): {sql[:1000]}", file=sys.stderr, flush=True)
        print(f"ERROR: pg_list_stock_raw: Params: {params}", file=sys.stderr, flush=True)
        import traceback
        print(f"ERROR: pg_list_stock_raw: Traceback: {traceback.format_exc()}", file=sys.stderr, flush=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch stock items: {str(e)}")

    out: List[Dict[str, Any]] = []
    for row in rows:
        try:
            if len(row) > 0:
                raw_obj = row[0]
                stage_id = row[1] if len(row) > 1 else None
                stage_name = row[2] if len(row) > 2 else None
                
                if isinstance(raw_obj, dict):
                    # Добавляем stage_id и stage_name в raw_obj для использования в классификации
                    if stage_id:
                        raw_obj["_stageId"] = stage_id
                    if stage_name:
                        raw_obj["_stage_name"] = stage_name
                    out.append(raw_obj)
                elif isinstance(raw_obj, str):
                    try:
                        parsed = json.loads(raw_obj)
                        if isinstance(parsed, dict):
                            if stage_id:
                                parsed["_stageId"] = stage_id
                            if stage_name:
                                parsed["_stage_name"] = stage_name
                            out.append(parsed)
                    except Exception:
                        pass
        except Exception as e:
            print(f"WARNING: pg_list_stock_raw: Failed to process row: {e}", file=sys.stderr, flush=True)
            continue
    
    print(f"DEBUG: pg_list_stock_raw: Successfully processed {len(out)} items", file=sys.stderr, flush=True)
    
    if is_centru_query:
        print(f"DEBUG: pg_list_stock_raw: *** CENTRU FINAL RESULT *** Processed {len(out)} items for Centru", file=sys.stderr, flush=True)
        if len(out) == 0:
            print(f"WARNING: pg_list_stock_raw: *** CENTRU FINAL WARNING *** No items processed for Centru after SQL query", file=sys.stderr, flush=True)
            print(f"WARNING: pg_list_stock_raw: *** CENTRU TROUBLESHOOTING ***", file=sys.stderr, flush=True)
            print(f"WARNING: pg_list_stock_raw: *** 1. Check if branch_value='{branch_value_str}' is correct for Centru", file=sys.stderr, flush=True)
            print(f"WARNING: pg_list_stock_raw: *** 2. Check if branch_field='{branch_field}' is correct", file=sys.stderr, flush=True)
            print(f"WARNING: pg_list_stock_raw: *** 3. Check if data in database uses different format (object with id/value/name)", file=sys.stderr, flush=True)
            print(f"WARNING: pg_list_stock_raw: *** 4. Check if Centru data exists in table '{table_safe}' at all", file=sys.stderr, flush=True)
            print(f"WARNING: pg_list_stock_raw: *** 5. Check logs above for SAMPLE VALUES to see actual data format", file=sys.stderr, flush=True)
    
    return out


# ---------------- PG list DEALS for Auto Date ----------------
def pg_list_deals_auto_date(
    conn,
    table: str,
    branch_field: str,
    branch_id: str,
    limit: int,
    assigned_by_ids: Optional[List[int]] = None,
    branch_name: Optional[str] = None,
    only_today: bool = True,
) -> List[Dict[str, Any]]:
    """
    Получает сделки для таблицы "Auto Date" с фильтрами:
    - category_name НЕ фильтруется (временно убрали category_id)
    - assigned_by_name - по assigned_by_ids для филиала (из PDF_FILTERS_ASSIGNED_BY_IDS)
    - DATE("Data - se da in chirie") = CURRENT_DATE (если only_today=True)
    
    ВАЖНО: НЕ фильтрует по стадии "în chirie" (как в SQL из BI конструктора)
    """
    table = _safe_ident(table, "table")

    sursa_f = _safe_ident(DEALS_F_SURSA, "DEALS_F_SURSA")
    carno_f = _safe_ident(DEALS_F_CARNO, "DEALS_F_CARNO")
    brand_f = _safe_ident(DEALS_F_BRAND, "DEALS_F_BRAND")
    model_f = _safe_ident(DEALS_F_MODEL, "DEALS_F_MODEL")
    fromdt_f = _safe_ident(DEALS_F_FROMDT, "DEALS_F_FROMDT")
    todt_f = _safe_ident(DEALS_F_TODT, "DEALS_F_TODT")

    sql = f"""
        SELECT
            id,
            id_2,
            title,
            raw,
            category_id,
            assigned_by_id,
            assigned_by_name,
            opportunity,
            {sursa_f} AS sursa_val,
            {carno_f} AS carno_val,
            {brand_f} AS brand_val,
            {model_f} AS model_val,
            {fromdt_f} AS fromdt_val,
            {todt_f} AS todt_val
        FROM {table}
        WHERE category_id::text = %s
        ORDER BY id DESC NULLS LAST
        LIMIT %s
    """
    params: List[Any] = [str(DEALS_CATEGORY_ID), int(limit)]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    out = [dict(r) for r in rows]

    # Фильтры по ответственным
    # Используем assigned_by_ids для филиала, если передан branch_name
    if assigned_by_ids is None and branch_name:
        # Пытаемся найти филиал в PDF_FILTERS_ASSIGNED_BY_IDS
        if branch_name in PDF_FILTERS_ASSIGNED_BY_IDS:
            assigned_by_ids = PDF_FILTERS_ASSIGNED_BY_IDS[branch_name]
        else:
            # Пробуем найти по lowercase
            bn = branch_name.lower()
            for k in PDF_FILTERS_ASSIGNED_BY_IDS.keys():
                if k.lower() == bn:
                    assigned_by_ids = PDF_FILTERS_ASSIGNED_BY_IDS[k]
                    break
    
    # Если assigned_by_ids не задан, используем старый способ по именам ТОЛЬКО для Centru
    use_ids_filter = assigned_by_ids is not None and len(assigned_by_ids) > 0
    if not use_ids_filter:
        # Fallback на имена только если это Centru или филиал не указан
        if not branch_name or branch_name.lower() == "centru":
            responsabil_names_lower = [name.lower().strip() for name in DEALS_FILTER_RESPONSABIL_NAMES]
            print(f"DEBUG: pg_list_deals_auto_date: Using name-based filter for Centru: {DEALS_FILTER_RESPONSABIL_NAMES}", file=sys.stderr, flush=True)
        else:
            # Для других филиалов без ID - не фильтруем по ответственным (показываем всех)
            print(f"WARNING: pg_list_deals_auto_date: Branch '{branch_name}' not found in PDF_FILTERS_ASSIGNED_BY_IDS, not filtering by responsabil", file=sys.stderr, flush=True)
            responsabil_names_lower = []
    else:
        print(f"DEBUG: pg_list_deals_auto_date: Using ID-based filter for branch '{branch_name}': {assigned_by_ids}", file=sys.stderr, flush=True)

    # Фильтруем по ответственным и по дате (DATE("Data - se da in chirie") = CURRENT_DATE)
    now_utc = datetime.now(timezone.utc)
    today_date = _today_in_report_tz()
    filtered: List[Dict[str, Any]] = []
    
    for d in out:
        raw = d.get("raw") if isinstance(d.get("raw"), dict) else {}
        
        # Фильтр по Responsabil
        responsabil_match = False
        if use_ids_filter:
            # Фильтруем по assigned_by_id
            assigned_by_id = d.get("assigned_by_id")
            if assigned_by_id:
                try:
                    assigned_id_int = int(assigned_by_id)
                    if assigned_id_int in assigned_by_ids:
                        responsabil_match = True
                except (ValueError, TypeError):
                    pass
        else:
            # Старый способ - по именам (только если есть имена для фильтрации)
            if responsabil_names_lower:
                assigned_name = d.get("assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                
                if assigned_name:
                    assigned_name_lower = assigned_name.lower().strip()
                    for name in responsabil_names_lower:
                        if name in assigned_name_lower or assigned_name_lower in name:
                            responsabil_match = True
                            break
            else:
                # Если нет фильтра по ответственным - пропускаем все (responsabil_match = True)
                responsabil_match = True
        
        # Фильтр по дате: DATE("Data - se da in chirie") = CURRENT_DATE
        date_match = True  # По умолчанию пропускаем все, если only_today=False
        if only_today:
            dt_from = _deal_dt_from_any(d)
            dt_from_date = None
            if dt_from:
                try:
                    dt_from_in_tz = dt_from.astimezone(REPORT_TZINFO)
                    dt_from_date = dt_from_in_tz.date()
                except Exception as e:
                    print(f"DEBUG: pg_list_deals_auto_date: Error processing fromdt for deal {d.get('id')}: {e}", file=sys.stderr, flush=True)
            # fallback: DATE_CREATE if fromdt отсутствует или не совпал
            if dt_from_date != today_date:
                raw_dt_create = _raw_get(raw, "DATE_CREATE") or _raw_get(raw, "date_create")
                dt_create = _to_dt(raw_dt_create)
                if dt_create:
                    try:
                        dt_create_in_tz = dt_create.astimezone(REPORT_TZINFO)
                        if dt_create_in_tz.date() == today_date:
                            dt_from_date = today_date
                    except Exception as e:
                        print(f"DEBUG: pg_list_deals_auto_date: Error processing DATE_CREATE for deal {d.get('id')}: {e}", file=sys.stderr, flush=True)
            date_match = dt_from_date == today_date
            if not date_match and len(filtered) < 5:
                print(f"DEBUG: pg_list_deals_auto_date: Deal {d.get('id')} skipped by date (fromdt={dt_from_date}, today={today_date})", file=sys.stderr, flush=True)
        
        # Применяем фильтры (Responsabil И дата)
        if responsabil_match and date_match:
            filtered.append(d)
            if len(filtered) <= 5:
                print(f"DEBUG: pg_list_deals_auto_date: Deal {d.get('id')} MATCHED - responsabil={responsabil_match}, date={date_match}", file=sys.stderr, flush=True)

    print(
        f"DEBUG: pg_list_deals_auto_date: filters -> {len(filtered)}/{len(out)} deals (expected date={today_date}, branch={branch_name})",
        file=sys.stderr,
        flush=True,
    )
    return filtered


# ---------------- PG list DEALS for Second Table (with filters) ----------------
def pg_list_deals_second_table(
    conn,
    table: str,
    limit: int = 5000,
    branch_name: Optional[str] = None,
    assigned_by_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Получает сделки для второй таблицы с фильтрами:
    - Status IN ("Contract închis", "Сделка провалена")
    - Responsabil - по assigned_by_ids для филиала (из PDF_FILTERS_ASSIGNED_BY_IDS) или по переданным ID
    - DATE(moved_time) = CURRENT_DATE
    """
    table = _safe_ident(table, "table")

    # Поля для выборки
    carno_f = _safe_ident(DEALS_F_CARNO, "DEALS_F_CARNO")
    brand_f = _safe_ident(DEALS_F_BRAND, "DEALS_F_BRAND")
    model_f = _safe_ident(DEALS_F_MODEL, "DEALS_F_MODEL")
    fromdt_f = _safe_ident(DEALS_F_FROMDT, "DEALS_F_FROMDT")
    gps_f = _safe_ident(DEALS_F_GPS, "DEALS_F_GPS")
    amenda_f = _safe_ident(DEALS_F_AMENDA, "DEALS_F_AMENDA")
    com_amenda_f = _safe_ident(DEALS_F_COM_AMENDA, "DEALS_F_COM_AMENDA")
    suma_ramb_f = _safe_ident(DEALS_F_SUMA_RAMBURSARE, "DEALS_F_SUMA_RAMBURSARE")
    com_refuz_f = _safe_ident(DEALS_F_COM_REFUZ, "DEALS_F_COM_REFUZ")

    sql = f"""
        SELECT
            id,
            id_2,
            title,
            raw,
            category_id,
            assigned_by_id,
            assigned_by_name,
            {carno_f} AS carno_val,
            {brand_f} AS brand_val,
            {model_f} AS model_val,
            {fromdt_f} AS fromdt_val,
            {gps_f} AS gps_val,
            {amenda_f} AS amenda_val,
            {com_amenda_f} AS com_amenda_val,
            {suma_ramb_f} AS suma_ramb_val,
            {com_refuz_f} AS com_refuz_val
        FROM {table}
        WHERE raw IS NOT NULL
          AND category_id::text = %s
        ORDER BY id DESC NULLS LAST
        LIMIT %s
    """
    params: List[Any] = [str(DEALS_CATEGORY_ID), int(limit)]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        
        # Отладка: проверяем первые 3 строки на наличие suma_ramb_val
        if rows:
            print(f"DEBUG: pg_list_deals_second_table: SQL query executed, got {len(rows)} rows", file=sys.stderr, flush=True)
            print(f"DEBUG: pg_list_deals_second_table: SQL field name: {suma_ramb_f}, DEALS_F_SUMA_RAMBURSARE: {DEALS_F_SUMA_RAMBURSARE}", file=sys.stderr, flush=True)
            for i, r in enumerate(rows[:3]):
                suma_val = r.get("suma_ramb_val")
                print(f"DEBUG: pg_list_deals_second_table: Row {i} (deal {r.get('id')}) - suma_ramb_val: {repr(suma_val)}", file=sys.stderr, flush=True)

    out: List[Dict[str, Any]] = []
    # Сегодняшняя дата для сравнения (без манипуляций)
    today_date = _today_in_report_tz()

    # Загружаем enum для стадий
    enum_stage = pg_load_enum_map(conn, "deal", "STAGE_ID")
    print(f"DEBUG: pg_list_deals_second_table: Loaded stage enum map: {len(enum_stage)} stages", file=sys.stderr, flush=True)

    # Фильтры по ответственным
    # Используем assigned_by_ids для филиала, если передан branch_name
    if assigned_by_ids is None and branch_name:
        # Пытаемся найти филиал в PDF_FILTERS_ASSIGNED_BY_IDS
        if branch_name in PDF_FILTERS_ASSIGNED_BY_IDS:
            assigned_by_ids = PDF_FILTERS_ASSIGNED_BY_IDS[branch_name]
        else:
            # Пробуем найти по lowercase
            bn = branch_name.lower()
            for k in PDF_FILTERS_ASSIGNED_BY_IDS.keys():
                if k.lower() == bn:
                    assigned_by_ids = PDF_FILTERS_ASSIGNED_BY_IDS[k]
                    break
    
    # Если assigned_by_ids не задан, используем старый способ по именам ТОЛЬКО для Centru
    use_ids_filter = assigned_by_ids is not None and len(assigned_by_ids) > 0
    if not use_ids_filter:
        # Fallback на имена только если это Centru или филиал не указан
        if not branch_name or branch_name.lower() == "centru":
            responsabil_names_lower = [name.lower().strip() for name in DEALS_FILTER_RESPONSABIL_NAMES]
            print(f"DEBUG: pg_list_deals_second_table: Using name-based filter for Centru: {DEALS_FILTER_RESPONSABIL_NAMES}", file=sys.stderr, flush=True)
        else:
            # Для других филиалов без ID - не фильтруем по ответственным (показываем всех)
            print(f"WARNING: pg_list_deals_second_table: Branch '{branch_name}' not found in PDF_FILTERS_ASSIGNED_BY_IDS, not filtering by responsabil", file=sys.stderr, flush=True)
            responsabil_names_lower = []
    else:
        print(f"DEBUG: pg_list_deals_second_table: Using ID-based filter for branch '{branch_name}': {assigned_by_ids}", file=sys.stderr, flush=True)

    for r in rows:
        raw = r.get("raw") if isinstance(r.get("raw"), dict) else {}

        # Фильтр по Status (STAGE_ID/STAGE_NAME)
        stage_id = _row_get_any(r, raw, "STAGE_ID") or _row_get_any(r, raw, "stage_id") or ""
        stage_name = _row_get_any(r, raw, "STAGE_NAME") or _row_get_any(r, raw, "stage_name") or ""
        
        # Если stage_name пустой, пытаемся получить из enum по stage_id
        if not stage_name and stage_id and enum_stage:
            stage_name = enum_stage.get(str(stage_id)) or enum_stage.get(stage_id) or ""
        
        # Проверяем статус по названию
        status_match = False
        if stage_name and stage_name in DEALS_FILTER_STATUS_VALUES:
            status_match = True
        # Также проверяем по stage_id (может быть "C20:WON" = "Contract închis", "C20:LOSE" = "Сделка провалена")
        elif stage_id:
            stage_id_upper = str(stage_id).upper()
            # "WON" обычно означает закрытую сделку (Contract închis)
            if "WON" in stage_id_upper and "Contract închis" in DEALS_FILTER_STATUS_VALUES:
                status_match = True
            # "LOSE" обычно означает проваленную сделку
            elif "LOSE" in stage_id_upper and "Сделка провалена" in DEALS_FILTER_STATUS_VALUES:
                status_match = True

        # Фильтр по Responsabil
        responsabil_match = False
        if use_ids_filter:
            # Фильтруем по assigned_by_id
            assigned_by_id = r.get("assigned_by_id")
            if assigned_by_id:
                try:
                    assigned_id_int = int(assigned_by_id)
                    if assigned_id_int in assigned_by_ids:
                        responsabil_match = True
                except (ValueError, TypeError):
                    pass
        else:
            # Старый способ - по именам (только если есть имена для фильтрации)
            if responsabil_names_lower:
                assigned_name = r.get("assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                
                if assigned_name:
                    assigned_name_lower = assigned_name.lower().strip()
                    for name in responsabil_names_lower:
                        if name in assigned_name_lower or assigned_name_lower in name:
                            responsabil_match = True
                            break
            else:
                # Если нет фильтра по ответственным - пропускаем все (responsabil_match = True)
                responsabil_match = True
                    
        # Фильтр по moved_time (DATE(moved_time) = CURRENT_DATE)
        # Пробуем разные варианты названий поля
        moved_time = None
        moved_time_str = ""
        moved_time_keys_found = []
        
        # Сначала пробуем стандартную функцию
        moved_time = _get_moved_time(raw, {})
        
        # Если не нашли, ищем все возможные варианты
        if not moved_time:
            # Ищем все ключи, которые могут содержать moved_time
            for key in raw.keys():
                key_lower = key.lower()
                if 'move' in key_lower and 'time' in key_lower:
                    moved_time_keys_found.append(key)
                    dt = _to_dt(raw.get(key))
                    if dt:
                        moved_time = dt
                        print(f"DEBUG: pg_list_deals_second_table: Deal {r.get('id')} - found moved_time as '{key}': {dt}", file=sys.stderr, flush=True)
                        break
        
        # Если все еще не нашли, выводим все ключи для отладки
        if not moved_time:
            all_time_keys = [k for k in raw.keys() if 'time' in k.lower() or 'date' in k.lower() or 'move' in k.lower()]
            if all_time_keys:
                print(f"DEBUG: pg_list_deals_second_table: Deal {r.get('id')} - moved_time not found. Available time/date keys: {all_time_keys[:10]}", file=sys.stderr, flush=True)
        
        moved_time_match = False
        if moved_time:
            try:
                # Простое сравнение: DATE(moved_time) = CURRENT_DATE
                # Приводим moved_time к REPORT_TZ перед взятием даты, чтобы сравнение было корректным
                moved_time_in_tz = moved_time.astimezone(REPORT_TZINFO)
                moved_date = moved_time_in_tz.date()  # Дата в REPORT_TZ
                today_date = _today_in_report_tz()  # Сегодняшняя дата в REPORT_TZ
                moved_time_str = moved_time_in_tz.strftime("%Y-%m-%d %H:%M")
                
                # Чистое сравнение дат (обе в одном часовом поясе)
                if moved_date == today_date:
                    moved_time_match = True
                else:
                    # Отладка: показываем разницу
                    days_diff = (today_date - moved_date).days
                    print(f"DEBUG: pg_list_deals_second_table: Deal {r.get('id')} - moved_time date: {moved_date} vs today: {today_date} (diff={days_diff} days)", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"DEBUG: pg_list_deals_second_table: Error processing moved_time for deal {r.get('id')}: {e}", file=sys.stderr, flush=True)
                pass
        else:
            # Если moved_time не найден, сделка не проходит фильтр
            print(f"DEBUG: pg_list_deals_second_table: Deal {r.get('id')} - moved_time not found", file=sys.stderr, flush=True)

        # Применяем фильтры (все должны совпадать)
        # Отладка для всех сделок, которые проходят хотя бы один фильтр
        if status_match or responsabil_match or moved_time_match:
            deal_id = r.get('id')
            stage_id_str = str(stage_id) if stage_id else "None"
            stage_name_str = stage_name if stage_name else "None"
            assigned_name_str = r.get("assigned_by_name") or _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or "None"
            assigned_id_str = str(r.get("assigned_by_id")) if r.get("assigned_by_id") else "None"
            
            print(
                f"DEBUG: pg_list_deals_second_table: Deal {deal_id} - "
                f"status={status_match} (stage_id={stage_id_str}, stage_name='{stage_name_str}'), "
                f"responsabil={responsabil_match} (id={assigned_id_str}, name='{assigned_name_str}'), "
                f"moved_time={moved_time_str} (match={moved_time_match}, expected={today_date})",
                file=sys.stderr,
                flush=True,
            )
        
        if status_match and responsabil_match and moved_time_match:
            out.append(dict(r))
            # Также показываем Data - se da in chirie для сравнения
            dt_from = _to_dt(r.get("fromdt_val") or _row_get_any(r, raw, DEALS_F_FROMDT))
            dt_from_str = ""
            if dt_from:
                try:
                    dt_from_str = dt_from.astimezone(REPORT_TZINFO).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    dt_from_str = str(dt_from)
            print(
                f"DEBUG: pg_list_deals_second_table: Deal {r.get('id')} MATCHED - status={status_match}, responsabil={responsabil_match}, moved_time={moved_time_str} (match={moved_time_match}), fromdt={dt_from_str}",
                file=sys.stderr,
                flush=True,
            )

    print(
        f"DEBUG: pg_list_deals_second_table: filters -> {len(out)}/{len(rows)} deals (expected moved_time={today_date})",
        file=sys.stderr,
        flush=True,
    )

    return out


# ---------------- PG list DEALS for Third Table (Prelungire) ----------------
def pg_list_deals_third_table(
    conn,
    table: str,
    branch_field: str,
    branch_id: str,
    limit: int = 5000,
    branch_name: Optional[str] = None,
    assigned_by_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Получает сделки для третьей таблицы (Prelungire) с фильтрами:
    - category_name IN ('Сделки - прокат')
    - assigned_by_name - по assigned_by_ids для филиала (из PDF_FILTERS_ASSIGNED_BY_IDS)
    - Фильтр по филиалу (branch_field = branch_id)
    - "Доход от продления за тек.день" IS NOT NULL
    """
    print(f"DEBUG: pg_list_deals_third_table: FUNCTION CALLED - branch_name={branch_name}, branch_id={branch_id}, assigned_by_ids={assigned_by_ids}", file=sys.stderr, flush=True)
    table = _safe_ident(table, "table")

    # Поля для выборки
    carno_f = _safe_ident(DEALS_F_CARNO, "DEALS_F_CARNO")
    brand_f = _safe_ident(DEALS_F_BRAND, "DEALS_F_BRAND")
    model_f = _safe_ident(DEALS_F_MODEL, "DEALS_F_MODEL")
    fromdt_f = _safe_ident(DEALS_F_FROMDT, "DEALS_F_FROMDT")
    return_dt_f = _safe_ident(DEALS_F_RETURN_DT, "DEALS_F_RETURN_DT")
    branch_f = _safe_ident(branch_field, "branch_field")
    
    # Поля для продления
    prel1_dt_f = _safe_ident(DEALS_F_PRELUNGIRE_1_DT, "DEALS_F_PRELUNGIRE_1_DT")
    prel2_dt_f = _safe_ident(DEALS_F_PRELUNGIRE_2_DT, "DEALS_F_PRELUNGIRE_2_DT")
    prel3_dt_f = _safe_ident(DEALS_F_PRELUNGIRE_3_DT, "DEALS_F_PRELUNGIRE_3_DT")
    prel4_dt_f = _safe_ident(DEALS_F_PRELUNGIRE_4_DT, "DEALS_F_PRELUNGIRE_4_DT")
    prel5_dt_f = _safe_ident(DEALS_F_PRELUNGIRE_5_DT, "DEALS_F_PRELUNGIRE_5_DT")
    prel1_pret_f = _safe_ident(DEALS_F_PRELUNGIRE_1_PRET, "DEALS_F_PRELUNGIRE_1_PRET")
    prel2_pret_f = _safe_ident(DEALS_F_PRELUNGIRE_2_PRET, "DEALS_F_PRELUNGIRE_2_PRET")
    prel3_pret_f = _safe_ident(DEALS_F_PRELUNGIRE_3_PRET, "DEALS_F_PRELUNGIRE_3_PRET")
    prel4_pret_f = _safe_ident(DEALS_F_PRELUNGIRE_4_PRET, "DEALS_F_PRELUNGIRE_4_PRET")
    prel5_pret_f = _safe_ident(DEALS_F_PRELUNGIRE_5_PRET, "DEALS_F_PRELUNGIRE_5_PRET")

    sql = f"""
                SELECT 
            id,
            id_2,
            title,
            raw,
            category_id,
            assigned_by_id,
            assigned_by_name,
            {carno_f} AS carno_val,
            {brand_f} AS brand_val,
            {model_f} AS model_val,
            {fromdt_f} AS fromdt_val,
            {return_dt_f} AS return_dt_val,
            {branch_f} AS branch_val,
            {prel1_dt_f} AS prel1_dt_val,
            {prel2_dt_f} AS prel2_dt_val,
            {prel3_dt_f} AS prel3_dt_val,
            {prel4_dt_f} AS prel4_dt_val,
            {prel5_dt_f} AS prel5_dt_val,
            {prel1_pret_f} AS prel1_pret_val,
            {prel2_pret_f} AS prel2_pret_val,
            {prel3_pret_f} AS prel3_pret_val,
            {prel4_pret_f} AS prel4_pret_val,
            {prel5_pret_f} AS prel5_pret_val
        FROM {table}
            WHERE
            raw IS NOT NULL
        ORDER BY id DESC NULLS LAST
        LIMIT %s
    """

    params: List[Any] = [int(limit)]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []

    print(f"DEBUG: pg_list_deals_third_table: START - fetched {len(rows)} rows from DB, branch_name={branch_name}, branch_id={branch_id}, assigned_by_ids={assigned_by_ids}", file=sys.stderr, flush=True)

    out: List[Dict[str, Any]] = []
    
    # Сегодняшняя дата для сравнения
    today_date = _today_in_report_tz()
    print(f"DEBUG: pg_list_deals_third_table: START - Today date: {today_date}, branch_name={branch_name}, branch_id={branch_id}, assigned_by_ids param: {assigned_by_ids}", file=sys.stderr, flush=True)
    
    # Фильтры по ответственным
    # Используем assigned_by_ids для филиала, если передан branch_name
    if assigned_by_ids is None and branch_name:
        # Пытаемся найти филиал в PDF_FILTERS_ASSIGNED_BY_IDS
        if branch_name in PDF_FILTERS_ASSIGNED_BY_IDS:
            assigned_by_ids = PDF_FILTERS_ASSIGNED_BY_IDS[branch_name]
        else:
            # Пробуем найти по lowercase
            bn = branch_name.lower()
            for k in PDF_FILTERS_ASSIGNED_BY_IDS.keys():
                if k.lower() == bn:
                    assigned_by_ids = PDF_FILTERS_ASSIGNED_BY_IDS[k]
                    break
    
    # Если assigned_by_ids не задан, используем старый способ по именам ТОЛЬКО для Centru
    use_ids_filter = assigned_by_ids is not None and len(assigned_by_ids) > 0
    if not use_ids_filter:
        # Fallback на имена только если это Centru или филиал не указан
        if not branch_name or branch_name.lower() == "centru":
            responsabil_names_lower = [name.lower().strip() for name in DEALS_FILTER_RESPONSABIL_NAMES]
            print(f"DEBUG: pg_list_deals_third_table: Using name-based filter for Centru: {DEALS_FILTER_RESPONSABIL_NAMES}", file=sys.stderr, flush=True)
        else:
            # Для других филиалов без ID - не фильтруем по ответственным (показываем всех)
            print(f"WARNING: pg_list_deals_third_table: Branch '{branch_name}' not found in PDF_FILTERS_ASSIGNED_BY_IDS, not filtering by responsabil", file=sys.stderr, flush=True)
            responsabil_names_lower = []
    else:
        print(f"DEBUG: pg_list_deals_third_table: Using ID-based filter for branch '{branch_name}': {assigned_by_ids}", file=sys.stderr, flush=True)
    
    # Нормализуем значение филиала для сравнения
    branch_id_normalized = _normalize_branch_value(branch_id)

    for r in rows:
        raw = r.get("raw") if isinstance(r.get("raw"), dict) else {}

        # Фильтр по филиалу - НЕ используется (согласно SQL из BI конструктора)
        branch_match = True  # Всегда пропускаем, так как фильтр по филиалу не нужен

        # Фильтр по Responsabil
        responsabil_match = False
        if use_ids_filter:
            # Фильтруем по assigned_by_id
            assigned_by_id = r.get("assigned_by_id")
            if assigned_by_id:
                try:
                    assigned_id_int = int(assigned_by_id)
                    if assigned_id_int in assigned_by_ids:
                        responsabil_match = True
                except (ValueError, TypeError):
                    pass
        else:
            # Старый способ - по именам (только если есть имена для фильтрации)
            if responsabil_names_lower:
                assigned_name = r.get("assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                
                if assigned_name:
                    assigned_name_lower = assigned_name.lower().strip()
                    for name in responsabil_names_lower:
                        if name in assigned_name_lower or assigned_name_lower in name:
                            responsabil_match = True
                            break
            else:
                # Если нет фильтра по ответственным - пропускаем все (responsabil_match = True)
                responsabil_match = True
        
        # Фильтр: "Доход от продления за тек.день" IS NOT NULL
        # Вычисляем "Доход от продления за тек.день" согласно CASE из SQL
        # Но показываем все сделки с продлениями, где есть хотя бы одна дата продления и цена
        dohod_ot_prodleniya = None
        
        # Получаем даты продления и цены
        prel1_dt = _to_dt(r.get("prel1_dt_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_1_DT))
        prel2_dt = _to_dt(r.get("prel2_dt_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_2_DT))
        prel3_dt = _to_dt(r.get("prel3_dt_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_3_DT))
        prel4_dt = _to_dt(r.get("prel4_dt_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_4_DT))
        prel5_dt = _to_dt(r.get("prel5_dt_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_5_DT))
        
        # Дата возврата без продления (UF_CRM_1749728773)
        dt_return_original = _to_dt(_row_get_any(r, raw, DEALS_F_TODT))
        
        # CASE для определения "Доход от продления за тек.день" согласно SQL из BI конструктора
        # Порядок ВАЖЕН! Должен точно соответствовать SQL
        dohod_ot_prodleniya = None
        
        # 1. Если CURRENT_DATE = дата первой продления (UF_CRM_1751889187) → берем цену второй продления (UF_CRM_1751886635)
        if prel1_dt:
            try:
                prel1_date = prel1_dt.astimezone(REPORT_TZINFO).date()
                if prel1_date == today_date:
                    pret_raw = r.get("prel2_pret_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_2_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            pret_val = float(pret_str) if pret_str else 0.0
                            if pret_val > 0:
                                dohod_ot_prodleniya = pret_val
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 2. Если CURRENT_DATE = дата второй продления (UF_CRM_1751894356) → берем цену третьей продления (UF_CRM_1751888121)
        if dohod_ot_prodleniya is None and prel2_dt:
            try:
                prel2_date = prel2_dt.astimezone(REPORT_TZINFO).date()
                if prel2_date == today_date:
                    pret_raw = r.get("prel3_pret_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_3_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            pret_val = float(pret_str) if pret_str else 0.0
                            if pret_val > 0:
                                dohod_ot_prodleniya = pret_val
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 3. Если CURRENT_DATE = дата возврата (UF_CRM_1749728773) → берем цену первой продления (UF_CRM_1751886604)
        if dohod_ot_prodleniya is None and dt_return_original:
            try:
                return_original_date = dt_return_original.astimezone(REPORT_TZINFO).date()
                if return_original_date == today_date:
                    pret_raw = r.get("prel1_pret_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_1_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            pret_val = float(pret_str) if pret_str else 0.0
                            if pret_val > 0:
                                dohod_ot_prodleniya = pret_val
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 4. Если CURRENT_DATE = дата третьей продления (UF_CRM_1751894409) → берем цену четвертой продления (UF_CRM_1751888928)
        if dohod_ot_prodleniya is None and prel3_dt:
            try:
                prel3_date = prel3_dt.astimezone(REPORT_TZINFO).date()
                if prel3_date == today_date:
                    pret_raw = r.get("prel4_pret_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_4_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            pret_val = float(pret_str) if pret_str else 0.0
                            if pret_val > 0:
                                dohod_ot_prodleniya = pret_val
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 5. Если CURRENT_DATE = дата четвертой продления (UF_CRM_1751894425) → берем цену пятой продления (UF_CRM_1751889092)
        if dohod_ot_prodleniya is None and prel4_dt:
            try:
                prel4_date = prel4_dt.astimezone(REPORT_TZINFO).date()
                if prel4_date == today_date:
                    pret_raw = r.get("prel5_pret_val") or _row_get_any(r, raw, DEALS_F_PRELUNGIRE_5_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            pret_val = float(pret_str) if pret_str else 0.0
                            if pret_val > 0:
                                dohod_ot_prodleniya = pret_val
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # Фильтр: "Доход от продления за тек.день" IS NOT NULL (согласно SQL)
        dohod_match = dohod_ot_prodleniya is not None and dohod_ot_prodleniya > 0
        
        # Отладка для первых 10 сделок или тех, которые проходят хотя бы один фильтр
        should_debug = len(out) < 10 or responsabil_match or dohod_match
        if should_debug:
            deal_id = r.get('id')
            assigned_id_str = str(r.get("assigned_by_id")) if r.get("assigned_by_id") else "None"
            assigned_name_str = r.get("assigned_by_name") or _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or "None"
            
            # Формируем строку с датами продления для отладки
            prel_dates_str = []
            if prel1_dt:
                try:
                    prel_dates_str.append(f"prel1={prel1_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if prel2_dt:
                try:
                    prel_dates_str.append(f"prel2={prel2_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if dt_return_original:
                try:
                    prel_dates_str.append(f"return_orig={dt_return_original.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            
            # Добавляем больше информации о датах продления
            all_prel_dates = []
            if prel1_dt:
                try:
                    all_prel_dates.append(f"prel1={prel1_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if prel2_dt:
                try:
                    all_prel_dates.append(f"prel2={prel2_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if prel3_dt:
                try:
                    all_prel_dates.append(f"prel3={prel3_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if prel4_dt:
                try:
                    all_prel_dates.append(f"prel4={prel4_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if prel5_dt:
                try:
                    all_prel_dates.append(f"prel5={prel5_dt.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            if dt_return_original:
                try:
                    all_prel_dates.append(f"return_orig={dt_return_original.astimezone(REPORT_TZINFO).date()}")
                except:
                    pass
            
            print(
                f"DEBUG: pg_list_deals_third_table: Deal {deal_id} - "
                f"responsabil={responsabil_match} (id={assigned_id_str}, name='{assigned_name_str}', expected_ids={assigned_by_ids}), "
                f"dohod={dohod_match} (value={dohod_ot_prodleniya}, today={today_date}), "
                f"all_dates={', '.join(all_prel_dates) if all_prel_dates else 'none'}",
                file=sys.stderr,
                flush=True,
            )
            
        # Применяем фильтры (Responsabil И доход от продления)
        # Фильтр по филиалу не используется (branch_match всегда True)
        if responsabil_match and dohod_match:
            out.append(dict(r))
            print(
                f"DEBUG: pg_list_deals_third_table: Deal {r.get('id')} MATCHED - responsabil={responsabil_match}, dohod={dohod_match} (value={dohod_ot_prodleniya})",
                file=sys.stderr,
                flush=True,
            )

    print(
        f"DEBUG: pg_list_deals_third_table: filters -> {len(out)}/{len(rows)} deals (Responsabil + Dohod filter, branch_id={branch_id}, branch_name={branch_name}, assigned_by_ids={assigned_by_ids})",
        file=sys.stderr,
        flush=True,
    )
    return out


def _build_deals_third_table_rows(deals: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Строит строки для третьей таблицы (Prelungire) с полями:
    - Nr tranzacției (title)
    - Responsabil (assigned_by_name)
    - Numar auto (uf_crm_1749550611) - жирным
    - Marca (uf_crm_1749556374)
    - Model (uf_crm_1749550573)
    - Data - se da in chirie (UF_CRM_1749728734) - формат: дд/мм/гггг НН:ММ
    - Data - return din chirie (UF_CRM_1749189804) - формат: дд/мм/гггг НН:ММ
    - Zile - вычисляемое: DATE_DIFF('day', CURRENT_DATE, "Data - return din chirie cu prelungire")
    - pret/zi - заглушка
    - Total prelungire - заглушка
    """
    out: List[List[Any]] = []
    now_utc = datetime.now(timezone.utc)
    today_local = _today_in_report_tz(now_utc)
    
    for idx, d in enumerate(deals):
        raw = d.get("raw") if isinstance(d.get("raw"), dict) else None

        # Nr tranzacției
        deal_title = d.get("title") or ""

        # Responsabil
        assigned_name = d.get("assigned_by_name") or ""
        if not assigned_name:
            assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
        if not assigned_name and d.get("assigned_by_id"):
            try:
                aid_int = int(d.get("assigned_by_id"))
                assigned_name = PDF_RESPONSIBLE_NAMES.get(aid_int) or ""
            except Exception:
                pass
        if not assigned_name:
            assigned_name = str(d.get("assigned_by_id") or "")

        # Numar auto (жирным будет в PDF)
        car_no = d.get("carno_val") or _row_get_any(d, raw, DEALS_F_CARNO) or ""
        
        # Пропускаем сделки без номера авто (пустые сделки)
        if not car_no or not str(car_no).strip():
            if idx < 3:
                print(
                    f"DEBUG: _build_deals_third_table_rows: Skipping deal {deal_title} - no car number (car_no is empty)",
                    file=sys.stderr,
                    flush=True,
                )
            continue

        # Marca
        marca = d.get("brand_val") or _row_get_any(d, raw, DEALS_F_BRAND) or ""

        # Model
        model = d.get("model_val") or _row_get_any(d, raw, DEALS_F_MODEL) or ""

        # Data - se da in chirie (формат: дд/мм/гггг НН:ММ)
        dt_from = _to_dt(d.get("fromdt_val") or _row_get_any(d, raw, DEALS_F_FROMDT))
        dt_from_s = _fmt_ddmmyyyy_hhmm(dt_from) if dt_from else ""

        # Data - return din chirie (si cu prelungire) (формат: дд/мм/гггг НН:ММ)
        dt_return = _to_dt(d.get("return_dt_val") or _row_get_any(d, raw, DEALS_F_RETURN_DT))
        dt_return_s = _fmt_ddmmyyyy_hhmm(dt_return) if dt_return else ""

        # Zile - вычисляемое: DATE_DIFF('day', CURRENT_DATE, "Data - return din chirie cu prelungire")
        zile = ""
        zile_int = 0
        if dt_return:
            try:
                return_date = dt_return.astimezone(REPORT_TZINFO).date()
                days_diff = (return_date - today_local).days
                zile_int = days_diff
                zile = str(days_diff)
            except Exception:
                zile = ""

        # Вычисляем "Доход от продления за тек.день" согласно CASE из SQL
        dohod_ot_prodleniya = None
        
        # Получаем даты продления
        prel1_dt = _to_dt(d.get("prel1_dt_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_1_DT))
        prel2_dt = _to_dt(d.get("prel2_dt_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_2_DT))
        prel3_dt = _to_dt(d.get("prel3_dt_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_3_DT))
        prel4_dt = _to_dt(d.get("prel4_dt_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_4_DT))
        prel5_dt = _to_dt(d.get("prel5_dt_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_5_DT))
        
        # Дата возврата без продления (UF_CRM_1749728773)
        dt_return_original = _to_dt(_row_get_any(d, raw, DEALS_F_TODT))
        
        # CASE для определения "Доход от продления за тек.день" (порядок важен!)
        # 1. Если CURRENT_DATE = дата первой продления → берем цену второй продления
        if prel1_dt:
            try:
                prel1_date = prel1_dt.astimezone(REPORT_TZINFO).date()
                if prel1_date == today_local:
                    pret_raw = d.get("prel2_pret_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_2_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            dohod_ot_prodleniya = float(pret_str) if pret_str else None
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 2. Если CURRENT_DATE = дата второй продления → берем цену третьей продления
        if dohod_ot_prodleniya is None and prel2_dt:
            try:
                prel2_date = prel2_dt.astimezone(REPORT_TZINFO).date()
                if prel2_date == today_local:
                    pret_raw = d.get("prel3_pret_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_3_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            dohod_ot_prodleniya = float(pret_str) if pret_str else None
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 3. Если CURRENT_DATE = дата возврата (UF_CRM_1749728773) → берем цену первой продления
        if dohod_ot_prodleniya is None and dt_return_original:
            try:
                return_original_date = dt_return_original.astimezone(REPORT_TZINFO).date()
                if return_original_date == today_local:
                    pret_raw = d.get("prel1_pret_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_1_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            dohod_ot_prodleniya = float(pret_str) if pret_str else None
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 4. Если CURRENT_DATE = дата третьей продления → берем цену четвертой продления
        if dohod_ot_prodleniya is None and prel3_dt:
            try:
                prel3_date = prel3_dt.astimezone(REPORT_TZINFO).date()
                if prel3_date == today_local:
                    pret_raw = d.get("prel4_pret_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_4_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            dohod_ot_prodleniya = float(pret_str) if pret_str else None
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # 5. Если CURRENT_DATE = дата четвертой продления → берем цену пятой продления
        if dohod_ot_prodleniya is None and prel4_dt:
            try:
                prel4_date = prel4_dt.astimezone(REPORT_TZINFO).date()
                if prel4_date == today_local:
                    pret_raw = d.get("prel5_pret_val") or _row_get_any(d, raw, DEALS_F_PRELUNGIRE_5_PRET)
                    if pret_raw:
                        try:
                            pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                            pret_str = pret_str.replace(',', '.')
                            dohod_ot_prodleniya = float(pret_str) if pret_str else None
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass
        
        # pret/zi - вычисляемое: "Доход от продления за тек.день" / Zile
        pret_zi = ""
        pret_zi_val = 0
        if dohod_ot_prodleniya is not None and dohod_ot_prodleniya > 0 and zile_int > 0:
            try:
                pret_zi_val = dohod_ot_prodleniya / zile_int
                pret_zi = f"{int(round(pret_zi_val))} MDL"
            except (ZeroDivisionError, ValueError, TypeError):
                pret_zi = ""
        
        # Вычисляем Pret/zi (euro): pret_zi_val / 20, округлить до целого
        pret_zi_euro = ""
        if pret_zi_val > 0:
            try:
                pret_zi_euro_val = pret_zi_val / 20
                pret_zi_euro = str(int(round(pret_zi_euro_val)))  # Без .0, целое число
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # Total Prelungire - вычисляемое: "Доход от продления за тек.день"
        total_prelungire = ""
        if dohod_ot_prodleniya is not None and dohod_ot_prodleniya > 0:
            total_prelungire = f"{int(round(dohod_ot_prodleniya))} MDL"

        out.append(
            [
                deal_title,
                assigned_name,
                car_no,  # будет жирным в PDF
                marca,
                model,
                dt_from_s,
                dt_return_s,
                zile,
                pret_zi,
                pret_zi_euro,  # Новая колонка с евро (будет жирным)
                total_prelungire,
            ]
        )
    
    # Сортируем по колонке "Zile" (индекс 4 после удаления FILIALA) от меньшего к большему
    def get_zile_value(row: List[Any]) -> int:
        if len(row) > 4:
            zile_str = str(row[4]) if row[4] else ""
            try:
                return int(zile_str) if zile_str else 999999
            except (ValueError, TypeError):
                return 999999
        return 999999
    
    out = sorted(out, key=get_zile_value)
    return out


# ---------------- PDF helpers ----------------
def _p(text: Any, style, bold: bool = False) -> Paragraph:
    if text is None:
        text = ""
    s = str(text).replace("\n", "<br/>")
    if bold and s:
        s = f"<b>{s}</b>"
    return Paragraph(s, style)


def _make_table_block_generic(
    title: str,
    header: List[str],
    rows: List[List[Any]],
    col_widths: List[float],
    font_name: str,
    styles,
    header_font_size: int = 9,
    body_font_size: int = 8,
    add_page_break: bool = False,
    bold_column_index: Optional[int] = None,
) -> List[Any]:
    h3 = ParagraphStyle("H3", parent=styles["Heading3"], fontName=font_name, fontSize=11, spaceAfter=6)
    cell = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName=font_name,
        fontSize=body_font_size,
        leading=body_font_size + 1,
    )

    # Приводим заголовки к капсу
    header_upper = [h.upper() if isinstance(h, str) else str(h).upper() for h in header]
    data: List[List[Any]] = [[_p(h, cell, bold=True) for h in header_upper]]

    if rows:
        for r in rows:
            row_data = []
            for idx, x in enumerate(r):
                # Делаем жирным, если это колонка с номером машины
                is_bold = (bold_column_index is not None and idx == bold_column_index)
                row_data.append(_p(x, cell, bold=is_bold))
            data.append(row_data)
    else:
        data.append([_p("", cell) for _ in range(len(header))])

    # Определяем имя жирного шрифта
    bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
    
    tbl = Table(data, repeatRows=1, colWidths=col_widths)
    style_commands = [
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, -1), font_name),
        ("FONTSIZE", (0, 0), (-1, 0), header_font_size + 1),  # Увеличен шрифт заголовков
        ("FONTSIZE", (0, 1), (-1, -1), body_font_size + 1),  # Увеличен шрифт данных
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),  # Центрируем все ячейки
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]
    
    # Делаем колонку с номером машины жирной, если указан индекс
    if bold_column_index is not None:
        if bold_font_name in pdfmetrics.getRegisteredFontNames():
            style_commands.append(("FONTNAME", (bold_column_index, 1), (bold_column_index, -1), bold_font_name))
        else:
            # Альтернатива: увеличиваем размер шрифта
            style_commands.append(("FONTSIZE", (bold_column_index, 1), (bold_column_index, -1), body_font_size + 1))
    
    tbl.setStyle(TableStyle(style_commands))

    block: List[Any] = []
    if title:
        block.append(Paragraph(title, h3))
    block += [tbl, Spacer(1, 8)]
    if add_page_break:
        block.append(PageBreak())
    return block


def _make_table_block(
    title: str,
    header: List[str],
    rows: List[List[Any]],
    font_name: str,
    styles,
    add_page_break: bool = False,
) -> List[Any]:
    h3 = ParagraphStyle("H3", parent=styles["Heading3"], fontName=font_name, fontSize=11, spaceAfter=6)
    cell = ParagraphStyle("Cell", parent=styles["Normal"], fontName=font_name, fontSize=8, leading=9)
    
    # Создаем отдельный стиль для жирного текста с жирным шрифтом
    bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
    cell_bold = ParagraphStyle("CellBold", parent=styles["Normal"], fontName=bold_font_name, fontSize=8, leading=9)

    # Преобразуем заголовок в Paragraph объекты
    data: List[List[Any]] = [[_p(h, cell, bold=True) for h in header]]

    if rows:
        for idx, r in enumerate(rows, start=1):
            car_no = r[0] if len(r) > 0 else ""
            brand = r[1] if len(r) > 1 else ""
            model = r[2] if len(r) > 2 else ""
            din_data = r[3] if len(r) > 3 else ""
            zile = r[4] if len(r) > 4 else ""

            # Используем жирный стиль напрямую для номера машины
            # Проверяем, зарегистрирован ли жирный шрифт
            if bold_font_name in pdfmetrics.getRegisteredFontNames():
                car_no_para = Paragraph(str(car_no).replace("\n", "<br/>"), cell_bold)
            else:
                # Если жирный шрифт не найден, используем обычный с увеличенным размером
                cell_bold_fallback = ParagraphStyle("CellBoldFallback", parent=styles["Normal"], fontName=font_name, fontSize=9, leading=10)
                car_no_para = Paragraph(str(car_no).replace("\n", "<br/>"), cell_bold_fallback)

            row_out = [
                _p(idx, cell),
                car_no_para,  # Номер машины - жирным через отдельный стиль
                _p(brand, cell),
                _p(model, cell),
                _p(din_data, cell),
                _p(zile, cell),
            ]
            data.append(row_out)
    else:
        data.append([_p("", cell) for _ in range(len(header))])

    col_widths = [
        10 * mm,
        28 * mm,
        26 * mm,
        45 * mm,
        26 * mm,
        18 * mm,
    ]

    # Определяем имя жирного шрифта
    bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
    
    tbl = Table(data, repeatRows=1, colWidths=col_widths)
    style_commands = [
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        # Применяем FONTNAME для всех колонок, кроме колонки "Nr Auto" (индекс 1)
        ("FONTNAME", (0, 0), (0, -1), font_name),  # Колонка 0
        ("FONTNAME", (2, 0), (-1, -1), font_name),  # Колонки 2 и далее
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("ALIGN", (6, 1), (6, -1), "CENTER"),
    ]
    
    # Не применяем FONTNAME для колонки "Nr Auto" (индекс 1), чтобы не переопределить стиль Paragraph
    tbl.setStyle(TableStyle(style_commands))

    block: List[Any] = [Paragraph(title, h3), tbl, Spacer(1, 8)]
    if add_page_break:
        block.append(PageBreak())
    return block


def _build_deals_auto_date_rows(deals: List[Dict[str, Any]], enum_map_sursa: Optional[Dict[str, str]] = None) -> List[List[Any]]:
    out: List[List[Any]] = []
    for idx, d in enumerate(deals):
        raw = d.get("raw") if isinstance(d.get("raw"), dict) else None

        deal_no = d.get("id") or d.get("id_2") or ""

        if idx < 5:
            print(
                f"DEBUG: _build_deals_auto_date_rows: Deal {deal_no} - assigned_by_id: {d.get('assigned_by_id')}, assigned_by_name: {repr(d.get('assigned_by_name'))}",
                file=sys.stderr,
                flush=True,
            )

        _ensure_assigned_name(d)
        assigned_name = d.get("assigned_by_name") or ""
        if not assigned_name:
            assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
        if not assigned_name:
            assigned_name = str(d.get("assigned_by_id") or "")

        sursa_raw = d.get("sursa_val") or _row_get_any(d, raw, DEALS_F_SURSA) or ""
        sursa = _enum_to_text(sursa_raw, enum_map_sursa or {})

        car_no = d.get("carno_val") or _row_get_any(d, raw, DEALS_F_CARNO) or ""
        
        # Пропускаем сделки без номера авто (пустые сделки)
        if not car_no or not str(car_no).strip():
            if idx < 3:
                print(
                    f"DEBUG: _build_deals_auto_date_rows: Skipping deal {deal_no} - no car number (car_no is empty)",
                    file=sys.stderr,
                    flush=True,
                )
            continue
        
        marca = d.get("brand_val") or _row_get_any(d, raw, DEALS_F_BRAND) or ""
        model = d.get("model_val") or _row_get_any(d, raw, DEALS_F_MODEL) or ""

        dt_from = _to_dt(d.get("fromdt_val") or _row_get_any(d, raw, DEALS_F_FROMDT))
        dt_to = _to_dt(d.get("todt_val") or _row_get_any(d, raw, DEALS_F_TODT))

        dt_from_s = _fmt_ddmmyyyy_hhmm(dt_from)
        dt_to_s = _fmt_ddmmyyyy_hhmm(dt_to)

        # Вычисляем Zile: DATE_DIFF('day', date("Data - se da in chirie"), date("Data - return din chirie"))
        zile = ""
        if dt_from and dt_to:
            try:
                days_diff = (dt_to.date() - dt_from.date()).days
                if days_diff >= 0:
                    zile = str(days_diff)
            except Exception:
                pass

        # Вычисляем Pret/zi: opportunity / Zile
        pret_zi = ""
        pret_zi_val = 0
        if zile:
            try:
                zile_int = int(zile)
                if zile_int > 0:
                    opportunity = float(d.get("opportunity") or 0)
                    pret_zi_val = opportunity / zile_int
                    pret_zi = f"{int(round(pret_zi_val))} MDL"
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # Вычисляем Pret/zi (euro): pret_zi_val / 20, округлить до целого
        pret_zi_euro = ""
        if pret_zi_val > 0:
            try:
                pret_zi_euro_val = pret_zi_val / 20
                pret_zi_euro = str(int(round(pret_zi_euro_val)))  # Без .0, целое число
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        # Servicii Aditionale (UF_CRM_1749212683547) — показываем только число
        servicii_val_raw = _row_get_any(d, raw, "UF_CRM_1749212683547") or ""
        servicii_aditionale_val = ""
        servicii_aditionale_num = 0.0
        if servicii_val_raw:
            try:
                import re

                # Берём первое число (с возможной запятой/точкой), игнорируя валюту
                match = re.search(r"[-+]?\d+(?:[.,]\d+)?", str(servicii_val_raw))
                if match:
                    num_str = match.group(0).replace(",", ".")
                    servicii_aditionale_num = float(num_str)
                    # Формат без валюты, убираем .0 если целое (для вычислений)
                    if servicii_aditionale_num.is_integer():
                        servicii_aditionale_val = str(int(servicii_aditionale_num))
                    else:
                        servicii_aditionale_val = num_str
            except Exception:
                pass
        # Для отображения добавляем код валюты, расчёты остаются числовыми
        if servicii_aditionale_val:
            servicii_aditionale_val = f"{servicii_aditionale_val} MDL"

        # Total Suma = opportunity (будет суммироваться в GROUP BY)
        total = ""
        opportunity = d.get("opportunity") or 0
        try:
            total_val = float(opportunity)
            if total_val > 0:
                total = f"{int(round(total_val))} MDL"
        except (ValueError, TypeError):
            pass

        out.append(
            [
                deal_no,
                assigned_name,
                sursa,
                car_no,
                marca,
                model,
                dt_from_s,
                dt_to_s,
                zile,
                pret_zi,
                pret_zi_euro,  # Новая колонка с евро (будет жирным)
                servicii_aditionale_val,
                total,
            ]
        )
    return out


def _build_deals_second_table_rows(deals: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Строит строки для второй таблицы с полями:
    - Nr tranzacției (title)
    - Responsabil (assigned_by_name)
    - Data - se da in chirie (UF_CRM_1749728734) - формат: дд/мм/гггг НН:ММ
    - Numar auto (uf_crm_1749550611) - жирным
    - Marca (uf_crm_1749556374)
    - Model (uf_crm_1749550573)
    - Viteaza GPS (uf_crm_1754124947425)
    - Amenda (UF_CRM_1749189180)
    - Comentariu Amenda (UF_CRM_1750430038)
    - Suma rambursare (UF_CRM_1750709202) - с префиксом MDL
    - Comentariu refuzului (uf_crm_1750709546)
    """
    out: List[List[Any]] = []
    for idx, d in enumerate(deals):
        raw = d.get("raw") if isinstance(d.get("raw"), dict) else None

        # Nr tranzacției
        deal_title = d.get("title") or ""

        # Responsabil (имя, а не числовой ID)
        # 1) пробуем уже сохранённое assigned_by_name (из синка)
        # 2) пробуем raw.ASSIGNED_BY_NAME (если Bitrix вернул)
        # 3) пробуем локальный mapping PDF_RESPONSIBLE_NAMES (без лишних запросов)
        # 4) в крайнем случае показываем ID
        _ensure_assigned_name(d)

        assigned_by_id = str(d.get("assigned_by_id") or "").strip()
        assigned_name = (str(d.get("assigned_by_name") or "").strip() or "")
        if not assigned_name:
            assigned_name = str(_raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or "").strip()
        if not assigned_name and assigned_by_id:
            assigned_name = str(PDF_RESPONSIBLE_NAMES.get(assigned_by_id, "")).strip()
        if not assigned_name:
            assigned_name = assigned_by_id

        # Data - se da in chirie (формат: дд/мм/гггг НН:ММ)
        dt_from = _to_dt(d.get("fromdt_val") or _row_get_any(d, raw, DEALS_F_FROMDT))
        dt_from_s = _fmt_ddmmyyyy_hhmm(dt_from) if dt_from else ""

        # Numar auto (жирным будет в PDF)
        car_no = d.get("carno_val") or _row_get_any(d, raw, DEALS_F_CARNO) or ""
        
        # Пропускаем сделки без номера авто (пустые сделки)
        if not car_no or not str(car_no).strip():
            if idx < 3:
                print(
                    f"DEBUG: _build_deals_second_table_rows: Skipping deal {d.get('id')} - no car number (car_no is empty)",
                    file=sys.stderr,
                    flush=True,
                )
            continue

        # Marca
        marca = d.get("brand_val") or _row_get_any(d, raw, DEALS_F_BRAND) or ""

        # Model
        model = d.get("model_val") or _row_get_any(d, raw, DEALS_F_MODEL) or ""

        # Viteaza GPS
        gps = d.get("gps_val") or _row_get_any(d, raw, DEALS_F_GPS) or ""

        # Amenda
        amenda = d.get("amenda_val") or _row_get_any(d, raw, DEALS_F_AMENDA) or ""

        # Comentariu Amenda
        com_amenda = d.get("com_amenda_val") or _row_get_any(d, raw, DEALS_F_COM_AMENDA) or ""

        # Suma rambursare (с префиксом MDL)
        # Пробуем разные варианты названия поля в raw JSON
        suma_ramb_raw = d.get("suma_ramb_val")  # Из SQL колонки (если есть)
        
        # Если не нашли в колонке, ищем в raw JSON с разными вариантами названия
        if not suma_ramb_raw and raw:
            # Пробуем разные варианты названия поля
            possible_keys = [
                DEALS_F_SUMA_RAMBURSARE,  # uf_crm_1750709202
                DEALS_F_SUMA_RAMBURSARE.upper(),  # UF_CRM_1750709202
                "UF_CRM_1750709202",
                "uf_crm_1750709202",
                "ufCrm1750709202",
            ]
            for key in possible_keys:
                value = raw.get(key)
                if value is not None and value != "":
                    suma_ramb_raw = value
                    if idx < 3:
                        print(f"DEBUG: _build_deals_second_table_rows: Deal {d.get('id')} - found suma_ramb with key '{key}': {repr(value)}", file=sys.stderr, flush=True)
                    break
        
        # Отладка для первых 5 сделок
        if idx < 5:
            print(
                f"DEBUG: _build_deals_second_table_rows: Deal {d.get('id')} - "
                f"suma_ramb_val from SQL: {repr(d.get('suma_ramb_val'))}, "
                f"DEALS_F_SUMA_RAMBURSARE: {DEALS_F_SUMA_RAMBURSARE}, "
                f"final suma_ramb_raw: {repr(suma_ramb_raw)}",
                file=sys.stderr,
                flush=True,
            )
            if raw:
                # Ищем все ключи, которые могут содержать это поле
                found_keys = [k for k in raw.keys() if '1750709202' in str(k) or 'rambursare' in str(k).lower() or 'ramburs' in str(k).lower()]
                if found_keys:
                    print(f"DEBUG: _build_deals_second_table_rows: Found keys with 1750709202 or 'ramburs': {found_keys}", file=sys.stderr, flush=True)
                    for fk in found_keys:
                        print(f"DEBUG: _build_deals_second_table_rows: Key '{fk}' value: {repr(raw.get(fk))}", file=sys.stderr, flush=True)
        
        suma_ramb = ""
        if suma_ramb_raw:
            try:
                # Пробуем преобразовать в число (может быть строка с числом)
                if isinstance(suma_ramb_raw, str):
                    # Убираем все нечисловые символы кроме точки, запятой и минуса
                    import re
                    cleaned = re.sub(r'[^0-9.,-]', '', suma_ramb_raw)
                    cleaned = cleaned.replace(',', '.')
                    suma_val = float(cleaned) if cleaned else 0
                else:
                    suma_val = float(suma_ramb_raw)
                if suma_val > 0:
                    suma_ramb = f"{int(round(suma_val))} MDL"
            except (ValueError, TypeError) as e:
                if idx < 3:
                    print(f"DEBUG: _build_deals_second_table_rows: Deal {d.get('id')} - error converting suma_ramb_raw '{suma_ramb_raw}': {e}", file=sys.stderr, flush=True)
                pass

        # Comentariu refuzului
        com_refuz = d.get("com_refuz_val") or _row_get_any(d, raw, DEALS_F_COM_REFUZ) or ""

        out.append(
            [
                deal_title,
                assigned_name,
                dt_from_s,
                car_no,  # будет жирным в PDF
                marca,
                model,
                gps,
                amenda,
                com_amenda,
                suma_ramb,
                com_refuz,
            ]
        )
    return out


def generate_pdf_stock_auto_split(
    raw_items: List[Dict[str, Any]],
    branch_name: str,
    branch_id: str,
    branch_field: str,
    branch_id_name_map: Dict[str, str],
    enum_map_brand: Dict[str, str],
    enum_map_model: Dict[str, str],
    deals_auto_date: Optional[List[Dict[str, Any]]] = None,
    enum_map_sursa: Optional[Dict[str, str]] = None,
    deals_second_table: Optional[List[Dict[str, Any]]] = None,
    deals_third_table: Optional[List[Dict[str, Any]]] = None,
) -> bytes:
    # ВСЕГДА используем weasyprint для всех филиалов (включая Centru)
    # Это гарантирует одинаковый формат PDF для всех филиалов
    if WEASYPRINT_AVAILABLE:
        try:
            return _generate_pdf_stock_auto_split_weasyprint(
                raw_items, branch_name, branch_id, branch_field, branch_id_name_map,
                enum_map_brand, enum_map_model, deals_auto_date, enum_map_sursa,
                deals_second_table, deals_third_table
            )
        except Exception as weasy_error:
            # Если weasyprint не работает, логируем ошибку, но НЕ используем ReportLab fallback
            # Это гарантирует, что все филиалы используют один и тот же формат
            print(f"ERROR: generate_pdf_stock_auto_split: WeasyPrint failed for '{branch_name}': {weasy_error}", file=sys.stderr, flush=True)
            import traceback
            print(f"ERROR: generate_pdf_stock_auto_split: WeasyPrint traceback:\n{traceback.format_exc()}", file=sys.stderr, flush=True)
            # Пробрасываем ошибку дальше, чтобы не использовать ReportLab
            raise
    else:
        # Если WeasyPrint недоступен, используем ReportLab (но это должно быть редко)
        print(f"WARNING: generate_pdf_stock_auto_split: WeasyPrint not available, using ReportLab for '{branch_name}'", file=sys.stderr, flush=True)
        return _generate_pdf_stock_auto_split_reportlab(
            raw_items, branch_name, branch_id, branch_field, branch_id_name_map,
            enum_map_brand, enum_map_model, deals_auto_date, enum_map_sursa,
            deals_second_table, deals_third_table
        )


def calculate_responsible_totals_global(
    deals_auto_date: Optional[List[Dict[str, Any]]],
    deals_third_table: Optional[List[Dict[str, Any]]],
    deals_second_table: Optional[List[Dict[str, Any]]] = None,
    third_table_rows: Optional[List[List[Any]]] = None,
) -> List[Tuple[str, float]]:
    """
    Вспомогательный расчёт дохода по ответственным (используется и в превью).
    Повторяет логику из PDF (opportunity + Servicii Aditionale + prodlenie + amenda - rambursare).
    """
    from collections import defaultdict
    import re

    totals_by_responsible: Dict[str, float] = defaultdict(float)

    # deals_auto_date: opportunity + Servicii Aditionale
    if deals_auto_date:
        for deal in deals_auto_date:
            raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
            assigned_name = deal.get("assigned_by_name") or ""
            if not assigned_name:
                assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
            if not assigned_name:
                assigned_name = f"ID:{deal.get('assigned_by_id')}" if deal.get("assigned_by_id") else "Неизвестно"

            try:
                opportunity = float(deal.get("opportunity") or 0)
                if opportunity > 0:
                    totals_by_responsible[assigned_name] += opportunity
            except Exception:
                pass

            # Servicii Aditionale
            try:
                servicii_raw = _row_get_any(deal, raw, "UF_CRM_1749212683547") or ""
                if servicii_raw:
                    match = re.search(r"[-+]?\d+(?:[.,]\d+)?", str(servicii_raw))
                    if match:
                        val = float(match.group(0).replace(",", "."))
                        if val > 0:
                            totals_by_responsible[assigned_name] += val
            except Exception:
                pass

    # deals_third_table: total prelungire
    if third_table_rows is not None and deals_third_table is not None and len(third_table_rows) > 0:
        for row in third_table_rows:
            if len(row) > 10:
                assigned_name = str(row[1]) if len(row) > 1 and row[1] else "Неизвестно"
                total_str = str(row[10]) if row[10] else ""
                numbers = re.findall(r"\d+", total_str)
                if numbers:
                    try:
                        total_val = float(numbers[0])
                        if total_val > 0:
                            totals_by_responsible[assigned_name] += total_val
                    except Exception:
                        pass
    elif deals_third_table:
        today_local = _today_in_report_tz(datetime.now(timezone.utc))
        for deal in deals_third_table:
            raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
            assigned_name = deal.get("assigned_by_name") or ""
            if not assigned_name:
                assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
            if not assigned_name:
                assigned_name = f"ID:{deal.get('assigned_by_id')}" if deal.get("assigned_by_id") else "Неизвестно"

            # Логика по датам продления (как в _build_deals_third_table_rows)
            prel_dts = [
                _to_dt(deal.get("prel1_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_1_DT)),
                _to_dt(deal.get("prel2_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_2_DT)),
                _to_dt(deal.get("prel3_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_3_DT)),
                _to_dt(deal.get("prel4_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_4_DT)),
                _to_dt(deal.get("prel5_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_5_DT)),
            ]
            pret_keys = [
                "prel2_pret_val",
                "prel3_pret_val",
                "prel4_pret_val",
                "prel5_pret_val",
                None,
            ]
            dohod_ot_prodleniya = None
            for idx_dt, prel_dt in enumerate(prel_dts):
                if prel_dt and dohod_ot_prodleniya is None:
                    try:
                        if prel_dt.astimezone(REPORT_TZINFO).date() == today_local:
                            pret_key = pret_keys[idx_dt]
                            if pret_key:
                                pret_raw = deal.get(pret_key) or _row_get_any(deal, raw, globals().get(f"DEALS_F_PRELUNGIRE_{idx_dt+2}_PRET", ""))
                            else:
                                pret_raw = None
                            if pret_raw:
                                pret_str = re.sub(r"[^0-9.,-]", "", str(pret_raw)).replace(",", ".")
                                dohod_ot_prodleniya = float(pret_str) if pret_str else None
                    except Exception:
                        pass
            if dohod_ot_prodleniya and dohod_ot_prodleniya > 0:
                totals_by_responsible[assigned_name] += dohod_ot_prodleniya

    # deals_second_table: amenda - rambursare
    if deals_second_table:
        for deal in deals_second_table:
            if not deal:
                continue
            try:
                assigned_name = deal.get("assigned_by_name") or ""
                if not assigned_name:
                    raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = f"ID:{deal.get('assigned_by_id')}" if deal.get("assigned_by_id") else "Неизвестно"
            except Exception:
                assigned_name = "Неизвестно"

            # Amenda
            try:
                amenda_val = deal.get("amenda_val")
                if amenda_val:
                    cleaned = re.sub(r"[^0-9.,-]", "", str(amenda_val)).replace(",", ".")
                    val = float(cleaned) if cleaned else 0
                    if val > 0:
                        totals_by_responsible[assigned_name] += val
            except Exception:
                pass

            # Rambursare (minus)
            try:
                suma_ramb_val = deal.get("suma_ramb_val")
                if suma_ramb_val:
                    cleaned = re.sub(r"[^0-9.,-]", "", str(suma_ramb_val)).replace(",", ".")
                    val = float(cleaned) if cleaned else 0
                    if val > 0:
                        totals_by_responsible[assigned_name] -= val
            except Exception:
                pass

    return list(totals_by_responsible.items())


def _generate_pdf_stock_auto_split_reportlab(
    raw_items: List[Dict[str, Any]],
    branch_name: str,
    branch_id: str,
    branch_field: str,
    branch_id_name_map: Dict[str, str],
    enum_map_brand: Dict[str, str],
    enum_map_model: Dict[str, str],
    deals_auto_date: Optional[List[Dict[str, Any]]] = None,
    enum_map_sursa: Optional[Dict[str, str]] = None,
    deals_second_table: Optional[List[Dict[str, Any]]] = None,
    deals_third_table: Optional[List[Dict[str, Any]]] = None,
) -> bytes:
    font_name, font_file = register_cyrillic_font()
    buf = BytesIO()

    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A3),
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TitleCyr", parent=styles["Title"], fontName=font_name, fontSize=14)
    normal = ParagraphStyle("NormalCyr", parent=styles["Normal"], fontName=font_name, fontSize=8, leading=9)

    header = ["№", "Nr Auto", "Marca", "Model", "Din data", "Zile"]
    now = datetime.now(timezone.utc)

    buckets: Dict[str, Any] = {
        "CHIRIE": [],
        "SERVICE": [],
        "PARCARE": [],
        "ALTE": {},
        "FARA_STATUS": [],
    }

    for raw_obj in raw_items:
        fields = _extract_fields_from_raw(raw_obj)
        bucket, subkey = stock_classify_default(fields, now)

        filiala_val = fields.get(branch_field) if branch_field else None
        filiala_norm = _normalize_branch_value(filiala_val)
        filiala_name = branch_id_name_map.get(filiala_norm, filiala_norm) if filiala_norm else branch_name

        car_no = fields.get(STOCK_F_CARNO, "") or ""
        if not car_no:
            car_no = raw_obj.get("title", "") or ""

        brand_raw = fields.get(STOCK_F_BRAND, "") or ""
        model_raw = fields.get(STOCK_F_MODEL, "") or ""

        brand = _enum_to_text(brand_raw, enum_map_brand)
        model = _enum_to_text(model_raw, enum_map_model)

        if not brand or (brand == str(brand_raw) and brand_raw):
            title = raw_obj.get("title", "") or ""
            if title:
                title_parts = title.strip().split()
                if title_parts:
                    brand = title_parts[0]

        moved_dt = _get_moved_time(raw_obj, fields)
        din_data = _fmt_ddmmyyyy(moved_dt)
        zile = _days_since(moved_dt, now)

        row = [car_no, brand, model, din_data, zile]

        if bucket == "ALTE":
            key = subkey or "Alt"
            buckets["ALTE"].setdefault(key, []).append(row)
        else:
            buckets[bucket].append(row)

    # Функция для сортировки по колонке "Zile" (индекс 4 после удаления FILIALA)
    def sort_by_zile(rows: List[List[Any]]) -> List[List[Any]]:
        def get_zile_value(row: List[Any]) -> int:
            if len(row) > 4:
                zile_str = str(row[4]) if row[4] else ""
                try:
                    return int(zile_str) if zile_str else 999999
                except (ValueError, TypeError):
                    return 999999
            return 999999
        return sorted(rows, key=get_zile_value)

    story: List[Any] = [
        Paragraph(f"STOCK AUTO — {branch_name}", title_style),
        Paragraph(datetime.now().strftime("%Y-%m-%d %H:%M"), normal),
        Spacer(1, 8),
    ]

    # Подготавливаем данные для layout 2x2
    page_width_landscape = landscape(A3)[0]
    page_height_landscape = landscape(A3)[1]
    available_width = page_width_landscape - 2 * 10 * mm
    available_height = page_height_landscape - 2 * 10 * mm - 50 * mm
    
    section_width = (available_width - 5 * mm) / 2
    section_height = (available_height - 5 * mm) / 2
    
    # Подготавливаем данные для каждой секции
    # Top-Left: SERVICE
    service_rows = sort_by_zile(buckets["SERVICE"])
    
    # Top-Right: ALTELE (ALTE) - объединяем все локации
    alte_rows_all: List[List[Any]] = []
    alte_items = sorted(buckets["ALTE"].items(), key=lambda x: str(x[0]).lower())
    for loc_name, rows in alte_items:
        alte_rows_all.extend(rows)
    alte_rows = sort_by_zile(alte_rows_all)
    
    # Bottom-Left: Disponibile (PARCARE + FARA_STATUS)
    disponibile_rows = sort_by_zile(buckets["PARCARE"] + buckets["FARA_STATUS"])
    
    # Bottom-Right: in chirie (CHIRIE)
    chirie_rows = sort_by_zile(buckets["CHIRIE"])
    
    # Уменьшаем ширину колонок для размещения 4 таблиц в формате 2x2
    original_col_widths = [8 * mm, 20 * mm, 18 * mm, 30 * mm, 18 * mm, 12 * mm]
    total_original_width = sum(original_col_widths)
    scale_factor = (section_width - 4 * mm) / total_original_width
    scaled_col_widths = [w * scale_factor for w in original_col_widths]
    
    # Создаем 4 таблицы напрямую для формата 2x2
    h2_style = ParagraphStyle("H2Small", parent=styles["Heading2"], fontName=font_name, fontSize=12, spaceAfter=1)
    cell = ParagraphStyle("Cell", parent=styles["Normal"], fontName=font_name, fontSize=18, leading=20)  # Увеличен в 2 раза (было 9)
    bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
    
    def create_table(rows_data: List[List[Any]], table_title: str) -> Table:
        # Приводим заголовки к капсу
        header_upper = [h.upper() if isinstance(h, str) else str(h).upper() for h in header]
        data = [[_p(h, cell, bold=True) for h in header_upper]]
        for row_num, r in enumerate(rows_data, 1):
            row_data = []
            # Добавляем нумерацию в начало
            row_data.append(_p(str(row_num), cell))
            # Обрабатываем остальные элементы строки
            for idx, x in enumerate(r):
                is_bold = (idx == 0)  # "Nr Auto" - индекс 0 в исходной строке (car_no)
                row_data.append(_p(x, cell, bold=is_bold))
            data.append(row_data)
        if not rows_data:
            data.append([_p("", cell) for _ in range(len(header))])
        
        tbl = Table(data, repeatRows=1, colWidths=scaled_col_widths)
        style_cmds = [
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("FONTSIZE", (0, 0), (-1, 0), 12),  # Заголовки
            ("FONTSIZE", (0, 1), (-1, -1), 12),  # Уменьшен (было 24) - данные
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),  # Центрируем все ячейки
            ("LEFTPADDING", (0, 0), (-1, -1), 2),
            ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]
        if bold_font_name in pdfmetrics.getRegisteredFontNames():
            style_cmds.append(("FONTNAME", (1, 1), (1, -1), bold_font_name))
        tbl.setStyle(TableStyle(style_cmds))
        return tbl
    
    service_tbl = create_table(service_rows, "SERVICE")
    alte_tbl = create_table(alte_rows, "ALTELE")
    disponibile_tbl = create_table(disponibile_rows, "Disponibile")
    chirie_tbl = create_table(chirie_rows, "in chirie")
    
    # Layout 2x2: размещаем таблицы последовательно, но с правильными размерами
    # Верхний ряд: SERVICE | ALTELE
    story.append(Paragraph("SERVICE", h2_style))
    story.append(Spacer(1, 1 * mm))  # Уменьшен отступ
    story.append(service_tbl)
    story.append(Spacer(1, 1 * mm))  # Уменьшен отступ
    story.append(Paragraph("ALTELE", h2_style))
    story.append(Spacer(1, 1 * mm))  # Уменьшен отступ
    story.append(alte_tbl)
    story.append(Spacer(1, 1 * mm))  # Минимальный отступ между рядами
    
    # Нижний ряд: Disponibile | in chirie (приводим к капсу)
    story.append(Paragraph("DISPONIBILE", h2_style))
    story.append(Spacer(1, 1 * mm))  # Уменьшен отступ
    story.append(disponibile_tbl)
    story.append(Spacer(1, 1 * mm))  # Уменьшен отступ
    story.append(Paragraph("IN CHIRIE", h2_style))
    story.append(Spacer(1, 1 * mm))  # Уменьшен отступ
    story.append(chirie_tbl)

    # Разрыв страницы перед таблицами deals (сделки)
    story.append(PageBreak())

    # --------- Auto Date (Deals) ----------
    if deals_auto_date is not None:
        deals_header = [
            "Nr tranzacției",
            "Responsabil",
            "Sursa",
            "Numar auto",
            "Marca",
            "Model",
            "Data se dă\nin chirie",
            "Data retur\ndin chirie",
            "Zile",
            "Pret/zi\n(MDL)",
            "Pret/zi\n(euro)",
            "Servicii\nAditionale",
            "Total\nsuma",
        ]

        deals_col_widths = [
            22 * mm,
            30 * mm,
            20 * mm,
            22 * mm,
            16 * mm,
            24 * mm,
            32 * mm,
            32 * mm,
            12 * mm,
            18 * mm,
            18 * mm,  # Новая колонка с евро
            18 * mm,  # Servicii Aditionale
            18 * mm,
        ]

        deal_rows = _build_deals_auto_date_rows(deals_auto_date, enum_map_sursa=enum_map_sursa)
        deals_count = len(deal_rows) if deal_rows else 0

        # Подсчитываем суммы по колонкам "Servicii Aditionale" (индекс 11) и "Total suma" (индекс 12)
        servicii_total = 0.0
        total_sum = 0.0
        if deal_rows:
            for row in deal_rows:
                import re
                if len(row) > 11:
                    servicii_str = str(row[11]) if row[11] else ""
                    num = re.findall(r"[-+]?\d+(?:[.,]\d+)?", servicii_str)
                    if num:
                        try:
                            servicii_total += float(num[0].replace(",", "."))
                        except (ValueError, TypeError):
                            pass
                if len(row) > 12:
                    total_str = str(row[12]) if row[12] else ""
                    numbers = re.findall(r"\d+", total_str)
                    if numbers:
                        try:
                            total_sum += float(numbers[0])
                        except (ValueError, TypeError):
                            pass

        story.append(Spacer(1, 3 * mm))  # Уменьшен отступ перед таблицей
        story.append(
            Paragraph(
                f"Auto Date (Deals) — {branch_name} | Deals: {deals_count}",
                ParagraphStyle("H2Cyr", parent=styles["Heading2"], fontName=font_name, fontSize=13, spaceAfter=2),
            )
        )
        story.append(Spacer(1, 2 * mm))  # Уменьшен отступ после заголовка

        # Создаем таблицу с жирным шрифтом для "Numar auto" (колонка 3, индекс 3)
        cell = ParagraphStyle(
            "Cell",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=9,  # Увеличен шрифт
            leading=10,
        )
        # Приводим заголовки к капсу
        deals_header_upper = [h.upper() if isinstance(h, str) else str(h).upper() for h in deals_header]
        data: List[List[Any]] = [[_p(h, cell, bold=True) for h in deals_header_upper]]

        if deal_rows:
            for r in deal_rows:
                row_data = []
                for idx, x in enumerate(r):
                    # Колонка "Numar auto" (индекс 3) - жирным
                    # Колонка "Pret/zi (euro)" (индекс 10) - жирным
                    is_bold = idx == 3 or idx == 10
                    row_data.append(_p(x, cell, bold=is_bold))
                data.append(row_data)
        else:
            data.append([_p("", cell) for _ in range(len(deals_header))])
        
        # Определяем имя жирного шрифта
        bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
        
        # Добавляем итоговую строку: суммируем Servicii Aditionale в общую сумму Total
        combined_total = servicii_total + total_sum
        if deal_rows and combined_total > 0:
            total_row: List[Any] = [_p("", cell) for _ in range(len(deals_header))]
            if combined_total.is_integer():
                total_text = f"{int(combined_total)} MDL"
            else:
                total_text = f"{combined_total:.2f} MDL".rstrip("0").rstrip(".")
            total_row[12] = _p(total_text, cell, bold=True)
            data.append(total_row)
        
        tbl = Table(data, repeatRows=1, colWidths=deals_col_widths)
        style_commands = [
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("FONTSIZE", (0, 0), (-1, 0), 14),  # Заголовки
            ("FONTSIZE", (0, 1), (-1, -1), 12),  # Уменьшен (было 24) - данные
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),  # Центрируем все ячейки
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
        
        # Делаем колонку "Numar auto" (индекс 3) жирной
        if bold_font_name in pdfmetrics.getRegisteredFontNames():
            style_commands.append(("FONTNAME", (3, 1), (3, -1), bold_font_name))
        else:
            # Альтернатива: увеличиваем размер шрифта
            style_commands.append(("FONTSIZE", (3, 1), (3, -1), 8))
        
        # Выделяем итоговую строку (последняя строка) - жирным и фоном
        if deal_rows and total_sum > 0:
            total_row_index = len(data) - 1
            style_commands.append(("BACKGROUND", (0, total_row_index), (-1, total_row_index), colors.lightgrey))
            style_commands.append(("FONTNAME", (0, total_row_index), (-1, total_row_index), bold_font_name))
            style_commands.append(("FONTSIZE", (0, total_row_index), (-1, total_row_index), 12))
        
        tbl.setStyle(TableStyle(style_commands))

        story += [tbl, Spacer(1, 2 * mm)]  # Уменьшен отступ после таблицы

    # --------- Second Table (Deals with filters) ----------
    if deals_second_table is not None:
        second_header = [
            "Deals",
            "Responsabil",
            "Data - se da\nin chirie",
            "Numar auto",
            "Marca",
            "Model",
            "Viteaza GPS",
            "Amenda",
            "Comentariu\nAmenda",
            "Suma\nrambursare",
            "Comentariu\nrefuzului",
        ]

        second_col_widths = [
            25 * mm,  # Nr tranzacției
            30 * mm,  # Responsabil
            28 * mm,  # Data - se da in chirie
            22 * mm,  # Numar auto (жирным)
            16 * mm,  # Marca
            20 * mm,  # Model
            18 * mm,  # Viteaza GPS
            18 * mm,  # Amenda
            25 * mm,  # Comentariu Amenda
            20 * mm,  # Suma rambursare
            25 * mm,  # Comentariu refuzului
        ]

        second_rows = _build_deals_second_table_rows(deals_second_table)
        second_count = len(second_rows) if second_rows else 0

        story.append(Spacer(1, 3 * mm))  # Уменьшен отступ перед таблицей
        story.append(
            Paragraph(
                f"Auto Primite — {branch_name} | Deals: {second_count}",
                ParagraphStyle("H2Cyr", parent=styles["Heading2"], fontName=font_name, fontSize=13, spaceAfter=2),
            )
        )
        story.append(Spacer(1, 2 * mm))  # Уменьшен отступ после заголовка

        # Создаем таблицу с жирным шрифтом для "Numar auto" (колонка 3, индекс 3)
        cell = ParagraphStyle(
            "Cell",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=9,  # Увеличен шрифт
            leading=10,
        )
        # Приводим заголовки к капсу
        second_header_upper = [h.upper() if isinstance(h, str) else str(h).upper() for h in second_header]
        data: List[List[Any]] = [[_p(h, cell, bold=True) for h in second_header_upper]]

        if second_rows:
            for r in second_rows:
                row_data = []
                for idx, x in enumerate(r):
                    # Колонка "Numar auto" (индекс 3) - жирным
                    is_bold = idx == 3
                    row_data.append(_p(x, cell, bold=is_bold))
                data.append(row_data)
        else:
            data.append([_p("", cell) for _ in range(len(second_header))])

        # Определяем имя жирного шрифта
        bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
        
        tbl = Table(data, repeatRows=1, colWidths=second_col_widths)
        style_commands = [
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("FONTSIZE", (0, 0), (-1, 0), 14),  # Заголовки
            ("FONTSIZE", (0, 1), (-1, -1), 12),  # Уменьшен (было 24) - данные
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),  # Центрируем все ячейки
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
        
        # Делаем колонку "Numar auto" (индекс 3) и "Pret/zi (euro)" (индекс 10) жирными
        if bold_font_name in pdfmetrics.getRegisteredFontNames():
            style_commands.append(("FONTNAME", (3, 1), (3, -1), bold_font_name))  # Numar auto
            style_commands.append(("FONTNAME", (10, 1), (10, -1), bold_font_name))  # Pret/zi (euro)
        else:
            # Альтернатива: увеличиваем размер шрифта
            style_commands.append(("FONTSIZE", (3, 1), (3, -1), 8))
            style_commands.append(("FONTSIZE", (10, 1), (10, -1), 8))
        
        tbl.setStyle(TableStyle(style_commands))

        story += [tbl, Spacer(1, 2 * mm)]  # Уменьшен отступ после таблицы

    # --------- Third Table (Prelungire) ----------
    if deals_third_table is not None:
        third_header = [
            "Deals",
            "Responsabil",
            "Numar auto",
            "Marca",
            "Model",
            "Data - se da\nin chirie",
            "Data - return\ndin chirie",
            "Zile",
            "pret/zi",
            "pret/zi\n(euro)",
            "Total\nprelungire",
        ]

        third_col_widths = [
            25 * mm,  # Nr tranzacției
            30 * mm,  # Responsabil
            22 * mm,  # Numar auto (жирным)
            16 * mm,  # Marca
            20 * mm,  # Model
            28 * mm,  # Data - se da in chirie
            28 * mm,  # Data - return din chirie
            15 * mm,  # Zile
            18 * mm,  # pret/zi
            18 * mm,  # pret/zi (euro) - новая колонка
            20 * mm,  # Total prelungire
        ]

        third_rows = _build_deals_third_table_rows(deals_third_table)
        third_count = len(third_rows) if third_rows else 0

        # Подсчитываем сумму в колонке "Total prelungire" (индекс 10, после добавления колонки евро)
        total_sum = 0
        if third_rows:
            for row in third_rows:
                if len(row) > 10:
                    total_str = str(row[10]) if row[10] else ""
                    # Извлекаем число из строки типа "600 MDL" или "4900 MDL"
                    import re
                    numbers = re.findall(r'\d+', total_str)
                    if numbers:
                        try:
                            total_sum += int(numbers[0])
                        except (ValueError, TypeError):
                            pass

        story.append(Spacer(1, 3 * mm))  # Уменьшен отступ перед таблицей
        story.append(
            Paragraph(
                f"Prelungire — {branch_name} | Deals: {third_count}",
                ParagraphStyle("H2Cyr", parent=styles["Heading2"], fontName=font_name, fontSize=13, spaceAfter=2),
            )
        )
        story.append(Spacer(1, 2 * mm))  # Уменьшен отступ после заголовка

        # Создаем таблицу с жирным шрифтом для "Numar auto" (колонка 2, индекс 2)
        cell = ParagraphStyle(
            "Cell",
            parent=styles["Normal"],
            fontName=font_name,
            fontSize=9,  # Увеличен шрифт
            leading=10,
        )
        # Приводим заголовки к капсу
        third_header_upper = [h.upper() if isinstance(h, str) else str(h).upper() for h in third_header]
        data: List[List[Any]] = [[_p(h, cell, bold=True) for h in third_header_upper]]

        if third_rows:
            for r in third_rows:
                row_data = []
                for idx, x in enumerate(r):
                    # Колонка "Numar auto" (индекс 2) - жирным
                    # Колонка "pret/zi (euro)" (индекс 9) - жирным
                    is_bold = idx == 2 or idx == 9
                    row_data.append(_p(x, cell, bold=is_bold))
                data.append(row_data)
        else:
            data.append([_p("", cell) for _ in range(len(third_header))])
        
        # Определяем имя жирного шрифта
        bold_font_name = "DejaVuSans-Bold" if font_name == "DejaVuSans" else font_name
        
        # Добавляем итоговую строку с суммой (только если есть данные и сумма > 0)
        if third_rows and total_sum > 0:
            total_row = [_p("", cell) for _ in range(len(third_header) - 1)]  # Пустые ячейки (10 штук)
            total_row.append(_p(f"{total_sum} MDL", cell, bold=True))  # Итоговая сумма в последней колонке (индекс 10)
            data.append(total_row)
        
        tbl = Table(data, repeatRows=1, colWidths=third_col_widths)
        style_commands = [
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("FONTSIZE", (0, 0), (-1, 0), 14),  # Заголовки
            ("FONTSIZE", (0, 1), (-1, -1), 12),  # Уменьшен (было 24) - данные
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),  # Центрируем все ячейки
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
        
        # Делаем колонку "Numar auto" (индекс 2) и "pret/zi (euro)" (индекс 9) жирными
        if bold_font_name in pdfmetrics.getRegisteredFontNames():
            style_commands.append(("FONTNAME", (2, 1), (2, -1), bold_font_name))  # Numar auto
            style_commands.append(("FONTNAME", (9, 1), (9, -1), bold_font_name))  # pret/zi (euro)
        else:
            # Альтернатива: увеличиваем размер шрифта
            style_commands.append(("FONTSIZE", (2, 1), (2, -1), 8))
            style_commands.append(("FONTSIZE", (9, 1), (9, -1), 8))
        
        # Выделяем итоговую строку (последняя строка) - жирным и фоном
        if third_rows and total_sum > 0:
            total_row_index = len(data) - 1
            style_commands.append(("BACKGROUND", (0, total_row_index), (-1, total_row_index), colors.lightgrey))
            style_commands.append(("FONTNAME", (0, total_row_index), (-1, total_row_index), bold_font_name))
            style_commands.append(("FONTSIZE", (0, total_row_index), (-1, total_row_index), 12))
        
        tbl.setStyle(TableStyle(style_commands))

        story += [tbl, Spacer(1, 2 * mm)]  # Уменьшен отступ после таблицы

    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            f"Font: {font_name} ({font_file})",
            ParagraphStyle("Footer", parent=normal, fontSize=6, leading=7),
        )
    )

    doc.build(story)
    return buf.getvalue()


def _generate_pdf_stock_auto_split_weasyprint(
    raw_items: List[Dict[str, Any]],
    branch_name: str,
    branch_id: str,
    branch_field: str,
    branch_id_name_map: Dict[str, str],
    enum_map_brand: Dict[str, str],
    enum_map_model: Dict[str, str],
    deals_auto_date: Optional[List[Dict[str, Any]]] = None,
    enum_map_sursa: Optional[Dict[str, str]] = None,
    deals_second_table: Optional[List[Dict[str, Any]]] = None,
    deals_third_table: Optional[List[Dict[str, Any]]] = None,
) -> bytes:
    """
    Генерирует PDF используя weasyprint с CSS Grid для 2x2 layout.
    Для Centru использует более компактный формат A4 landscape.
    """
    import html
    
    # Определяем, является ли это Centru
    is_centru_branch = "centru" in str(branch_name).lower() or str(branch_id) == "1668"
    
    # Обрабатываем данные (та же логика что и в reportlab версии)
    now = datetime.now(timezone.utc)
    header = ["№", "Nr Auto", "Marca", "Model", "Din data", "Zile"]
    
    buckets: Dict[str, Any] = {
        "CHIRIE": [],
        "SERVICE": [],
        "PARCARE": [],
        "ALTE": {},
        "FARA_STATUS": [],
    }
    
    for raw_obj in raw_items:
        fields = _extract_fields_from_raw(raw_obj)
        bucket, subkey = stock_classify_default(fields, now)
        
        filiala_val = fields.get(branch_field) if branch_field else None
        filiala_norm = _normalize_branch_value(filiala_val)
        filiala_name = branch_id_name_map.get(filiala_norm, filiala_norm) if filiala_norm else branch_name
        
        car_no = fields.get(STOCK_F_CARNO, "") or ""
        if not car_no:
            car_no = raw_obj.get("title", "") or ""
        
        brand_raw = fields.get(STOCK_F_BRAND, "") or ""
        model_raw = fields.get(STOCK_F_MODEL, "") or ""
        
        brand = _enum_to_text(brand_raw, enum_map_brand)
        model = _enum_to_text(model_raw, enum_map_model)
        
        if not brand or (brand == str(brand_raw) and brand_raw):
            title = raw_obj.get("title", "") or ""
            if title:
                title_parts = title.strip().split()
                if title_parts:
                    brand = title_parts[0]
        
        moved_dt = _get_moved_time(raw_obj, fields)
        din_data = _fmt_ddmmyyyy(moved_dt)
        zile = _days_since(moved_dt, now)
        
        # Добавляем нумерацию в начало: [№, Nr Auto, Marca, Model, Din data, Zile]
        row = ["", car_no, brand, model, din_data, zile]
        
        if bucket == "ALTE":
            key = subkey or "Alt"
            buckets["ALTE"].setdefault(key, []).append(row)
        else:
            buckets[bucket].append(row)
    
    # Сортировка по Zile (индекс 5 после удаления FILIALA)
    def sort_by_zile(rows: List[List[Any]]) -> List[List[Any]]:
        def get_zile_value(row: List[Any]) -> int:
            if len(row) > 5:
                zile_str = str(row[5]) if row[5] else ""
                try:
                    return int(zile_str) if zile_str else 999999
                except (ValueError, TypeError):
                    return 999999
            return 999999
        return sorted(rows, key=get_zile_value)
    
    service_rows = sort_by_zile(buckets["SERVICE"])
    alte_rows_all: List[List[Any]] = []
    alte_items = sorted(buckets["ALTE"].items(), key=lambda x: str(x[0]).lower())
    for loc_name, rows in alte_items:
        alte_rows_all.extend(rows)
    alte_rows = sort_by_zile(alte_rows_all)
    disponibile_rows = sort_by_zile(buckets["PARCARE"] + buckets["FARA_STATUS"])
    chirie_rows = sort_by_zile(buckets["CHIRIE"])
    
    # Добавляем нумерацию к строкам
    def add_numbers(rows: List[List[Any]]) -> List[List[Any]]:
        numbered = []
        for idx, row in enumerate(rows, 1):
            new_row = [str(idx)] + row[1:]  # Заменяем пустую нумерацию на номер
            numbered.append(new_row)
        return numbered
    
    service_rows = add_numbers(service_rows)
    alte_rows = add_numbers(alte_rows)
    disponibile_rows = add_numbers(disponibile_rows)
    chirie_rows = add_numbers(chirie_rows)
    
    # Подсчитываем данные для круговой диаграммы
    total_cars = len(service_rows) + len(alte_rows) + len(disponibile_rows) + len(chirie_rows)
    service_count = len(service_rows)
    chirie_count = len(chirie_rows)
    disponibile_count = len(disponibile_rows)
    alte_count = len(alte_rows)
    
    # Функция для генерации SVG donut chart
    def generate_donut_chart_svg(
        service: int, chirie: int, disponibile: int, alte: int, total: int,
        avg_days_service: float = 0, avg_days_chirie: float = 0, 
        avg_days_disponibile: float = 0, avg_days_alte: float = 0
    ) -> Tuple[str, List[Tuple[str, int, str, str]]]:
        """Генерирует SVG код для donut chart (круговая диаграмма с отверстием)
        Цвета зависят от среднего количества дней:
        - SERVICE, DISPONIBILE, ALTELE: чем больше дней, тем краснее
        - CHIRIE: чем больше дней, тем зеленее
        """
        import math
        
        if total == 0:
            # Пустая диаграмма
            return """
            <svg width="300" height="300" viewBox="0 0 300 300" xmlns="http://www.w3.org/2000/svg">
                <circle cx="150" cy="150" r="100" fill="none" stroke="#ccc" stroke-width="30"/>
                <text x="150" y="140" text-anchor="middle" font-size="28" font-weight="bold">Total:</text>
                <text x="150" y="175" text-anchor="middle" font-size="36" font-weight="bold">0</text>
            </svg>
            """, []
        
        # Параметры диаграммы (увеличены на 100% от исходного размера)
        center_x, center_y = 150, 150
        outer_radius = 100  # Увеличено на 100% от исходного (50 * 2.0)
        inner_radius = 60  # Увеличено на 100% от исходного (30 * 2.0)
        start_angle = -90  # Начинаем сверху (0° = справа, -90° = сверху)
        
        # Функция для вычисления цвета на основе дней (краснее = больше дней)
        def get_red_color(days: float, max_days: float = 365) -> str:
            """Возвращает цвет от светло-красного до темно-красного"""
            if max_days == 0:
                return '#FFB6B6'  # Светло-красный по умолчанию
            # Нормализуем от 0 до 1, но ограничиваем максимум
            intensity = min(days / max_days, 1.0)
            # От светло-красного (#FFB6B6) до темно-красного (#CC0000)
            r = int(255 - intensity * (255 - 204))  # 255 -> 204
            g = int(182 - intensity * (182 - 0))     # 182 -> 0
            b = int(182 - intensity * (182 - 0))     # 182 -> 0
            return f'#{r:02X}{g:02X}{b:02X}'
        
        # Функция для вычисления цвета на основе дней (зеленее = больше дней)
        def get_green_color(days: float, max_days: float = 365) -> str:
            """Возвращает цвет от светло-зеленого до темно-зеленого"""
            if max_days == 0:
                return '#B6FFB6'  # Светло-зеленый по умолчанию
            # Нормализуем от 0 до 1, но ограничиваем максимум
            intensity = min(days / max_days, 1.0)
            # От светло-зеленого (#B6FFB6) до темно-зеленого (#00CC00)
            r = int(182 - intensity * (182 - 0))     # 182 -> 0
            g = int(255 - intensity * (255 - 204))  # 255 -> 204
            b = int(182 - intensity * (182 - 0))     # 182 -> 0
            return f'#{r:02X}{g:02X}{b:02X}'
        
        # Находим максимальное количество дней для нормализации
        max_days = max(avg_days_service, avg_days_chirie, avg_days_disponibile, avg_days_alte, 1)
        
        # Цвета для сегментов - используем разные базовые цвета для лучшей различимости
        # SERVICE - красный (#FF6B6B)
        # DISPONIBILE - синий (#4A90E2)
        # ALTELE - оранжевый (#FFA500)
        # CHIRIE - зеленый (#90EE90)
        colors = {
            'service': '#FF6B6B',  # Яркий красный для SERVICE
            'chirie': '#90EE90',  # Светло-зеленый для CHIRIE
            'disponibile': '#4A90E2',  # Синий для DISPONIBILE
            'alte': '#FFA500'  # Оранжевый для ALTELE
        }
        
        # Данные для сегментов
        segments = []
        label_map = {
            'service': 'In service',
            'chirie': 'In chirie',
            'disponibile': 'Disponibile',
            'alte': 'Altele'
        }
        if service > 0:
            segments.append(('service', service, colors['service'], 'SERVICE'))
        if chirie > 0:
            segments.append(('chirie', chirie, colors['chirie'], 'IN CHIRIE'))
        if disponibile > 0:
            segments.append(('disponibile', disponibile, colors['disponibile'], 'DISPONIBILE'))
        if alte > 0:
            segments.append(('alte', alte, colors['alte'], 'ALTELE'))
        
        # Генерируем SVG path для каждого сегмента
        svg_paths = []
        current_angle = start_angle
        
        for seg_name, count, color, label in segments:
            # Вычисляем угол для этого сегмента
            angle = (count / total) * 360
            
            # Конвертируем углы в радианы
            start_rad = math.radians(current_angle)
            end_rad = math.radians(current_angle + angle)
            
            # Координаты для внешнего круга
            x1_outer = center_x + outer_radius * math.cos(start_rad)
            y1_outer = center_y + outer_radius * math.sin(start_rad)
            x2_outer = center_x + outer_radius * math.cos(end_rad)
            y2_outer = center_y + outer_radius * math.sin(end_rad)
            
            # Координаты для внутреннего круга (отверстие)
            x1_inner = center_x + inner_radius * math.cos(start_rad)
            y1_inner = center_y + inner_radius * math.sin(start_rad)
            x2_inner = center_x + inner_radius * math.cos(end_rad)
            y2_inner = center_y + inner_radius * math.sin(end_rad)
            
            # Флаг для больших дуг (если угол > 180°)
            large_arc_flag = 1 if angle > 180 else 0
            
            # Строим path для сегмента donut chart
            path_d = f"""
                M {x1_outer} {y1_outer}
                A {outer_radius} {outer_radius} 0 {large_arc_flag} 1 {x2_outer} {y2_outer}
                L {x2_inner} {y2_inner}
                A {inner_radius} {inner_radius} 0 {large_arc_flag} 0 {x1_inner} {y1_inner}
                Z
            """.strip()
            
            svg_paths.append(f'<path d="{path_d}" fill="{color}" stroke="white" stroke-width="2"/>')
            
            current_angle += angle
        
        # Собираем SVG (увеличен размер на 100% от исходного)
        svg_content = f"""
        <svg width="300" height="300" viewBox="0 0 300 300" xmlns="http://www.w3.org/2000/svg">
            {''.join(svg_paths)}
            <!-- Центральный текст -->
            <text x="{center_x}" y="{center_y - 8}" text-anchor="middle" font-size="16" font-weight="bold">Total:</text>
            <text x="{center_x}" y="{center_y + 24}" text-anchor="middle" font-size="22" font-weight="bold">{total}</text>
        </svg>
        """
        
        return svg_content, segments
    
    # Вычисляем среднее количество дней для каждой категории
    def calculate_avg_days(rows: List[List[Any]]) -> float:
        """Вычисляет среднее количество дней из строк (индекс 6 - Zile)"""
        if not rows:
            return 0.0
        days_list = []
        for row in rows:
            # После add_numbers структура: [номер, car_no, brand, model, din_data, zile] - Zile на индексе 5
            # Но если rows еще не прошли через add_numbers, структура: ["", car_no, brand, model, din_data, zile] - Zile тоже на индексе 5
            if len(row) > 5:
                zile_str = str(row[5]) if row[5] else ""
                try:
                    days = int(zile_str) if zile_str else 0
                    if days > 0:
                        days_list.append(days)
                except (ValueError, TypeError):
                    pass
        return sum(days_list) / len(days_list) if days_list else 0.0
    
    avg_days_service = calculate_avg_days(service_rows)
    avg_days_chirie = calculate_avg_days(chirie_rows)
    avg_days_disponibile = calculate_avg_days(disponibile_rows)
    avg_days_alte = calculate_avg_days(alte_rows)
    
    # Генерируем SVG диаграмму (возвращает SVG и segments для легенды)
    donut_chart_svg, chart_segments = generate_donut_chart_svg(
        service_count, chirie_count, disponibile_count, alte_count, total_cars,
        avg_days_service, avg_days_chirie, avg_days_disponibile, avg_days_alte
    )
    
    # Подсчет по всем ответственным (как в BI конструкторе)
    def calculate_responsible_totals(
        deals_auto_date: Optional[List[Dict[str, Any]]],
        deals_third_table: Optional[List[Dict[str, Any]]],
        deals_second_table: Optional[List[Dict[str, Any]]] = None,
        third_table_rows: Optional[List[List[Any]]] = None
    ) -> List[Tuple[str, float]]:
        """Подсчитывает общий доход по каждому ответственному из deals_auto_date, deals_third_table и deals_second_table
        Учитывает: opportunity + prodlenie_price + amenda - suma_rambursare
        """
        from collections import defaultdict
        import re
        
        totals_by_responsible: Dict[str, float] = defaultdict(float)
        
        # Подсчет из deals_auto_date (opportunity)
        if deals_auto_date:
            for deal in deals_auto_date:
                raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
                assigned_name = deal.get("assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = f"ID:{deal.get('assigned_by_id')}" if deal.get('assigned_by_id') else "Неизвестно"
                
                opportunity = float(deal.get("opportunity") or 0)
                if opportunity > 0:
                    totals_by_responsible[assigned_name] += opportunity
                
                # Добавляем Servicii Aditionale (UF_CRM_1749212683547)
                servicii_raw = _row_get_any(deal, raw, "UF_CRM_1749212683547") or ""
                if servicii_raw:
                    try:
                        match = re.search(r"[-+]?\d+(?:[.,]\d+)?", str(servicii_raw))
                        if match:
                            val = float(match.group(0).replace(",", "."))
                            if val > 0:
                                totals_by_responsible[assigned_name] += val
                    except Exception:
                        pass
        
        # Подсчет из deals_third_table (используем уже построенные строки таблицы, если они есть)
        if third_table_rows is not None and deals_third_table is not None and len(third_table_rows) > 0:
            # Используем уже построенные строки - извлекаем значения из колонки "TOTAL PRELUNGIRE" (индекс 10)
            import re
            for idx, row in enumerate(third_table_rows):
                if len(row) > 10:
                    # Получаем ответственного из строки (индекс 1)
                    assigned_name = str(row[1]) if len(row) > 1 and row[1] else "Неизвестно"
                    
                    # Извлекаем значение из колонки "TOTAL PRELUNGIRE" (индекс 10)
                    total_str = str(row[10]) if row[10] else ""
                    if total_str:
                        # Извлекаем число из строки типа "600 MDL" или "4900 MDL"
                        numbers = re.findall(r'\d+', total_str)
                        if numbers:
                            try:
                                total_val = float(numbers[0])
                                if total_val > 0:
                                    totals_by_responsible[assigned_name] += total_val
                            except (ValueError, TypeError):
                                pass
        elif deals_third_table:
            # Fallback: если строки не переданы, используем старую логику
            today_local = _today_in_report_tz(datetime.now(timezone.utc))
            for deal in deals_third_table:
                raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
                assigned_name = deal.get("assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = f"ID:{deal.get('assigned_by_id')}" if deal.get('assigned_by_id') else "Неизвестно"
                
                # Используем ту же логику что в _build_deals_third_table_rows для расчета "Доход от продления за тек.день"
                dohod_ot_prodleniya = None
                
                # Получаем даты продления
                prel1_dt = _to_dt(deal.get("prel1_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_1_DT))
                prel2_dt = _to_dt(deal.get("prel2_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_2_DT))
                prel3_dt = _to_dt(deal.get("prel3_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_3_DT))
                prel4_dt = _to_dt(deal.get("prel4_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_4_DT))
                prel5_dt = _to_dt(deal.get("prel5_dt_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_5_DT))
                
                # Дата возврата без продления
                dt_return_original = _to_dt(_row_get_any(deal, raw, DEALS_F_TODT))
                
                # CASE для определения "Доход от продления за тек.день" (та же логика что в _build_deals_third_table_rows)
                # 1. Если CURRENT_DATE = дата первой продления → берем цену второй продления
                if prel1_dt:
                    try:
                        prel1_date = prel1_dt.astimezone(REPORT_TZINFO).date()
                        if prel1_date == today_local:
                            pret_raw = deal.get("prel2_pret_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_2_PRET)
                            if pret_raw:
                                try:
                                    pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                                    pret_str = pret_str.replace(',', '.')
                                    dohod_ot_prodleniya = float(pret_str) if pret_str else None
                                except (ValueError, TypeError):
                                    pass
                    except Exception:
                        pass
                
                # 2. Если CURRENT_DATE = дата второй продления → берем цену третьей продления
                if dohod_ot_prodleniya is None and prel2_dt:
                    try:
                        prel2_date = prel2_dt.astimezone(REPORT_TZINFO).date()
                        if prel2_date == today_local:
                            pret_raw = deal.get("prel3_pret_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_3_PRET)
                            if pret_raw:
                                try:
                                    pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                                    pret_str = pret_str.replace(',', '.')
                                    dohod_ot_prodleniya = float(pret_str) if pret_str else None
                                except (ValueError, TypeError):
                                    pass
                    except Exception:
                        pass
                
                # 3. Если CURRENT_DATE = дата возврата (UF_CRM_1749728773) → берем цену первой продления
                if dohod_ot_prodleniya is None and dt_return_original:
                    try:
                        return_original_date = dt_return_original.astimezone(REPORT_TZINFO).date()
                        if return_original_date == today_local:
                            pret_raw = deal.get("prel1_pret_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_1_PRET)
                            if pret_raw:
                                try:
                                    pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                                    pret_str = pret_str.replace(',', '.')
                                    dohod_ot_prodleniya = float(pret_str) if pret_str else None
                                except (ValueError, TypeError):
                                    pass
                    except Exception:
                        pass
                
                # 4. Если CURRENT_DATE = дата третьей продления → берем цену четвертой продления
                if dohod_ot_prodleniya is None and prel3_dt:
                    try:
                        prel3_date = prel3_dt.astimezone(REPORT_TZINFO).date()
                        if prel3_date == today_local:
                            pret_raw = deal.get("prel4_pret_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_4_PRET)
                            if pret_raw:
                                try:
                                    pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                                    pret_str = pret_str.replace(',', '.')
                                    dohod_ot_prodleniya = float(pret_str) if pret_str else None
                                except (ValueError, TypeError):
                                    pass
                    except Exception:
                        pass
                
                # 5. Если CURRENT_DATE = дата четвертой продления → берем цену пятой продления
                if dohod_ot_prodleniya is None and prel4_dt:
                    try:
                        prel4_date = prel4_dt.astimezone(REPORT_TZINFO).date()
                        if prel4_date == today_local:
                            pret_raw = deal.get("prel5_pret_val") or _row_get_any(deal, raw, DEALS_F_PRELUNGIRE_5_PRET)
                            if pret_raw:
                                try:
                                    pret_str = re.sub(r'[^0-9.,-]', '', str(pret_raw))
                                    pret_str = pret_str.replace(',', '.')
                                    dohod_ot_prodleniya = float(pret_str) if pret_str else None
                                except (ValueError, TypeError):
                                    pass
                    except Exception:
                        pass
                
                # Добавляем "Доход от продления за тек.день" (это и есть TOTAL PRELUNGIRE для этой сделки)
                if dohod_ot_prodleniya is not None and dohod_ot_prodleniya > 0:
                    totals_by_responsible[assigned_name] += dohod_ot_prodleniya
        
        # Подсчет из deals_second_table (аменда добавляется, рамбурсаре вычитается)
        # deals_second_table уже отфильтрованы по moved_time = сегодня
        if deals_second_table:
            for deal in deals_second_table:
                raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
                assigned_name = deal.get("assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
                if not assigned_name:
                    assigned_name = f"ID:{deal.get('assigned_by_id')}" if deal.get('assigned_by_id') else "Неизвестно"
                
                # Получаем аменду (штраф) - добавляем к доходу
                amenda_raw = deal.get("amenda_val") or _row_get_any(deal, raw, DEALS_F_AMENDA) or ""
                if amenda_raw:
                    try:
                        if isinstance(amenda_raw, str):
                            # Убираем все нечисловые символы кроме точки, запятой и минуса
                            cleaned = re.sub(r'[^0-9.,-]', '', amenda_raw)
                            cleaned = cleaned.replace(',', '.')
                            amenda_val = float(cleaned) if cleaned else 0
                        else:
                            amenda_val = float(amenda_raw)
                        if amenda_val > 0:
                            totals_by_responsible[assigned_name] += amenda_val
                    except (ValueError, TypeError):
                        pass
                
                # Получаем рамбурсаре - вычитаем из дохода
                suma_ramb_raw = deal.get("suma_ramb_val") or ""
                if not suma_ramb_raw and raw:
                    # Пробуем разные варианты названия поля
                    possible_keys = [
                        DEALS_F_SUMA_RAMBURSARE,
                        DEALS_F_SUMA_RAMBURSARE.upper(),
                        "UF_CRM_1750709202",
                        "uf_crm_1750709202",
                        "ufCrm1750709202",
                    ]
                    for key in possible_keys:
                        value = raw.get(key)
                        if value is not None and value != "":
                            suma_ramb_raw = value
                            break
                
                if suma_ramb_raw:
                    try:
                        if isinstance(suma_ramb_raw, str):
                            # Убираем все нечисловые символы кроме точки, запятой и минуса
                            cleaned = re.sub(r'[^0-9.,-]', '', suma_ramb_raw)
                            cleaned = cleaned.replace(',', '.')
                            suma_val = float(cleaned) if cleaned else 0
                        else:
                            suma_val = float(suma_ramb_raw)
                        if suma_val > 0:
                            totals_by_responsible[assigned_name] -= suma_val
                    except (ValueError, TypeError):
                        pass
        
        # Сортируем по убыванию суммы
        sorted_totals = sorted(totals_by_responsible.items(), key=lambda x: x[1], reverse=True)
        return sorted_totals
    
    # Сначала строим строки для третьей таблицы, чтобы использовать их значения
    third_rows = None
    try:
        if deals_third_table is not None and len(deals_third_table) > 0:
            third_rows = _build_deals_third_table_rows(deals_third_table)
    except Exception as e:
        print(f"WARNING: generate_pdf_stock_auto_split: Failed to build third_table_rows: {e}", file=sys.stderr, flush=True)
        third_rows = None
    
    # Вычисляем суммы по ответственным (передаем уже построенные строки третьей таблицы)
    try:
        responsible_totals = calculate_responsible_totals(deals_auto_date, deals_third_table, deals_second_table, third_table_rows=third_rows)
    except Exception as e:
        print(f"WARNING: generate_pdf_stock_auto_split: Failed to calculate responsible_totals: {e}", file=sys.stderr, flush=True)
        import traceback
        print(f"WARNING: generate_pdf_stock_auto_split: Traceback: {traceback.format_exc()}", file=sys.stderr, flush=True)
        responsible_totals = []
    
    # Функция для генерации HTML таблицы
    def make_html_table(title: str, rows: List[List[Any]]) -> str:
        html_rows = []
        # Header - приводим к капсу
        html_rows.append("<tr>")
        for h in header:
            h_upper = str(h).upper() if isinstance(h, str) else str(h).upper()
            html_rows.append(f"<th>{html.escape(h_upper)}</th>")
        html_rows.append("</tr>")
        # Data rows
        # Индекс колонки "Zile" (последняя колонка)
        zile_idx = len(header) - 1
        
        # Определяем, является ли таблица "IN CHIRIE" (для зеленого градиента)
        is_chirie = "CHIRIE" in title.upper()
        
        if rows:
            for r in rows:
                html_rows.append("<tr>")
                # Убеждаемся, что количество ячеек соответствует заголовкам
                for idx in range(len(header)):
                    if idx < len(r):
                        cell = r[idx]
                    else:
                        cell = ""
                    cell_str = html.escape(str(cell) if cell is not None else "")
                    # "Nr Auto" (индекс 1 после №) - жирным
                    if idx == 1:
                        html_rows.append(f"<td><strong>{cell_str}</strong></td>")
                    # Колонка "Zile" - с градиентной заливкой
                    elif idx == zile_idx:
                        # Извлекаем количество дней
                        days = 0
                        try:
                            days = int(str(cell).strip()) if cell else 0
                        except (ValueError, TypeError):
                            days = 0
                        
                        # Вычисляем цвет в зависимости от дней
                        # 0 дней = прозрачный, чем больше дней - тем насыщеннее цвет
                        if days == 0:
                            # Прозрачный фон для 0 дней
                            html_rows.append(f'<td style="background-color: transparent; font-weight: bold;">{cell_str}</td>')
                        else:
                            # Используем более короткий диапазон (0-90 дней) для более заметного градиента
                            # Это обеспечит видимый цвет даже для 1 дня
                            max_days = 90.0
                            intensity = min(days / max_days, 1.0)
                            
                            if is_chirie:
                                # Для CHIRIE: чем больше дней, тем зеленее
                                # От светло-зеленого (230, 255, 230) к темно-зеленому (144, 238, 144)
                                # Даже 1 день будет иметь заметный светло-зеленый цвет
                                r = int(255 - intensity * (255 - 144))  # 255 -> 144
                                g = int(255 - intensity * (255 - 238))  # 255 -> 238
                                b = int(255 - intensity * (255 - 144))  # 255 -> 144
                            else:
                                # Для SERVICE, DISPONIBILE, ALTELE: чем больше дней, тем краснее
                                # От светло-красного (255, 230, 230) к темно-красному (255, 182, 193)
                                # Даже 1 день будет иметь заметный светло-красный цвет
                                r = int(255 - intensity * (255 - 255))  # 255 -> 255
                                g = int(255 - intensity * (255 - 182))  # 255 -> 182
                                b = int(255 - intensity * (255 - 193))  # 255 -> 193
                            
                            bg_color = f"#{r:02X}{g:02X}{b:02X}"
                            html_rows.append(f'<td style="background-color: {bg_color}; font-weight: bold;">{cell_str}</td>')
                    else:
                        html_rows.append(f"<td>{cell_str}</td>")
                html_rows.append("</tr>")
        else:
            html_rows.append("<tr>" + "<td></td>" * len(header) + "</tr>")
        
        return f"""
        <div class="table-section" style="page-break-inside: avoid !important; page-break-after: avoid !important; page-break-before: avoid !important;">
            <h3 style="page-break-after: avoid !important; page-break-before: avoid !important;">{html.escape(title)}</h3>
            <table style="page-break-inside: avoid !important; page-break-before: avoid !important;">
                {''.join(html_rows)}
            </table>
        </div>
        """
    
    # Генерируем HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{
                size: {'A4 landscape' if is_centru_branch else 'A3 landscape'};  /* Для Centru используем A4 landscape - более компактно */
                margin: {'3mm' if is_centru_branch else '8mm'};  /* Для Centru минимальные поля */
            }}
            body {{
                font-family: Arial, sans-serif;
                font-size: 7pt;  /* Компактный размер для всех филиалов */
                margin: 0;
                padding: 0;
            }}
            h1 {{
                font-size: 10pt;  /* Компактный размер для всех филиалов */
                margin-bottom: 1mm;  /* Компактный отступ для всех филиалов */
                page-break-after: avoid !important;  /* Заголовок не должен быть отдельно */
                page-break-before: avoid !important;
            }}
            p {{
                page-break-after: avoid !important;  /* Дата не должна быть отдельно */
                page-break-before: avoid !important;
                margin-bottom: {'0.5mm' if is_centru_branch else '1mm'};  /* Минимальный отступ */
            }}
            .stock-auto-page {{
                page-break-after: auto;  /* Разрешаем разрыв после основного содержимого для deals */
                page-break-inside: avoid !important;  /* Не разрываем внутри */
                page-break-before: avoid !important;  /* Не разрываем перед */
                orphans: 10;  /* Минимум 10 строк внизу страницы */
                widows: 10;  /* Минимум 10 строк вверху страницы */
            }}
            /* Двухколоночный layout: диаграмма слева, таблицы справа */
            .main-layout {{
                display: flex;
                flex-direction: row;
                gap: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru минимальный gap */
                margin: {'0.5mm 0' if is_centru_branch else '2mm 0'};  /* Для Centru минимальный margin */
                align-items: flex-start;
                page-break-inside: avoid !important;  /* Не разрываем основной layout */
                page-break-before: avoid !important;  /* Не разрываем перед layout */
                page-break-after: avoid !important;  /* Не разрываем после layout */
            }}
            .left-column {{
                flex: 0 0 {'200px' if is_centru_branch else '320px'};  /* Для Centru очень компактная ширина */
                display: flex;
                flex-direction: column;
                min-width: 0;  /* Позволяет shrink */
                page-break-inside: avoid !important;  /* Не разрываем левую колонку */
                page-break-before: avoid !important;
            }}
            .right-column {{
                flex: 1 1 auto;  /* Занимает оставшееся пространство для таблиц */
                display: flex;
                flex-direction: column;
                page-break-inside: avoid !important;  /* Не разрываем правую колонку */
                page-break-before: avoid !important;
            }}
            .grid-container {{
                display: flex;
                flex-wrap: wrap;
                gap: {'0.5mm' if is_centru_branch else '1mm'};  /* Для Centru минимальный отступ */
            }}
            .grid-container .table-section {{
                flex: 1 1 calc(50% - 0.5mm);  /* Две колонки с учетом gap */
                min-width: 0;  /* Позволяет shrink */
            }}
            .table-section {{
                display: flex;
                flex-direction: column;
            }}
            .table-section h3 {{
                font-size: 8pt;  /* Компактный размер для всех филиалов */
                margin: 0 0 0.5mm 0;  /* Компактный отступ для всех филиалов */
                font-weight: bold;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 7pt;  /* Компактный размер для всех филиалов */
                table-layout: fixed;
            }}
            th, td {{
                border: 0.5pt solid #ccc;
                padding: 1pt;  /* Компактный padding для всех филиалов */
                text-align: center;  /* Центрируем все ячейки */
                vertical-align: middle;
                overflow: hidden;
                text-overflow: ellipsis;
            }}
            th {{
                background-color: #f0f0f0;
                font-weight: bold;
                font-size: 8pt;  /* Компактный размер для всех филиалов */
                text-align: center;
            }}
            td strong {{
                font-weight: bold;
            }}
            /* Оптимизированные ширины колонок для STOCK AUTO таблиц */
            .table-section table th:nth-child(1),
            .table-section table td:nth-child(1) {{
                width: {'3mm' if is_centru_branch else '4mm'};  /* № - для Centru компактнее */
            }}
            .table-section table th:nth-child(2),
            .table-section table td:nth-child(2) {{
                width: {'14mm' if is_centru_branch else '18mm'};  /* Nr Auto - для Centru компактнее */
            }}
            .table-section table th:nth-child(3),
            .table-section table td:nth-child(3) {{
                width: {'12mm' if is_centru_branch else '15mm'};  /* Marca - для Centru компактнее */
            }}
            .table-section table th:nth-child(4),
            .table-section table td:nth-child(4) {{
                width: {'20mm' if is_centru_branch else '25mm'};  /* Model - для Centru компактнее */
            }}
            .table-section table th:nth-child(5),
            .table-section table td:nth-child(5) {{
                width: {'15mm' if is_centru_branch else '20mm'};  /* Filiala - для Centru компактнее */
            }}
            .table-section table th:nth-child(6),
            .table-section table td:nth-child(6) {{
                width: {'12mm' if is_centru_branch else '15mm'};  /* Din data - для Centru компактнее */
            }}
            .table-section table th:nth-child(7),
            .table-section table td:nth-child(7) {{
                width: {'6mm' if is_centru_branch else '8mm'};  /* Zile - для Centru компактнее */
            }}
            /* Стили для диаграммы, легенды и Total Venit - вертикальный layout */
            .chart-section {{
                display: flex;
                flex-direction: column;
                gap: {'0.5mm' if is_centru_branch else '1.5mm'};  /* Для Centru минимальный gap */
                background-color: #f9f9f9;
                border: 1pt solid #ddd;
                border-radius: 3pt;
                padding: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru минимальный padding */
                min-height: 0;  /* Позволяет shrink */
                overflow: visible;  /* Не обрезаем контент */
                page-break-inside: avoid !important;  /* Не разрываем секцию */
                page-break-before: avoid !important;
            }}
            .chart-container {{
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                padding: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru минимальный padding */
                flex: 0 0 auto;
                min-width: {'180px' if is_centru_branch else '250px'};  /* Для Centru меньше */
                background-color: transparent;
                border: none;
            }}
            .chart-container svg {{
                max-width: 100%;
                height: auto;
                width: {'150px' if is_centru_branch else '250px'};  /* Для Centru очень компактно */
            }}
            .chart-legend {{
                padding: 0;
                background-color: transparent;
                border: none;
                display: flex;
                flex-direction: column;
                gap: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru меньше gap */
                border-top: 1pt solid #ddd;
                padding-top: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru меньше */
                margin-top: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru меньше */
            }}
            .chart-legend-content {{
                display: flex;
                flex-direction: column;
            }}
            .chart-legend h4 {{
                font-size: 8pt;  /* Компактный размер для всех филиалов */
                margin: 0 0 0.5mm 0;  /* Компактный отступ для всех филиалов */
                font-weight: bold;
            }}
            .chart-legend-item {{
                display: flex;
                align-items: center;
                margin-bottom: 0.25mm;  /* Компактный отступ для всех филиалов */
                font-size: 7pt;  /* Компактный размер для всех филиалов */
            }}
            .chart-legend-color {{
                width: {'6mm' if is_centru_branch else '8mm'};  /* Для Centru меньше */
                height: {'2mm' if is_centru_branch else '3mm'};  /* Для Centru меньше */
                margin-right: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru меньше */
                border: 0.5pt solid #999;
                flex-shrink: 0;
            }}
            /* Стили для Total Venit (теперь сверху) */
            .total-venit-corner {{
                display: flex;
                flex-direction: column;
                padding: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru минимальный padding */
                background-color: transparent;
                border: none;
                border-bottom: 1pt solid #ddd;
                padding-bottom: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru меньше */
                margin-bottom: {'1mm' if is_centru_branch else '2mm'};  /* Для Centru меньше */
            }}
            .total-venit-label {{
                font-size: 6pt;  /* Компактный размер для всех филиалов */
                margin-bottom: 0.5mm;  /* Компактный отступ для всех филиалов */
                font-weight: bold;
                color: #333;
            }}
            .total-venit-sum {{
                font-size: 20pt;  /* Компактный размер для всех филиалов */
                font-weight: bold;
                text-align: center;
                color: #333;
                display: flex;
                align-items: baseline;
                justify-content: center;
                gap: 1mm;  /* Компактный отступ для всех филиалов */
                white-space: nowrap;
                flex-wrap: nowrap;
            }}
        </style>
    </head>
    <body>
        <div class="stock-auto-page" style="page-break-inside: avoid !important; page-break-after: avoid !important; page-break-before: avoid !important;">
            <!-- Двухколоночный layout: диаграмма слева, таблицы справа -->
            <div class="main-layout" style="page-break-inside: avoid !important; page-break-before: avoid !important; page-break-after: avoid !important;">
                <!-- Левая колонка: Диаграмма и легенда -->
                <div class="left-column">
                    <div class="chart-section">
                        <!-- Total Venit - только общая сумма (сверху) -->
                        <div class="total-venit-corner">
                            <div class="total-venit-label">Total Venit astazi</div>
                            <div class="total-venit-sum">
                                <span>{int(round(sum(total for _, total in responsible_totals))) if responsible_totals else 0}</span>
                                <span style="font-size: 0.6em;">MDL</span>
                            </div>
                        </div>
                        
                        <!-- Диаграмма (снизу) -->
                        <div class="chart-container">
                            {donut_chart_svg}
                        </div>
                        
                        <!-- Легенда для диаграммы -->
                        <div class="chart-legend">
                            <div class="chart-legend-content">
                                <h4>Distribuția auto pe statuturi</h4>
                                {''.join([
                                    f'''<div class="chart-legend-item">
                                        <div class="chart-legend-color" style="background-color: {color};"></div>
                                        <span>{('In service' if seg_name == 'service' else 'In chirie' if seg_name == 'chirie' else 'Disponibile' if seg_name == 'disponibile' else 'Altele')}: {count} ({((count / total_cars) * 100):.1f}%)</span>
                                    </div>'''
                                    for seg_name, count, color, label in chart_segments
                                ]) if chart_segments else ''}
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Правая колонка: Таблицы -->
                <div class="right-column" style="page-break-inside: avoid !important; page-break-before: avoid !important;">
                    <div class="grid-container" style="page-break-inside: avoid !important; page-break-before: avoid !important;">
                        {make_html_table("SERVICE", service_rows)}
                        {make_html_table("ALTELE", alte_rows)}
                        {make_html_table("DISPONIBILE", disponibile_rows)}
                        {make_html_table("IN CHIRIE", chirie_rows)}
                    </div>
                </div>
            </div>
        </div>
    """
    
    # Функция для генерации HTML таблицы deals (определяем ДО использования)
    def make_html_table_deals(title: str, header: List[str], rows: List[List[Any]], bold_column_indices: List[int] = None, total_row_index: int = -1) -> str:
        if bold_column_indices is None:
            bold_column_indices = []
        html_rows = []
        # Header - приводим к капсу
        html_rows.append("<tr>")
        for h in header:
            # Разбиваем по \n, экранируем каждую часть, приводим к капсу, соединяем через <br/>
            h_str = str(h)
            parts = h_str.split('\n')
            h_escaped_parts = [html.escape(part.upper()) for part in parts]
            h_with_br = '<br/>'.join(h_escaped_parts)
            html_rows.append(f"<th>{h_with_br}</th>")
        html_rows.append("</tr>")
        # Data rows
        if rows:
            for row_idx, r in enumerate(rows):
                is_total_row = (total_row_index >= 0 and row_idx == total_row_index)
                row_class = ' class="total-row"' if is_total_row else ""
                html_rows.append(f"<tr{row_class}>")
                for idx, cell in enumerate(r):
                    cell_str = html.escape(str(cell) if cell is not None else "")
                    # Жирным для указанных колонок или для итоговой строки
                    if idx in bold_column_indices or is_total_row:
                        html_rows.append(f"<td><strong>{cell_str}</strong></td>")
                    else:
                        html_rows.append(f"<td>{cell_str}</td>")
                html_rows.append("</tr>")
        else:
            html_rows.append("<tr>" + "<td></td>" * len(header) + "</tr>")
        
        title_html = f"<h3>{html.escape(title)}</h3>" if title else ""
        return f"""
        {title_html}
        <table class="deals-table">
            {''.join(html_rows)}
        </table>
        """
    
    # Добавляем deals таблицы на отдельные страницы
    deals_html_parts = []
    
    # --------- Auto Date (Deals) ----------
    if deals_auto_date is not None:
        deals_header = [
            "Deals", "Responsabil", "Sursa", "Numar auto", "Marca", "Model",
            "Data se dă\nin chirie", "Data retur\ndin chirie", "Zile", "Pret/zi\n(MDL)", "Pret/zi\n(euro)", "Servicii\nAditionale", "Total\nsuma"
        ]
        deal_rows = _build_deals_auto_date_rows(deals_auto_date, enum_map_sursa=enum_map_sursa)
        deals_count = len(deal_rows) if deal_rows else 0
        
        # Подсчитываем суммы в колонках "Servicii Aditionale" (индекс 11) и "Total suma" (индекс 12)
        servicii_total = 0.0
        total_sum = 0.0
        if deal_rows:
            import re
            for row in deal_rows:
                if len(row) > 11:
                    servicii_str = str(row[11]) if row[11] else ""
                    numbers = re.findall(r"[-+]?\d+(?:[.,]\d+)?", servicii_str)
                    if numbers:
                        try:
                            servicii_total += float(numbers[0].replace(",", "."))
                        except (ValueError, TypeError):
                            pass
                if len(row) > 12:
                    total_str = str(row[12]) if row[12] else ""
                    numbers = re.findall(r'\d+', total_str)
                    if numbers:
                        try:
                            total_sum += float(numbers[0])
                        except (ValueError, TypeError):
                            pass
        
        # Добавляем итоговую строку
        deal_rows_with_total = list(deal_rows) if deal_rows else []
        total_row_index = -1
        combined_total = servicii_total + total_sum
        if deal_rows_with_total and combined_total > 0:
            total_row = [""] * len(deals_header)
            if combined_total.is_integer():
                total_text = f"{int(combined_total)} MDL"
            else:
                total_text = f"{combined_total:.2f} MDL".rstrip("0").rstrip(".")
            total_row[12] = total_text
            deal_rows_with_total.append(total_row)
            total_row_index = len(deal_rows_with_total) - 1  # Индекс последней строки (итоговой)
        
        deals_html = f"""
        <div class="deals-page">
            <h2>Auto Date (Deals) — {html.escape(branch_name)} | Deals: {deals_count}</h2>
            {make_html_table_deals("", deals_header, deal_rows_with_total, bold_column_indices=[3, 10], total_row_index=total_row_index)}
        </div>
        """
        deals_html_parts.append(deals_html)
    
    # --------- Second Table (Auto Primite) ----------
    if deals_second_table is not None:
        second_header = [
            "Deals", "Responsabil", "Data - se da\nin chirie", "Numar auto",
            "Marca", "Model", "Viteaza GPS", "Amenda", "Comentariu\nAmenda",
            "Suma\nrambursare", "Comentariu\nrefuzului"
        ]
        second_rows = _build_deals_second_table_rows(deals_second_table)
        second_count = len(second_rows) if second_rows else 0
        
        second_html = f"""
        <div class="deals-page">
            <h2>Auto Primite — {html.escape(branch_name)} | Deals: {second_count}</h2>
            {make_html_table_deals("", second_header, second_rows, bold_column_indices=[3], total_row_index=-1)}
        </div>
        """
        deals_html_parts.append(second_html)
    
    # --------- Third Table (Prelungire) ----------
    if deals_third_table is not None:
        third_header = [
            "Deals", "Responsabil", "Numar auto", "Marca", "Model",
            "Data - se da\nin chirie", "Data - return\ndin chirie", "Zile", "pret/zi", "pret/zi\n(euro)", "Total\nprelungire"
        ]
        # third_rows уже построены выше, используем их
        third_count = len(third_rows) if third_rows else 0
        
        # Подсчитываем сумму в колонке "Total prelungire" (индекс 10, после добавления колонки евро)
        total_sum = 0
        if third_rows:
            import re
            for row in third_rows:
                if len(row) > 10:
                    total_str = str(row[10]) if row[10] else ""
                    # Извлекаем число из строки типа "600 MDL" или "4900 MDL"
                    numbers = re.findall(r'\d+', total_str)
                    if numbers:
                        try:
                            total_sum += int(numbers[0])
                        except (ValueError, TypeError):
                            pass
        
        # Добавляем итоговую строку
        third_rows_with_total = list(third_rows) if third_rows else []
        total_row_index = -1
        if third_rows_with_total and total_sum > 0:
            total_row = [""] * (len(third_header) - 1) + [f"{total_sum} MDL"]
            third_rows_with_total.append(total_row)
            total_row_index = len(third_rows_with_total) - 1  # Индекс последней строки (итоговой)
        
        third_html = f"""
        <div class="deals-page">
            <h2>Prelungire — {html.escape(branch_name)} | Deals: {third_count}</h2>
            {make_html_table_deals("", third_header, third_rows_with_total, bold_column_indices=[2, 9], total_row_index=total_row_index)}
        </div>
        """
        deals_html_parts.append(third_html)
    
    # Объединяем весь HTML
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{
                size: A3 landscape;
                margin: 10mm;
            }}
            body {{
                font-family: Arial, sans-serif;
                font-size: 8pt;
                margin: 0;
                padding: 0;
            }}
            h1 {{
                font-size: 14pt;
                margin-bottom: 5mm;
            }}
            h2 {{
                font-size: 12pt;
                margin-bottom: 2mm;  /* Уменьшен отступ снизу */
                margin-top: 2mm;  /* Небольшой отступ сверху */
                page-break-after: avoid;
            }}
            .grid-container {{
                display: flex;
                flex-wrap: wrap;
                gap: {'1mm' if is_centru_branch else '1.5mm'};  /* Для Centru минимальный gap */
                page-break-inside: avoid !important;  /* Не разрываем внутри контейнера */
                page-break-before: avoid !important;  /* Не разрываем перед контейнером */
                page-break-after: avoid !important;  /* Не разрываем после контейнера */
                orphans: 10;  /* Минимум 10 строк внизу */
                widows: 10;  /* Минимум 10 строк вверху */
            }}
            .grid-container .table-section {{
                flex: 1 1 calc(50% - 1mm);  /* Две колонки с учетом уменьшенного gap */
                min-width: 0;  /* Позволяет shrink */
                margin-bottom: 1mm;  /* Минимальный отступ снизу */
                page-break-inside: avoid;  /* Не разрываем таблицы */
            }}
            .table-section {{
                display: flex;
                flex-direction: column;
                page-break-inside: avoid !important;  /* НЕ разрываем заголовок и таблицу */
                page-break-after: avoid !important;  /* НЕ разрываем после секции */
                page-break-before: avoid !important;  /* НЕ разрываем перед секцией */
            }}
            .table-section h3 {{
                font-size: 8pt;  /* Компактный размер для всех филиалов */
                margin: 0 0 0.5mm 0;  /* Компактный отступ для всех филиалов */
                font-weight: bold;
                padding: 0.3mm 0.5mm;  /* Компактный padding для всех филиалов */
                background-color: #f5f5f5;
                border-bottom: 1pt solid #ddd;
                page-break-after: avoid;  /* Заголовок не должен быть отдельно от таблицы */
            }}
            .table-section table {{
                page-break-inside: avoid !important;  /* Таблица не должна разрываться */
                page-break-before: avoid !important;  /* Не разрываем перед таблицей */
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 6.5pt;  /* Компактный размер для всех филиалов */
                table-layout: fixed;
                margin-bottom: 0.3mm;  /* Компактный отступ для всех филиалов */
                page-break-inside: avoid !important;  /* Не разрываем таблицы */
                page-break-before: avoid !important;  /* Не разрываем перед таблицей */
                orphans: 10;  /* Минимум 10 строк внизу страницы */
                widows: 10;  /* Минимум 10 строк вверху страницы */
            }}
            .deals-table {{
                font-size: 10pt;  /* Оптимальный размер для deals таблиц */
                table-layout: fixed;
            }}
            th, td {{
                border: 0.5pt solid #ccc;
                padding: 1.5pt 1pt;  /* Компактный padding для всех филиалов */
                text-align: center;  /* Центрируем все ячейки */
                vertical-align: middle;
                overflow: hidden;
                text-overflow: ellipsis;
                word-wrap: break-word;  /* Перенос длинных слов */
                line-height: 1.1;  /* Компактная высота для всех филиалов */
            }}
            th {{
                background-color: #f0f0f0;
                font-weight: bold;
                font-size: 8pt;
            }}
            .deals-table th {{
                background-color: #f0f0f0;
                font-weight: bold;
                font-size: 14pt;  /* Уменьшен (было 22pt) */
                text-align: center;
            }}
            .total-row {{
                background-color: #e0e0e0;
                font-weight: bold;
            }}
            .total-row td {{
                font-size: 12pt;  /* Уменьшен (было 24pt) */
                font-weight: bold;
            }}
            /* Оптимизированные ширины колонок для STOCK AUTO таблиц */
            .table-section table th:nth-child(1),
            .table-section table td:nth-child(1) {{
                width: 4mm;  /* № - увеличен для читаемости */
            }}
            .table-section table th:nth-child(2),
            .table-section table td:nth-child(2) {{
                width: 18mm;  /* Nr Auto - увеличен */
            }}
            .table-section table th:nth-child(3),
            .table-section table td:nth-child(3) {{
                width: 15mm;  /* Marca - увеличен */
            }}
            .table-section table th:nth-child(4),
            .table-section table td:nth-child(4) {{
                width: 25mm;  /* Model - увеличен для длинных названий */
            }}
            .table-section table th:nth-child(5),
            .table-section table td:nth-child(5) {{
                width: 20mm;  /* Filiala - увеличен */
            }}
            .table-section table th:nth-child(6),
            .table-section table td:nth-child(6) {{
                width: 15mm;  /* Din data - увеличен */
            }}
            .table-section table th:nth-child(7),
            .table-section table td:nth-child(7) {{
                width: 8mm;  /* Zile - увеличен для читаемости */
            }}
            td strong {{
                font-weight: bold;
            }}
            /* Контейнер для всех таблиц deals - принудительный разрыв страницы */
            .deals-container {{
                page-break-before: always !important;  /* Принудительный разрыв перед всеми таблицами deals */
                page-break-inside: avoid !important;  /* Не разрываем внутри контейнера */
            }}
            .deals-page {{
                margin-top: 2mm;  /* Отступ сверху */
                margin-bottom: 2mm;  /* Уменьшен отступ снизу */
                page-break-inside: avoid !important;  /* Не разрываем внутри */
                page-break-after: avoid !important;  /* Не разрываем после таблицы */
            }}
            .deals-page:first-of-type {{
                margin-top: 0;  /* Убираем отступ сверху для первой таблицы на новой странице */
            }}
            /* Остальные таблицы deals следуют друг за другом на той же странице */
            .deals-page:not(:first-of-type) {{
                page-break-before: avoid !important;  /* Не разрываем перед остальными таблицами */
            }}
        </style>
    </head>
    <body>
        {html_content}
        {f'<div class="deals-container">{chr(10).join(deals_html_parts)}</div>' if deals_html_parts else ''}
    </body>
    </html>
    """
    
    # Конвертируем HTML в PDF с обработкой ошибок
    try:
        buf = BytesIO()
        html_doc = HTML(string=full_html)
        html_doc.write_pdf(buf)
        pdf_bytes = buf.getvalue()
        if not pdf_bytes or len(pdf_bytes) == 0:
            raise ValueError("Generated PDF is empty")
        return pdf_bytes
    except Exception as e:
        # НЕ используем ReportLab fallback - это гарантирует одинаковый формат PDF для всех филиалов
        # Если weasyprint не работает, пробрасываем ошибку дальше
        print(f"ERROR: _generate_pdf_stock_auto_split_weasyprint: WeasyPrint failed for '{branch_name}': {e}", file=sys.stderr, flush=True)
        import traceback
        print(f"ERROR: _generate_pdf_stock_auto_split_weasyprint: WeasyPrint traceback:\n{traceback.format_exc()}", file=sys.stderr, flush=True)
        # Пробрасываем ошибку дальше - НЕ используем ReportLab fallback
        raise


# ---------------- Telegram / Bitrix ----------------
def send_pdf_to_telegram(pdf_bytes: bytes, filename: str, caption: str) -> Dict[str, Any]:
    try:
        print(
            f"DEBUG: send_pdf_to_telegram: Starting - filename={filename}, pdf_size={len(pdf_bytes)} bytes",
            file=sys.stderr,
            flush=True,
        )

        if not TG_TOKEN:
            error_msg = "TG_TOKEN is empty (set env TG_TOKEN)"
            print(f"ERROR: send_pdf_to_telegram: {error_msg}", file=sys.stderr, flush=True)
            raise RuntimeError(error_msg)
        if not TG_CHAT_ID:
            error_msg = "TG_CHAT_ID is empty (set env TG_CHAT_ID)"
            print(f"ERROR: send_pdf_to_telegram: {error_msg}", file=sys.stderr, flush=True)
            raise RuntimeError(error_msg)

        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
        files = {"document": (filename, pdf_bytes, "application/pdf")}
        data = {"chat_id": TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"}

        print("DEBUG: send_pdf_to_telegram: Sending to Telegram API...", file=sys.stderr, flush=True)
        r = requests.post(url, files=files, data=data, timeout=60)
        print(
            f"DEBUG: send_pdf_to_telegram: Telegram API response status={r.status_code}",
            file=sys.stderr,
            flush=True,
        )

        r.raise_for_status()
        result = r.json()
        print(
            f"DEBUG: send_pdf_to_telegram: Successfully sent PDF '{filename}' to Telegram",
            file=sys.stderr,
            flush=True,
        )
        return result
    except requests.exceptions.RequestException as e:
        error_msg = f"Telegram API request failed: {e}"
        print(f"ERROR: send_pdf_to_telegram: {error_msg}", file=sys.stderr, flush=True)
        if hasattr(e, "response") and e.response is not None:
            print(
                f"ERROR: send_pdf_to_telegram: Response status={e.response.status_code}, body={e.response.text[:500]}",
                file=sys.stderr,
                flush=True,
            )
        raise
    except Exception as e:
        error_msg = f"Unexpected error in send_pdf_to_telegram: {e}"
        print(f"ERROR: send_pdf_to_telegram: {error_msg}", file=sys.stderr, flush=True)
        import traceback

        print(f"ERROR: send_pdf_to_telegram: Traceback:\n{traceback.format_exc()}", file=sys.stderr, flush=True)
        raise


def _bitrix_reports_webhook() -> str:
    """
    Возвращает webhook для отправки PDF в Bitrix (system.notifications@nobilauto.md).
    Можно переопределить через BITRIX_WEBHOOK_REPORTS, иначе используется BITRIX_WEBHOOK.
    """
    if BITRIX_WEBHOOK_REPORTS:
        return BITRIX_WEBHOOK_REPORTS
    return BITRIX_WEBHOOK


def _caption_html_to_bitrix_bb(caption: str) -> str:
    """Конвертирует caption из HTML (Telegram) в BB-коды Bitrix: [B]...[/B] для жирного."""
    if not caption:
        return ""
    s = caption.replace("</b>", "[/B]").replace("<b>", "[B]")
    s = s.replace("</i>", "[/I]").replace("<i>", "[I]")
    # убираем оставшиеся HTML-теги
    s = re.sub(r"<[^>]+>", "", s)
    return s.strip()


def send_pdf_to_bitrix(pdf_bytes: bytes, filename: str, caption: str) -> Dict[str, Any]:
    """
    Отправляет PDF в чат Bitrix по webhook:
    0) Разделитель (полоска с названием филиала), чтобы было видно, какой PDF к какому отчёту
    1) Получаем папку диска для диалога (im.disk.folder.get)
    2) Загружаем файл в эту папку (disk.folder.uploadfile)
    3) Коммитим файл в чат (im.disk.file.commit)
    4) Отправляем текст превью отдельным сообщением (im.message.add) — как в Telegram под файлом
    """
    try:
        webhook = _bitrix_reports_webhook()
        if not webhook:
            raise RuntimeError("BITRIX_WEBHOOK_REPORTS/BITRIX_WEBHOOK is empty")

        chat_id_raw = BITRIX_REPORT_CHAT_ID or "136188"
        dialog_id = chat_id_raw if str(chat_id_raw).startswith("chat") else f"chat{chat_id_raw}"

        print(
            f"DEBUG: send_pdf_to_bitrix: Starting - dialog_id={dialog_id}, filename={filename}, pdf_size={len(pdf_bytes)} bytes",
            file=sys.stderr,
            flush=True,
        )

        # 1) Получаем папку диска чата
        folder_url = f"{webhook.rstrip('/')}/im.disk.folder.get.json"
        folder_params = {"DIALOG_ID": dialog_id}
        folder_resp = requests.post(folder_url, json=folder_params, timeout=30)
        folder_resp.raise_for_status()
        folder_data = folder_resp.json()
        folder_result = folder_data.get("result") or {}
        folder_id = str(folder_result.get("ID") or folder_result.get("id") or "").strip()
        if not folder_id:
            raise RuntimeError(f"im.disk.folder.get returned no folder ID: {folder_data}")

        print(
            f"DEBUG: send_pdf_to_bitrix: Got folder_id={folder_id} for dialog_id={dialog_id}",
            file=sys.stderr,
            flush=True,
        )

        # 2) Запрашиваем uploadUrl (без файла), затем отправляем PDF по этому URL
        upload_method_url = f"{webhook.rstrip('/')}/disk.folder.uploadfile.json"
        # Шаг 2a: только id папки и имя файла — Bitrix вернёт uploadUrl
        init_payload = {"id": folder_id, "NAME": filename}
        init_resp = requests.post(upload_method_url, json=init_payload, timeout=30)
        init_resp.raise_for_status()
        init_json = init_resp.json()
        init_result = init_json.get("result") or {}
        upload_url_to_use = (init_result.get("uploadUrl") or init_result.get("upload_url") or "").strip()
        field_name = (init_result.get("field") or "file").strip() or "file"
        if not upload_url_to_use:
            raise RuntimeError(f"disk.folder.uploadfile did not return uploadUrl: {init_json}")

        print(
            f"DEBUG: send_pdf_to_bitrix: POSTing file to uploadUrl, field={field_name}",
            file=sys.stderr,
            flush=True,
        )
        # Шаг 2b: отправляем файл на uploadUrl (multipart)
        step2_resp = requests.post(
            upload_url_to_use,
            files={field_name: (filename, pdf_bytes, "application/pdf")},
            timeout=60,
        )
        step2_resp.raise_for_status()
        step2_content_type = (step2_resp.headers.get("content-type") or "").split(";")[0].strip().lower()
        step2_json = step2_resp.json() if "application/json" in step2_content_type else {}
        step2_result = step2_json.get("result")
        if isinstance(step2_result, dict):
            file_id = str(step2_result.get("ID") or step2_result.get("id") or "").strip()
        else:
            file_id = str(step2_json.get("ID") or step2_json.get("id") or "").strip() if isinstance(step2_json, dict) else ""
        if not file_id and isinstance(step2_json, dict):
            file_id = str(step2_json.get("result", {}).get("ID") or step2_json.get("result", {}).get("id") or "").strip()
        if not file_id:
            raise RuntimeError(f"Upload to uploadUrl returned no file ID: {step2_resp.text[:500]}")

        print(
            f"DEBUG: send_pdf_to_bitrix: Uploaded file_id={file_id} to folder_id={folder_id}",
            file=sys.stderr,
            flush=True,
        )

        # 3) Коммитим файл в чат (отображение в диалоге)
        commit_url = f"{webhook.rstrip('/')}/im.disk.file.commit.json"
        commit_params: Dict[str, Any] = {
            "CHAT_ID": int(chat_id_raw) if str(chat_id_raw).isdigit() else chat_id_raw,
            "FILE_ID": file_id,
        }
        if caption:
            commit_params["COMMENT"] = caption

        commit_resp = requests.post(commit_url, json=commit_params, timeout=30)
        commit_resp.raise_for_status()
        commit_json = commit_resp.json()

        print(
            f"DEBUG: send_pdf_to_bitrix: Successfully committed file_id={file_id} to CHAT_ID={chat_id_raw}",
            file=sys.stderr,
            flush=True,
        )

        # 4) Превью: текст под файлом, в конце — полоска после строки Încărcare filială (разделитель перед следующим PDF)
        if caption:
            msg_text = _caption_html_to_bitrix_bb(caption)
            if msg_text:
                msg_text = msg_text + "\n─────────────────────────"
                msg_url = f"{webhook.rstrip('/')}/im.message.add.json"
                msg_params = {"DIALOG_ID": dialog_id, "MESSAGE": msg_text}
                try:
                    msg_resp = requests.post(msg_url, json=msg_params, timeout=15)
                    msg_resp.raise_for_status()
                    print(
                        f"DEBUG: send_pdf_to_bitrix: Preview message sent (im.message.add)",
                        file=sys.stderr,
                        flush=True,
                    )
                except Exception as msg_err:
                    print(
                        f"WARNING: send_pdf_to_bitrix: im.message.add failed (preview text): {msg_err}",
                        file=sys.stderr,
                        flush=True,
                    )

        return {
            "ok": True,
            "folder_id": folder_id,
            "file_id": file_id,
            "commit": commit_json,
        }
    except requests.exceptions.RequestException as e:
        error_msg = f"Bitrix REST request failed: {e}"
        print(f"ERROR: send_pdf_to_bitrix: {error_msg}", file=sys.stderr, flush=True)
        if hasattr(e, "response") and e.response is not None:
            try:
                body = e.response.text[:500]
            except Exception:
                body = "<no body>"
            print(
                f"ERROR: send_pdf_to_bitrix: Response status={e.response.status_code}, body={body}",
                file=sys.stderr,
                flush=True,
            )
        raise
    except Exception as e:
        error_msg = f"Unexpected error in send_pdf_to_bitrix: {e}"
        print(f"ERROR: send_pdf_to_bitrix: {error_msg}", file=sys.stderr, flush=True)
        import traceback

        print(f"ERROR: send_pdf_to_bitrix: Traceback:\n{traceback.format_exc()}", file=sys.stderr, flush=True)
        raise


def _generate_test_pdf_bytes() -> bytes:
    """Минимальный одностраничный PDF для теста отправки в Bitrix."""
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4)
    styles = getSampleStyleSheet()
    story = [
        Paragraph("Test PDF for Bitrix", styles["Title"]),
        Spacer(1, 20),
        Paragraph(
            f"Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC. "
            "If you see this in the chat, Bitrix integration works.",
            styles["Normal"],
        ),
    ]
    doc.build(story)
    buf.seek(0)
    return buf.read()


@router.get("/reports/stock_auto/pdf/test-send-bitrix")
def test_send_pdf_to_bitrix() -> Dict[str, Any]:
    """
    Тестовый эндпоинт: генерирует один небольшой PDF и отправляет его в чат Bitrix
    (BITRIX_REPORT_CHAT_ID). Вызови в браузере: GET /api/data/reports/stock_auto/pdf/test-send-bitrix
    """
    try:
        pdf_bytes = _generate_test_pdf_bytes()
        filename = f"test_bitrix_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.pdf"
        caption = "Test PDF — проверка отправки отчётов в Bitrix"
        result = send_pdf_to_bitrix(pdf_bytes, filename=filename, caption=caption)
        return {
            "ok": True,
            "message": "Test PDF sent to Bitrix chat",
            "chat_id": BITRIX_REPORT_CHAT_ID,
            "filename": filename,
            "bitrix_result": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------- API ----------------
@router.get("/deals")
def get_deals(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    category_id: Optional[int] = Query(None),
) -> Dict[str, Any]:
    """
    Возвращает список сделок для админки с 7 полями:
    ID, Title, Stage, Begin Date, Close Date, Amount, Automobile, Assigned By
    """
    conn = pg_conn()
    try:
        sql = f"""
            SELECT
                id,
                title,
                raw,
                category_id,
                opportunity,
                assigned_by_id,
                source_id
            FROM {DEALS_TABLE}
            WHERE 1=1
        """
        params: List[Any] = []

        if category_id is not None:
            sql += """
                AND (
                    category_id = %s
                    OR raw->>'CATEGORY_ID' = %s
                    OR raw->>'category_id' = %s
                    OR raw->>'categoryId' = %s
                )
            """
            cid = str(int(category_id))
            params.extend([cid, cid, cid, cid])

        sql += " ORDER BY id DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        count_sql = f"SELECT COUNT(*) FROM {DEALS_TABLE} WHERE 1=1"
        count_params: List[Any] = []
        if category_id is not None:
            count_sql += """
                AND (
                    category_id = %s
                    OR raw->>'CATEGORY_ID' = %s
                    OR raw->>'category_id' = %s
                    OR raw->>'categoryId' = %s
                )
            """
            cid = str(int(category_id))
            count_params.extend([cid, cid, cid, cid])

        with conn.cursor() as cur:
            cur.execute(count_sql, count_params)
            total = int(cur.fetchone()[0])

        deals = []
        for row in rows:
            raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}

            stage_id = _row_get_any(row, raw, "STAGE_ID") or _row_get_any(row, raw, "stage_id") or ""
            begin_date = _row_get_any(row, raw, "BEGINDATE") or _row_get_any(row, raw, "begindate") or ""
            close_date = _row_get_any(row, raw, "CLOSEDATE") or _row_get_any(row, raw, "closedate") or ""
            automobile = _row_get_any(row, raw, DEALS_F_CARNO) or ""

            begin_dt = _to_dt(begin_date)
            close_dt = _to_dt(close_date)
            begin_date_str = _fmt_ddmmyyyy(begin_dt) if begin_dt else ""
            close_date_str = _fmt_ddmmyyyy(close_dt) if close_dt else ""

            assigned_by_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
            assigned_by = assigned_by_name if assigned_by_name else str(row.get("assigned_by_id") or "")

            deals.append(
                {
                    "id": row.get("id"),
                    "title": row.get("title") or "",
                    "stage": str(stage_id),
                    "begin_date": begin_date_str,
                    "close_date": close_date_str,
                    "amount": float(row.get("opportunity") or 0),
                    "automobile": automobile,
                    "assigned_by": assigned_by,
                }
            )

        return {
            "ok": True,
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": len(deals),
            "data": deals,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/reports/deals_second_table/debug")
def deals_second_table_debug(
    limit: int = Query(5000, ge=1, le=20000),
):
    """
    Debug endpoint для второй таблицы - показывает все сделки и результаты фильтрации
    """
    conn = None
    try:
        conn = pg_conn()
        
        # Загружаем enum для стадий
        enum_stage = pg_load_enum_map(conn, "deal", "STAGE_ID")
        
        # Получаем все сделки БЕЗ фильтров
        table = _safe_ident(DEALS_TABLE, "DEALS_TABLE")
        cid = str(int(DEALS_CATEGORY_ID))
        
        carno_f = _safe_ident(DEALS_F_CARNO, "DEALS_F_CARNO")
        brand_f = _safe_ident(DEALS_F_BRAND, "DEALS_F_BRAND")
        model_f = _safe_ident(DEALS_F_MODEL, "DEALS_F_MODEL")
        fromdt_f = _safe_ident(DEALS_F_FROMDT, "DEALS_F_FROMDT")
        gps_f = _safe_ident(DEALS_F_GPS, "DEALS_F_GPS")
        amenda_f = _safe_ident(DEALS_F_AMENDA, "DEALS_F_AMENDA")
        com_amenda_f = _safe_ident(DEALS_F_COM_AMENDA, "DEALS_F_COM_AMENDA")
        suma_ramb_f = _safe_ident(DEALS_F_SUMA_RAMBURSARE, "DEALS_F_SUMA_RAMBURSARE")
        com_refuz_f = _safe_ident(DEALS_F_COM_REFUZ, "DEALS_F_COM_REFUZ")

        sql = f"""
            SELECT
                id,
                id_2,
                title,
                raw,
                category_id,
                assigned_by_id,
                assigned_by_name,
                {carno_f} AS carno_val,
                {brand_f} AS brand_val,
                {model_f} AS model_val,
                {fromdt_f} AS fromdt_val,
                {gps_f} AS gps_val,
                {amenda_f} AS amenda_val,
                {com_amenda_f} AS com_amenda_val,
                {suma_ramb_f} AS suma_ramb_val,
                {com_refuz_f} AS com_refuz_val
            FROM {table}
            WHERE
                category_id = %s
                AND raw IS NOT NULL
            ORDER BY id DESC NULLS LAST
            LIMIT %s
        """

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, [cid, int(limit)])
            all_rows = cur.fetchall() or []

        now_utc = datetime.now(timezone.utc)
        today_local = _today_in_report_tz(now_utc)
        from datetime import timedelta
        test_date = today_local - timedelta(days=1)
        
        responsabil_names_lower = [name.lower().strip() for name in DEALS_FILTER_RESPONSABIL_NAMES]

        # Детальная информация по каждой сделке
        deals_detail = []
        status_matched = []
        responsabil_matched = []
        moved_time_matched = []
        all_filters_matched = []

        for r in all_rows:
            raw = r.get("raw") if isinstance(r.get("raw"), dict) else {}
            
            # Извлекаем данные
            deal_id = r.get("id") or r.get("id_2") or ""
            deal_title = r.get("title") or ""
            
            stage_id = _row_get_any(r, raw, "STAGE_ID") or _row_get_any(r, raw, "stage_id") or ""
            stage_name = _row_get_any(r, raw, "STAGE_NAME") or _row_get_any(r, raw, "stage_name") or ""
            
            # Если stage_name пустой, пытаемся получить из enum по stage_id
            if not stage_name and stage_id and enum_stage:
                stage_name = enum_stage.get(str(stage_id)) or enum_stage.get(stage_id) or ""
            
            assigned_name = r.get("assigned_by_name") or ""
            if not assigned_name:
                assigned_name = _raw_get(raw, "ASSIGNED_BY_NAME") or _raw_get(raw, "assigned_by_name") or ""
            
            # Используем moved_time для фильтра (как показано на картинке)
            moved_time = _get_moved_time(raw, {})
            moved_date_str = ""
            if moved_time:
                try:
                    moved_date = moved_time.astimezone(REPORT_TZINFO).date()
                    moved_date_str = str(moved_date)
                except Exception:
                    moved_date_str = str(moved_time.date()) if moved_time else ""

            # Проверяем фильтры
            status_match = False
            if stage_name and stage_name in DEALS_FILTER_STATUS_VALUES:
                status_match = True
            # Также проверяем по stage_id (может быть "C20:WON" = "Contract închis", "C20:LOSE" = "Сделка провалена")
            elif stage_id:
                stage_id_upper = str(stage_id).upper()
                if "WON" in stage_id_upper and "Contract închis" in DEALS_FILTER_STATUS_VALUES:
                    status_match = True
                elif "LOSE" in stage_id_upper and "Сделка провалена" in DEALS_FILTER_STATUS_VALUES:
                    status_match = True
            
            if status_match:
                status_matched.append(deal_id)

            responsabil_match = False
            if assigned_name:
                assigned_name_lower = assigned_name.lower().strip()
                for name in responsabil_names_lower:
                    if name in assigned_name_lower or assigned_name_lower in name:
                        responsabil_match = True
                        responsabil_matched.append(deal_id)
                        break
                    
            # Фильтр по moved_time (DATE(moved_time) = ВЧЕРАШНЯЯ ДАТА для теста)
            fromdt_match = False
            if moved_time:
                try:
                    moved_date = moved_time.astimezone(REPORT_TZINFO).date()
                    if moved_date == test_date:
                        fromdt_match = True
                        moved_time_matched.append(deal_id)
                except Exception:
                    pass

            all_match = status_match and responsabil_match and fromdt_match
            if all_match:
                all_filters_matched.append(deal_id)

            deals_detail.append({
                "id": deal_id,
                "title": deal_title,
                "stage_id": stage_id,
                "stage_name": stage_name,
                "stage_name_from_enum": enum_stage.get(str(stage_id)) or enum_stage.get(stage_id) or "" if stage_id and enum_stage else "",
                "assigned_by_id": r.get("assigned_by_id"),
                "assigned_by_name": assigned_name,
                "moved_time": moved_date_str,
                "filters": {
                    "status_match": status_match,
                    "status_expected": DEALS_FILTER_STATUS_VALUES,
                    "responsabil_match": responsabil_match,
                    "responsabil_expected": DEALS_FILTER_RESPONSABIL_NAMES,
                    "moved_time_match": fromdt_match,
                    "moved_time_expected": str(test_date),
                    "all_match": all_match,
                }
            })
        
        return {
            "ok": True,
            "filters_config": {
                "status_values": DEALS_FILTER_STATUS_VALUES,
                "responsabil_names": DEALS_FILTER_RESPONSABIL_NAMES,
                "test_date": str(test_date),
                "today_local": str(today_local),
                "report_tz": REPORT_TZ,
                "stage_enum_count": len(enum_stage),
                "stage_enum_sample": dict(list(enum_stage.items())[:10]) if enum_stage else {},
            },
            "summary": {
                "total_deals": len(all_rows),
                "status_matched": len(status_matched),
                "responsabil_matched": len(responsabil_matched),
                "moved_time_matched": len(moved_time_matched),
                "all_filters_matched": len(all_filters_matched),
            },
            "matched_ids": {
                "status": status_matched,
                "responsabil": responsabil_matched,
                "moved_time": moved_time_matched,
                "all": all_filters_matched,
            },
            "deals_detail": deals_detail[:100],  # Первые 100 для просмотра
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


@router.get("/reports/deals_auto_date/debug")
def deals_auto_date_debug(
    branch_id: str = Query("1668"),
    limit: int = Query(2000, ge=1, le=20000),
    only_today: bool = Query(True, description="Filter deals by today's date (Data se dă in chirie)"),
):
    conn = None
    try:
        conn = pg_conn()
        rows = pg_list_deals_auto_date(
            conn=conn,
            table=DEALS_TABLE,
            branch_field=DEALS_F_BRANCH,
            branch_id=branch_id,
            limit=limit,
            only_today=only_today,
        )
        sample = rows[:3]
        return {
            "table": DEALS_TABLE,
            "category_id": DEALS_CATEGORY_ID,
            "branch_field": DEALS_F_BRANCH,
            "branch_id": branch_id,
            "only_today": only_today,
            "report_tz": REPORT_TZ,
            "today": str(_today_in_report_tz()),
            "rows": len(rows),
            "sample": [
                {
                    "id": r.get("id"),
                    "category_id": r.get("category_id"),
                    "sursa_val": r.get("sursa_val"),
                    "carno_val": r.get("carno_val"),
                    "brand_val": r.get("brand_val"),
                    "model_val": r.get("model_val"),
                    "fromdt_val": str(r.get("fromdt_val")) if r.get("fromdt_val") else None,
                    "todt_val": str(r.get("todt_val")) if r.get("todt_val") else None,
                    "assigned_by_id": r.get("assigned_by_id"),
                    "assigned_by_name": r.get("assigned_by_name"),
                }
                for r in sample
            ],
        }
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@router.post("/deals/update_assigned_by_name")
def update_assigned_by_name():
    """
    Добавляет колонку assigned_by_name если её нет и заполняет её из raw JSON для существующих записей.
    """
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE b24_crm_deal
                ADD COLUMN IF NOT EXISTS assigned_by_name TEXT;
            """
            )
            conn.commit()

            cur.execute(
                """
                UPDATE b24_crm_deal
                SET assigned_by_name = COALESCE(
                    raw->>'ASSIGNED_BY_NAME',
                    raw->>'assigned_by_name'
                )
                WHERE assigned_by_name IS NULL
                  AND raw IS NOT NULL
                  AND (
                    raw->>'ASSIGNED_BY_NAME' IS NOT NULL
                    OR raw->>'assigned_by_name' IS NOT NULL
                  );
            """
            )
            updated_from_raw = cur.rowcount

            cur.execute(
                """
                SELECT COUNT(*)
                FROM b24_crm_deal
                WHERE assigned_by_id IS NOT NULL
                  AND assigned_by_name IS NULL;
            """
            )
            need_update_count = cur.fetchone()[0]

            conn.commit()

        return {
            "ok": True,
            "message": f"Updated {updated_from_raw} records with assigned_by_name from raw JSON. {need_update_count} records still need update (will be filled on next data sync).",
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@router.post("/reports/stock_auto/pdf/send_filtered")
def send_stock_auto_reports_filtered(
    branch_name: str = Query(..., description="Branch name (e.g., 'Centru')"),
    assigned_by_ids: str = Query(..., description="Comma-separated list of assigned_by_id (e.g., '3238,8136,8138')"),
    limit: int = Query(3000, ge=1, le=20000),
    deals_limit: int = Query(2000, ge=1, le=20000),
):
    """
    Генерирует PDF отчет с фильтрацией сделок:
    - СТРОГО только по ответственным из списка (IDs), но с запасным совпадением по имени:
      ID -> NAME (Bitrix user.get) и затем сравнение с assigned_by_name.
    - Auto Date включает только сделки текущего дня по полю DEALS_F_FROMDT,
      по REPORT_TZ (по умолчанию Europe/Chisinau). Можно отключить env DEALS_ONLY_TODAY=0.
    """
    branches = parse_branches(BRANCHES)
    branch_id_name_map = branches_id_to_name(branches)

    branch_filter_value = None
    for display_name, filter_value in branches:
        if str(display_name).strip().lower() == branch_name.strip().lower():
            branch_filter_value = filter_value
            break

    if branch_filter_value is None:
        raise HTTPException(status_code=400, detail=f"Branch '{branch_name}' not found in BRANCHES")

    fv = int(branch_filter_value) if str(branch_filter_value).isdigit() else str(branch_filter_value)

    table = stock_table_name(STOCK_ENTITY_TYPE_ID)
    entity_key = meta_entity_key(STOCK_ENTITY_TYPE_ID)

    cat_id: Optional[int] = None
    if STOCK_CATEGORY_ID:
        try:
            cat_id = int(STOCK_CATEGORY_ID)
        except Exception:
            raise HTTPException(status_code=400, detail="STOCK_CATEGORY_ID must be int")

    conn = pg_conn()
    try:
        enum_brand = pg_load_enum_map(conn, entity_key, STOCK_F_BRAND)
        enum_model = pg_load_enum_map(conn, entity_key, STOCK_F_MODEL)
        enum_sursa = pg_load_enum_map(conn, "deal", "SourceId")

        raw_items = pg_list_stock_raw(
            conn=conn,
            table=table,
            branch_field=STOCK_F_BRANCH,
            branch_value=fv,
            limit=limit,
            category_id=cat_id,
        )

        assigned_ids_list: List[int] = []
        try:
            assigned_ids_list = [int(aid.strip()) for aid in assigned_by_ids.split(",") if aid.strip().isdigit()]
            print(f"DEBUG: send_stock_auto_reports_filtered: Filtering by assigned_by_ids: {assigned_ids_list}", file=sys.stderr, flush=True)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid assigned_by_ids format: {e}")

        if not assigned_ids_list:
            raise HTTPException(status_code=400, detail="assigned_by_ids cannot be empty")

        allowed_names_norm = _build_allowed_assigned_names(assigned_ids_list)
        print(f"DEBUG: send_stock_auto_reports_filtered: Allowed assigned names (norm): {allowed_names_norm}", file=sys.stderr, flush=True)

        all_deals = pg_list_deals_auto_date(
            conn=conn,
            table=DEALS_TABLE,
            branch_field=DEALS_F_BRANCH,
            branch_id=str(fv),
            limit=deals_limit * 10,
            assigned_by_ids=assigned_ids_list,
            branch_name=branch_name,
            only_today=DEALS_ONLY_TODAY,
        )
        print(f"DEBUG: send_stock_auto_reports_filtered: Got {len(all_deals)} deals from DB (only_today={DEALS_ONLY_TODAY})", file=sys.stderr, flush=True)

        deals_for_pdf: List[Dict[str, Any]] = []
        unmatched_debug = []
        branch_is_comrat = branch_name.strip().lower() == "comrat"
        comrat_debug: List[Dict[str, Any]] = []
        for d in all_deals:
            if _deal_matches_assigned_filter(d, assigned_ids_list, allowed_names_norm):
                # Подставляем имя ответственного, если пусто
                if not d.get("assigned_by_name") and d.get("assigned_by_id"):
                    try:
                        aid_int = int(d.get("assigned_by_id"))
                        if aid_int in PDF_RESPONSIBLE_NAMES:
                            d["assigned_by_name"] = PDF_RESPONSIBLE_NAMES[aid_int]
                    except Exception:
                        pass
                deals_for_pdf.append(d)
            elif len(unmatched_debug) < 5:
                raw = d.get("raw") if isinstance(d.get("raw"), dict) else {}
                dt_from = _deal_dt_from_any(d)
                dt_from_str = ""
                if dt_from:
                    try:
                        dt_from_str = dt_from.astimezone(REPORT_TZINFO).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        dt_from_str = str(dt_from)
                dt_create = _to_dt(_raw_get(raw, "DATE_CREATE") or _raw_get(raw, "date_create"))
                dt_create_str = ""
                if dt_create:
                    try:
                        dt_create_str = dt_create.astimezone(REPORT_TZINFO).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        dt_create_str = str(dt_create)
                aid_val = d.get("assigned_by_id") or _raw_get(raw, "ASSIGNED_BY_ID") or _raw_get(raw, "assigned_by_id")
                unmatched_debug.append({
                    "id": d.get("id"),
                    "assigned_by_id": aid_val,
                    "assigned_by_name": d.get("assigned_by_name") or _raw_get(raw, "ASSIGNED_BY_NAME"),
                    "fromdt": dt_from_str,
                    "date_create": dt_create_str,
                })

            if branch_is_comrat and len(comrat_debug) < 10:
                raw = d.get("raw") if isinstance(d.get("raw"), dict) else {}
                dt_from = _deal_dt_from_any(d)
                dt_create = _to_dt(_raw_get(raw, "DATE_CREATE") or _raw_get(raw, "date_create"))
                comrat_debug.append({
                    "id": d.get("id"),
                    "match": _deal_matches_assigned_filter(d, assigned_ids_list, allowed_names_norm),
                    "assigned_by_id": d.get("assigned_by_id") or _raw_get(raw, "ASSIGNED_BY_ID") or _raw_get(raw, "assigned_by_id"),
                    "assigned_by_name": d.get("assigned_by_name") or _raw_get(raw, "ASSIGNED_BY_NAME"),
                    "fromdt": dt_from.isoformat() if dt_from else None,
                    "date_create": dt_create.isoformat() if dt_create else None,
                })

        print(
            f"DEBUG: send_stock_auto_reports_filtered: Filtered to {len(deals_for_pdf)} deals (ids={assigned_ids_list})",
            file=sys.stderr,
            flush=True,
        )
        if unmatched_debug:
            print(f"DEBUG: send_stock_auto_reports_filtered: First unmatched deals: {unmatched_debug}", file=sys.stderr, flush=True)
        if branch_is_comrat and comrat_debug:
            print(f"DEBUG: send_stock_auto_reports_filtered: Comrat detailed deals (first 10): {comrat_debug}", file=sys.stderr, flush=True)

        # Таблицы "Auto Date" и "Auto Primite"
        # Если DEALS_TABLES_BRANCHES пусто - для всех филиалов, иначе только для указанных
        branch_name_lower = branch_name.strip().lower()
        is_allowed_branch = True
        if DEALS_TABLES_BRANCHES:
            is_allowed_branch = any(b.strip().lower() == branch_name_lower for b in DEALS_TABLES_BRANCHES)
        
        # Для не-разрешенных филиалов не показываем таблицы сделок
        if not is_allowed_branch:
            deals_for_pdf = None
            print(
                f"DEBUG: send_stock_auto_reports_filtered: Skipping Auto Date table for '{branch_name}' (not in DEALS_TABLES_BRANCHES: {DEALS_TABLES_BRANCHES})",
                file=sys.stderr,
                flush=True,
            )

        # Получаем данные для второй таблицы
        deals_second_table = None
        if is_allowed_branch:
            try:
                deals_second_table = pg_list_deals_second_table(
                    conn=conn,
                    table=DEALS_TABLE,
                    limit=5000,
                    branch_name=branch_name,
                )
                print(
                    f"DEBUG: send_stock_auto_reports_filtered: Got {len(deals_second_table)} deals for second table for '{branch_name}'",
                    file=sys.stderr,
                    flush=True,
                )
            except Exception as e:
                print(f"WARNING: send_stock_auto_reports_filtered: Failed to fetch second table deals: {e}", file=sys.stderr, flush=True)
                deals_second_table = None
        else:
            print(
                f"DEBUG: send_stock_auto_reports_filtered: Skipping Auto Primite table for '{branch_name}' (not in DEALS_TABLES_BRANCHES: {DEALS_TABLES_BRANCHES})",
                file=sys.stderr,
                flush=True,
            )

        # Получаем данные для третьей таблицы (Prelungire)
        # Если DEALS_THIRD_TABLE_BRANCHES пусто - для всех филиалов, иначе только для указанных
        deals_third_table = []  # Инициализируем как пустой список, чтобы таблица всегда показывалась
        should_show_third_table = True
        if DEALS_THIRD_TABLE_BRANCHES:
            branch_name_lower_third = branch_name.strip().lower()
            should_show_third_table = any(b.strip().lower() == branch_name_lower_third for b in DEALS_THIRD_TABLE_BRANCHES)
        
        print(f"DEBUG: send_stock_auto_reports_filtered: should_show_third_table={should_show_third_table} for '{branch_name}', DEALS_THIRD_TABLE_BRANCHES={DEALS_THIRD_TABLE_BRANCHES}", file=sys.stderr, flush=True)
        
        if should_show_third_table:
            print(f"DEBUG: send_stock_auto_reports_filtered: Calling pg_list_deals_third_table for '{branch_name}'", file=sys.stderr, flush=True)
            try:
                # Получаем assigned_by_ids для филиала
                assigned_ids_list = None
                if branch_name in PDF_FILTERS_ASSIGNED_BY_IDS:
                    assigned_ids_list = PDF_FILTERS_ASSIGNED_BY_IDS[branch_name]
                else:
                    # Пробуем найти по lowercase
                    bn = branch_name.lower()
                    for k in PDF_FILTERS_ASSIGNED_BY_IDS.keys():
                        if k.lower() == bn:
                            assigned_ids_list = PDF_FILTERS_ASSIGNED_BY_IDS[k]
                            break
                
                print(f"DEBUG: send_stock_auto_reports_filtered: Calling pg_list_deals_third_table with assigned_ids_list={assigned_ids_list}", file=sys.stderr, flush=True)
                deals_third_table = pg_list_deals_third_table(
                    conn=conn,
                    table=DEALS_TABLE,
                    branch_field=DEALS_F_BRANCH,
                    branch_id=str(fv),
                    limit=5000,
                    branch_name=branch_name,
                    assigned_by_ids=assigned_ids_list,
                )
                print(
                    f"DEBUG: send_stock_auto_reports_filtered: Got {len(deals_third_table)} deals for third table for '{branch_name}'",
                    file=sys.stderr,
                    flush=True,
                )
            except Exception as e:
                print(f"WARNING: send_stock_auto_reports_filtered: Failed to fetch third table deals: {e}", file=sys.stderr, flush=True)
                deals_third_table = []  # Оставляем пустым списком, чтобы таблица показывалась
        else:
            print(
                f"DEBUG: send_stock_auto_reports_filtered: Skipping Prelungire table for '{branch_name}' (not in DEALS_THIRD_TABLE_BRANCHES: {DEALS_THIRD_TABLE_BRANCHES})",
                file=sys.stderr,
                flush=True,
            )

        print(f"DEBUG: send_stock_auto_reports_filtered: About to generate PDF for '{branch_name}' - raw_items={len(raw_items)}, deals_auto_date={len(deals_for_pdf) if deals_for_pdf else 0}, deals_second_table={len(deals_second_table) if deals_second_table else 0}, deals_third_table={len(deals_third_table) if deals_third_table else 0}", file=sys.stderr, flush=True)
        
        try:
            pdf = generate_pdf_stock_auto_split(
                raw_items,
                branch_name=branch_name,
                branch_id=str(fv),
                branch_field=STOCK_F_BRANCH,
                branch_id_name_map=branch_id_name_map,
                enum_map_brand=enum_brand,
                enum_map_model=enum_model,
                deals_auto_date=deals_for_pdf,
                enum_map_sursa=enum_sursa,
                deals_second_table=deals_second_table,
                deals_third_table=deals_third_table,
            )
            print(f"DEBUG: send_stock_auto_reports_filtered: PDF generated successfully for '{branch_name}' - size={len(pdf)} bytes", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"ERROR: send_stock_auto_reports_filtered: Failed to generate PDF for '{branch_name}': {e}", file=sys.stderr, flush=True)
            import traceback
            tb_str = traceback.format_exc()
            print(f"ERROR: send_stock_auto_reports_filtered: PDF generation traceback:\n{tb_str}", file=sys.stderr, flush=True)
            # Не выбрасываем исключение, чтобы увидеть полный traceback в логах
            raise

        safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(branch_name)).strip("_") or "branch"
        filename = f"stock_auto_{safe_name}_filtered_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        
        # Подсчитываем количество сделок из разных таблиц
        deals_auto_date_count = len(deals_for_pdf) if deals_for_pdf else 0
        deals_primite_count = len(deals_second_table) if deals_second_table else 0
        deals_prelungire_count = len(deals_third_table) if deals_third_table else 0
        
        # Тот же расчет, что и в отчете: используем calculate_responsible_totals
        caption_total = 0
        try:
            third_rows_for_caption = _build_deals_third_table_rows(deals_third_table) if deals_third_table else None
        except Exception:
            third_rows_for_caption = None
        try:
            responsible_totals_caption = calculate_responsible_totals_global(
                deals_for_pdf,
                deals_third_table,
                deals_second_table,
                third_table_rows=third_rows_for_caption,
            )
            caption_total = int(round(sum(total for _, total in responsible_totals_caption))) if responsible_totals_caption else 0
        except Exception as e:
            print(f"WARNING: send_stock_auto_reports_filtered: failed to calc caption_total: {e}", file=sys.stderr, flush=True)
        
        # Подсчитываем машины в прокате (CHIRIE) для расчета загрузки
        now = datetime.now(timezone.utc)
        chirie_count = 0
        for raw_obj in raw_items:
            fields = _extract_fields_from_raw(raw_obj)
            bucket, _ = stock_classify_default(fields, now)
            if bucket == "CHIRIE":
                chirie_count += 1
        
        # Рассчитываем загрузку филиала
        total_auto = len(raw_items)
        loading_percent = 0
        if total_auto > 0:
            loading_percent = round((chirie_count / total_auto) * 100, 1)
        
        # Форматируем дату в формате DD.MM.YYYY
        today_str = _today_in_report_tz().strftime("%d.%m.%Y")
        
        # Формируем caption в новом формате (румынский язык) с HTML форматированием
        caption = f"<b>{branch_name}</b> - {today_str}\n\n"
        caption += f"<b>Total venit astăzi - {caption_total} MDL</b>\n"
        caption += f"<b>Auto - {total_auto}</b>\n\n"
        caption += f"Mașini date - <b>{deals_auto_date_count}</b>\n"
        caption += f"Mașini primite - <b>{deals_primite_count}</b>\n"
        caption += f"Mașini prelungite - <b>{deals_prelungire_count}</b>\n\n"
        caption += f"Încărcare filială - <b>{loading_percent}%</b> (În chirie - <b>{chirie_count}</b> auto)"

        print(
            f"DEBUG: send_stock_auto_reports_filtered: About to send PDF to Telegram - filename={filename}, pdf_size={len(pdf)} bytes",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"DEBUG: send_stock_auto_reports_filtered: Caption length={len(caption)} chars",
            file=sys.stderr,
            flush=True,
        )

        try:
            send_pdf_to_telegram(pdf, filename=filename, caption=caption)
            print(
                f"DEBUG: send_stock_auto_reports_filtered: Successfully sent PDF '{filename}' to Telegram",
                file=sys.stderr,
                flush=True,
            )
        except Exception as telegram_error:
            error_msg = f"Failed to send PDF to Telegram: {telegram_error}"
            print(f"ERROR: send_stock_auto_reports_filtered: {error_msg}", file=sys.stderr, flush=True)
            import traceback

            print(
                f"ERROR: send_stock_auto_reports_filtered: Telegram send traceback:\n{traceback.format_exc()}",
                file=sys.stderr,
                flush=True,
            )
            raise

        # Дополнительно отправляем тот же PDF в Bitrix-чат
        try:
            send_pdf_to_bitrix(pdf, filename=filename, caption=caption)
            print(
                f"DEBUG: send_stock_auto_reports_filtered: Successfully sent PDF '{filename}' to Bitrix chat",
                file=sys.stderr,
                flush=True,
            )
        except Exception as bitrix_error:
            error_msg = f"Failed to send PDF to Bitrix: {bitrix_error}"
            print(f"ERROR: send_stock_auto_reports_filtered: {error_msg}", file=sys.stderr, flush=True)
        
        return {
            "ok": True,
            "message": "PDF sent to Telegram",
            "branch": branch_name,
            "stock_rows": len(raw_items),
            "deals_total_today": len(all_deals),
            "deals_filtered_today": len(deals_for_pdf),
            "assigned_by_ids": assigned_ids_list,
            "allowed_assigned_names_norm": allowed_names_norm,
            "only_today": DEALS_ONLY_TODAY,
            "report_tz": REPORT_TZ,
            "today": str(_today_in_report_tz()),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@router.get("/reports/stock_auto/pdf/send-now")
def trigger_daily_reports_now() -> Dict[str, Any]:
    """
    Тест крона: отправить 7 отчётов в Telegram и Bitrix **сейчас** (то же, что выполняется в 23:55).
    Открой в браузере: GET /api/data/reports/stock_auto/pdf/send-now — затем проверь чат Bitrix (ID 136188).
    """
    base = os.getenv("REPORT_CRON_BASE_URL", "http://127.0.0.1:7070").strip().rstrip("/")
    url = f"{base}/api/data/reports/stock_auto/pdf/send"
    try:
        r = requests.post(url, timeout=600)
        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        data = r.json() if "application/json" in ct else r.text[:1000]
        return {
            "ok": r.status_code == 200,
            "message": "Отправка запущена. Проверьте Telegram и чат Bitrix (ID 136188) — должно прийти 7 PDF.",
            "status_code": r.status_code,
            "response": data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reports/stock_auto/pdf/send")
def send_stock_auto_reports(
    limit: int = Query(3000, ge=1, le=20000),
    deals_limit: int = Query(2000, ge=1, le=20000),
    assigned_by_ids: Optional[str] = Query(None, description="Comma-separated list of assigned_by_id (e.g., '3238,8136,8138')"),
    force: bool = Query(False, description="If true, send even if already sent today (manual run)"),
    report_date: Optional[str] = Query(None, description="YYYY-MM-DD — отчёт за эту дату (например 2026-02-13, если уже 14-е и нужно отправить за вчера)"),
):
    """
    Основная отправка PDF по всем филиалам.
    Для Centru — строгий фильтр по ответственным (IDs + fallback по имени).
    Auto Date — только TODAY по REPORT_TZ (Europe/Chisinau). Можно отключить env DEALS_ONLY_TODAY=0.
    Не более одного запуска в сутки: при повторном вызове возвращает 200 и already_sent_today.
    ?force=1 — принудительная отправка. ?report_date=2026-02-13 — отчёт за 13-е (если уже 14-е).
    """
    mark_file: Optional[str] = None
    token = None
    if report_date:
        try:
            override_d = datetime.strptime(report_date.strip(), "%Y-%m-%d").date()
            token = _report_date_override.set(override_d)
            print(f"REPORT send: report_date override={report_date}", file=sys.stderr, flush=True)
        except ValueError:
            raise HTTPException(status_code=400, detail="report_date must be YYYY-MM-DD")

    def _clear_override():
        if token is not None:
            try:
                _report_date_override.reset(token)
            except Exception:
                pass

    if not force and not report_date:
        mark_dir = os.getenv("REPORT_CRON_MARK_DIR", "/tmp").strip() or "/tmp"
        now_local = datetime.now(REPORT_TZINFO)
        today_str = now_local.strftime("%Y-%m-%d")
        mark_file = os.path.join(mark_dir, f"report_cron_sent_{today_str}.mark")
        if os.path.exists(mark_file):
            print(
                f"REPORT send: skip (already sent today, mark={mark_file})",
                file=sys.stderr,
                flush=True,
            )
            _clear_override()
            return {
                "ok": True,
                "sent": 0,
                "reason": "already_sent_today",
                "mark_file": mark_file,
                "today": today_str,
            }
        try:
            fd = os.open(mark_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.close(fd)
        except OSError as e:
            if e.errno == errno.EEXIST:
                print(
                    f"REPORT send: skip (another process sent today, mark={mark_file})",
                    file=sys.stderr,
                    flush=True,
                )
                _clear_override()
                return {
                    "ok": True,
                    "sent": 0,
                    "reason": "already_sent_today",
                    "mark_file": mark_file,
                    "today": today_str,
                }
            raise
        print(f"REPORT send: mark created, sending 7 reports (mark={mark_file})", file=sys.stderr, flush=True)

    # Блокировка на время отправки: второй запрос (cron + что-то ещё) не пойдёт в отправку
    send_lock_file: Optional[str] = None
    mark_dir = os.getenv("REPORT_CRON_MARK_DIR", "/tmp").strip() or "/tmp"
    now_local = datetime.now(REPORT_TZINFO)
    today_str_lock = report_date.strip() if report_date else now_local.strftime("%Y-%m-%d")
    lock_file = os.path.join(mark_dir, f"report_cron_sending_{today_str_lock}.lock")
    try:
        fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
    except OSError as e:
        if e.errno == errno.EEXIST:
            print(
                f"REPORT send: skip (send already in progress or duplicate, lock={lock_file})",
                file=sys.stderr,
                flush=True,
            )
            _clear_override()
            return {
                "ok": False,
                "sent": 0,
                "reason": "send_already_in_progress",
                "detail": "Another request is already sending reports for this date.",
            }
        raise
    send_lock_file = lock_file
    print(f"REPORT send: lock acquired, sending (lock={lock_file})", file=sys.stderr, flush=True)

    if assigned_by_ids is None:
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'pdf_filters_config'
                    )
                """
                )
                table_exists = cur.fetchone()[0]

                if table_exists:
                    cur.execute(
                        """
                        SELECT assigned_by_ids
                        FROM pdf_filters_config
                        WHERE enabled = true
                        LIMIT 1
                    """
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        assigned_by_ids = row[0]
        except Exception as e:
            print(f"WARNING: Failed to load assigned_by_ids from config: {e}", file=sys.stderr, flush=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    print(
        f"DEBUG: send_stock_auto_reports: BRANCHES env value: '{BRANCHES}'",
        file=sys.stderr,
        flush=True,
    )
    
    # Проверяем, что BRANCHES не пустая
    if not BRANCHES or not BRANCHES.strip():
        error_msg = "BRANCHES environment variable is empty or not set!"
        print(
            f"ERROR: send_stock_auto_reports: {error_msg}",
            file=sys.stderr,
            flush=True,
        )
        raise HTTPException(
            status_code=500,
            detail=f"{error_msg} Please set BRANCHES in systemd service file. Example: BRANCHES='1668:Centru,1666:Buiucani,1670:Ungheni,1672:Comrat,1674:Cahul,1676:Mezon,1678:Balti'"
        )
    
    try:
        branches = parse_branches(BRANCHES)
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to parse BRANCHES: {e}"
        print(
            f"ERROR: send_stock_auto_reports: {error_msg}",
            file=sys.stderr,
            flush=True,
        )
        raise HTTPException(
            status_code=500,
            detail=f"{error_msg} BRANCHES value: '{BRANCHES}'"
        )
    
    branch_id_name_map = branches_id_to_name(branches)
    
    print(
        f"DEBUG: send_stock_auto_reports: Parsed {len(branches)} branches: {branches}",
        file=sys.stderr,
        flush=True,
    )
    
    # Проверяем, есть ли Centru в списке филиалов
    centru_found = False
    for name, val in branches:
        if "centru" in str(name).lower() or str(val) == "1668":
            centru_found = True
            print(
                f"DEBUG: send_stock_auto_reports: *** CENTRU FOUND IN BRANCHES *** name='{name}', value='{val}'",
                file=sys.stderr,
                flush=True,
            )
            break
    
    if not centru_found:
        print(
            f"WARNING: send_stock_auto_reports: *** CENTRU NOT FOUND IN BRANCHES! *** branches={branches}",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"WARNING: send_stock_auto_reports: BRANCHES env value was: '{BRANCHES}'",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"INFO: send_stock_auto_reports: *** AUTO-ADDING CENTRU TO BRANCHES LIST ***",
            file=sys.stderr,
            flush=True,
        )
        # Добавляем Centru в список (как для Ungheni)
        branches.append(("Centru", "1668"))
        branch_id_name_map["1668"] = "Centru"
        print(
            f"INFO: send_stock_auto_reports: Centru added to branches list. New branches: {branches}",
            file=sys.stderr,
            flush=True,
        )
    
    # Проверяем, есть ли Ungheni в списке
    ungheni_found = False
    for display_name, filter_value in branches:
        if "ungheni" in str(display_name).lower() or "1670" in str(filter_value):
            ungheni_found = True
            print(
                f"DEBUG: send_stock_auto_reports: Found Ungheni in branches: name='{display_name}', id='{filter_value}'",
                file=sys.stderr,
                flush=True,
            )
            break
    
    # ВАЖНО: Если Ungheni не найден в BRANCHES, добавляем его автоматически!
    # Это гарантирует, что PDF для Ungheni будет генерироваться всегда
    if not ungheni_found:
        print(
            f"WARNING: send_stock_auto_reports: *** UNGHENI NOT FOUND IN BRANCHES! ***",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"WARNING: send_stock_auto_reports: Parsed branches: {branches}",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"WARNING: send_stock_auto_reports: BRANCHES env value was: '{BRANCHES}'",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"INFO: send_stock_auto_reports: *** AUTO-ADDING UNGHENI TO BRANCHES LIST ***",
            file=sys.stderr,
            flush=True,
        )
        # Добавляем Ungheni в конец списка (будет 7-м, если других 6)
        branches.append(("Ungheni", "1670"))
        branch_id_name_map["1670"] = "Ungheni"
        print(
            f"INFO: send_stock_auto_reports: Ungheni added to branches list. New branches: {branches}",
            file=sys.stderr,
            flush=True,
        )

    table = stock_table_name(STOCK_ENTITY_TYPE_ID)
    entity_key = meta_entity_key(STOCK_ENTITY_TYPE_ID)

    cat_id: Optional[int] = None
    if STOCK_CATEGORY_ID:
        try:
            cat_id = int(STOCK_CATEGORY_ID)
        except Exception:
            raise HTTPException(status_code=400, detail="STOCK_CATEGORY_ID must be int")

    conn = pg_conn()
    try:
        enum_brand = pg_load_enum_map(conn, entity_key, STOCK_F_BRAND)
        enum_model = pg_load_enum_map(conn, entity_key, STOCK_F_MODEL)
        enum_sursa = pg_load_enum_map(conn, "deal", "SourceId")

        results = []
        errors: List[Dict[str, Any]] = []
        sent = 0

        # Сортируем филиалы для консистентного порядка (Ungheni должен быть 7-м)
        # Но не меняем порядок, если он уже правильный в BRANCHES
        branches_ordered = list(branches)
        
        # Логируем список филиалов перед обработкой
        print(f"DEBUG: send_stock_auto_reports: *** BRANCHES LIST BEFORE PROCESSING *** Total: {len(branches_ordered)} branches", file=sys.stderr, flush=True)
        for idx, (name, val) in enumerate(branches_ordered, start=1):
            is_centru_check = "centru" in str(name).lower() or str(val) == "1668"
            print(f"DEBUG: send_stock_auto_reports: *** BRANCH #{idx} *** name='{name}', value='{val}', is_centru={is_centru_check}", file=sys.stderr, flush=True)
        
        for idx, (display_name, filter_value) in enumerate(branches_ordered, start=1):
            fv = int(filter_value) if str(filter_value).isdigit() else str(filter_value)
            
            # Логируем начало обработки каждого филиала
            print(
                f"DEBUG: send_stock_auto_reports: ===== START Processing branch #{idx} '{display_name}' (filter_value={filter_value}, fv={fv}) =====",
                file=sys.stderr,
                flush=True,
            )
            
            # Особое внимание к Ungheni
            if "ungheni" in str(display_name).lower() or str(fv) == "1670":
                print(
                    f"DEBUG: send_stock_auto_reports: *** UNGHENI DETECTED *** Branch #{idx}, display_name='{display_name}', fv={fv}",
                    file=sys.stderr,
                    flush=True,
                )
                if idx != 7:
                    print(
                        f"WARNING: send_stock_auto_reports: *** UNGHENI IS NOT 7TH BRANCH! *** It's #{idx} in the list. Expected to be 7th.",
                        file=sys.stderr,
                        flush=True,
                    )

            # ВАЖНО: Для Centru и Ungheni гарантируем обработку ВСЕГДА
            is_centru = "centru" in str(display_name).lower() or str(fv) == "1668"
            is_ungheni = "ungheni" in str(display_name).lower() or str(fv) == "1670"
            
            if is_centru:
                print(
                    f"DEBUG: send_stock_auto_reports: *** CENTRU PROCESSING STARTED *** Will generate PDF even if no data!",
                    file=sys.stderr,
                    flush=True,
                )
                print(
                    f"DEBUG: send_stock_auto_reports: *** CENTRU BRANCH INFO *** display_name='{display_name}', filter_value={filter_value}, fv={fv}, fv_type={type(fv)}",
                    file=sys.stderr,
                    flush=True,
                )
            if is_ungheni:
                print(
                    f"DEBUG: send_stock_auto_reports: *** UNGHENI PROCESSING STARTED *** Will generate PDF even if no data!",
                    file=sys.stderr,
                    flush=True,
                )
            
            try:
                print(
                    f"DEBUG: send_stock_auto_reports: Calling pg_list_stock_raw for '{display_name}' (branch_value={fv}, branch_field={STOCK_F_BRANCH})",
                    file=sys.stderr,
                    flush=True,
                )
                
                # Особое внимание к Centru
                if is_centru:
                    print(
                        f"DEBUG: send_stock_auto_reports: *** CENTRU DATA LOADING START *** branch_value={fv} (type={type(fv)}), branch_field={STOCK_F_BRANCH}, table={table}",
                        file=sys.stderr,
                        flush=True,
                    )
                    print(
                        f"DEBUG: send_stock_auto_reports: *** CENTRU CALLING pg_list_stock_raw *** with fv={fv}, STOCK_F_BRANCH={STOCK_F_BRANCH}, table={table}",
                        file=sys.stderr,
                        flush=True,
                    )
                
                raw_items = pg_list_stock_raw(
                    conn=conn,
                    table=table,
                    branch_field=STOCK_F_BRANCH,
                    branch_value=fv,
                    limit=limit,
                    category_id=cat_id,
                )
                
                # Особое внимание к Centru после загрузки
                if is_centru:
                    raw_items_count = len(raw_items) if raw_items else 0
                    print(
                        f"DEBUG: send_stock_auto_reports: *** CENTRU DATA LOADED *** Got {raw_items_count} raw_items",
                        file=sys.stderr,
                        flush=True,
                    )
                    if not raw_items or len(raw_items) == 0:
                        print(
                            f"WARNING: send_stock_auto_reports: *** CENTRU HAS NO STOCK AUTO DATA! *** branch_value={fv} (type={type(fv)}), branch_field={STOCK_F_BRANCH}, table={table}",
                            file=sys.stderr,
                            flush=True,
                        )
                
                # Гарантируем, что raw_items - это список (не None)
                if raw_items is None:
                    raw_items = []
                    if is_centru:
                        print(
                            f"WARNING: send_stock_auto_reports: *** CENTRU raw_items was None, fixed to [] ***",
                            file=sys.stderr,
                            flush=True,
                        )
                    if is_ungheni:
                        print(
                            f"WARNING: send_stock_auto_reports: *** UNGHENI raw_items was None, fixed to [] ***",
                            file=sys.stderr,
                            flush=True,
                        )
                
                print(
                    f"DEBUG: send_stock_auto_reports: Got {len(raw_items)} raw_items for '{display_name}'",
                    file=sys.stderr,
                    flush=True,
                )
                
                # Особое внимание к Centru
                if is_centru:
                    print(
                        f"DEBUG: send_stock_auto_reports: *** CENTRU DATA *** Got {len(raw_items)} items for Centru",
                        file=sys.stderr,
                        flush=True,
                    )
                    if len(raw_items) == 0:
                        print(
                            f"WARNING: send_stock_auto_reports: *** CENTRU HAS NO DATA! *** branch_value={fv}, branch_field={STOCK_F_BRANCH}, table={table}",
                            file=sys.stderr,
                            flush=True,
                        )
                        print(
                            f"WARNING: send_stock_auto_reports: *** CENTRU DEBUG *** Check if branch_value={fv} matches data in database. STOCK_F_BRANCH={STOCK_F_BRANCH}",
                            file=sys.stderr,
                            flush=True,
                        )
                
                # Особое внимание к Ungheni
                if is_ungheni:
                    print(
                        f"DEBUG: send_stock_auto_reports: *** UNGHENI DATA *** Got {len(raw_items)} items for Ungheni",
                        file=sys.stderr,
                        flush=True,
                    )
                    if len(raw_items) == 0:
                        print(
                            f"WARNING: send_stock_auto_reports: *** UNGHENI HAS NO DATA! *** This might be normal if there are no cars for this branch.",
                            file=sys.stderr,
                            flush=True,
                        )

                deals_for_pdf: Optional[List[Dict[str, Any]]] = None

                # Применяем логику фильтрации для ВСЕХ филиалов, не только Centru
                assigned_ids_list: Optional[List[int]] = None

                if assigned_by_ids:
                    try:
                        assigned_ids_list = [int(aid.strip()) for aid in assigned_by_ids.split(",") if aid.strip().isdigit()]
                        print(
                            f"DEBUG: send_stock_auto_reports: Using assigned_by_ids from request: {assigned_ids_list}",
                            file=sys.stderr,
                            flush=True,
                        )
                    except Exception as e:
                        print(
                            f"WARNING: send_stock_auto_reports: Failed to parse assigned_by_ids '{assigned_by_ids}': {e}",
                            file=sys.stderr,
                            flush=True,
                        )
                        assigned_ids_list = None
                else:
                    branch_name = str(display_name).strip()
                    print(
                        f"DEBUG: send_stock_auto_reports: Looking for branch '{branch_name}' in PDF_FILTERS_ASSIGNED_BY_IDS (keys: {list(PDF_FILTERS_ASSIGNED_BY_IDS.keys())})",
                        file=sys.stderr,
                        flush=True,
                    )
                    
                    if branch_name in PDF_FILTERS_ASSIGNED_BY_IDS:
                        assigned_ids_list = PDF_FILTERS_ASSIGNED_BY_IDS[branch_name]
                        print(
                            f"DEBUG: send_stock_auto_reports: Found exact match for '{branch_name}' in PDF_FILTERS_ASSIGNED_BY_IDS",
                            file=sys.stderr,
                            flush=True,
                        )
                    else:
                        bn = branch_name.lower()
                        found_key = None
                        for k in PDF_FILTERS_ASSIGNED_BY_IDS.keys():
                            if k.lower() == bn:
                                found_key = k
                                print(
                                    f"DEBUG: send_stock_auto_reports: Found case-insensitive match: '{branch_name}' -> '{k}'",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                break
                        
                        if found_key:
                            assigned_ids_list = PDF_FILTERS_ASSIGNED_BY_IDS[found_key]
                        else:
                            assigned_ids_list = None
                            print(
                                f"WARNING: send_stock_auto_reports: Branch '{branch_name}' not found in PDF_FILTERS_ASSIGNED_BY_IDS. Available keys: {list(PDF_FILTERS_ASSIGNED_BY_IDS.keys())}",
                                file=sys.stderr,
                                flush=True,
                            )

                    print(
                        f"DEBUG: send_stock_auto_reports: Using PDF_FILTERS_ASSIGNED_BY_IDS for '{branch_name}': {assigned_ids_list}",
                        file=sys.stderr,
                        flush=True,
                    )

                try:
                    all_deals = pg_list_deals_auto_date(
                        conn=conn,
                        table=DEALS_TABLE,
                        branch_field=DEALS_F_BRANCH,
                        branch_id=str(fv),
                        limit=deals_limit * 10,
                        assigned_by_ids=assigned_ids_list,
                        branch_name=branch_name,
                        only_today=DEALS_ONLY_TODAY,
                    )
                    print(
                        f"DEBUG: send_stock_auto_reports: Got {len(all_deals)} deals from DB for '{display_name}' (only_today={DEALS_ONLY_TODAY})",
                        file=sys.stderr,
                        flush=True,
                    )

                    if assigned_ids_list:
                        allowed_names_norm = _build_allowed_assigned_names(assigned_ids_list)
                        print(
                            f"DEBUG: send_stock_auto_reports: Allowed assigned names (norm): {allowed_names_norm}",
                            file=sys.stderr,
                            flush=True,
                        )

                        # Специальный лог для Comrat: показываем первые 10 сделок и причину прохода/отсечения
                        branch_is_comrat = str(display_name).strip().lower() == "comrat"
                        comrat_debug: List[Dict[str, Any]] = []

                        deals_for_pdf = []
                        unmatched_debug = []
                        for d in all_deals:
                            if _deal_matches_assigned_filter(d, assigned_ids_list, allowed_names_norm):
                                if not d.get("assigned_by_name") and d.get("assigned_by_id"):
                                    try:
                                        aid_int = int(d.get("assigned_by_id"))
                                        if aid_int in PDF_RESPONSIBLE_NAMES:
                                            d["assigned_by_name"] = PDF_RESPONSIBLE_NAMES[aid_int]
                                    except Exception:
                                        pass
                                deals_for_pdf.append(d)
                            elif len(unmatched_debug) < 5:
                                raw = d.get("raw") if isinstance(d.get("raw"), dict) else {}
                                dt_from = _deal_dt_from_any(d)
                                dt_from_str = ""
                                if dt_from:
                                    try:
                                        dt_from_str = dt_from.astimezone(REPORT_TZINFO).strftime("%Y-%m-%d %H:%M")
                                    except Exception:
                                        dt_from_str = str(dt_from)
                                dt_create = _to_dt(_raw_get(raw, "DATE_CREATE") or _raw_get(raw, "date_create"))
                                dt_create_str = ""
                                if dt_create:
                                    try:
                                        dt_create_str = dt_create.astimezone(REPORT_TZINFO).strftime("%Y-%m-%d %H:%M")
                                    except Exception:
                                        dt_create_str = str(dt_create)
                                aid_val = d.get("assigned_by_id") or _raw_get(raw, "ASSIGNED_BY_ID") or _raw_get(raw, "assigned_by_id")
                                unmatched_debug.append({
                                    "id": d.get("id"),
                                    "assigned_by_id": aid_val,
                                    "assigned_by_name": d.get("assigned_by_name") or _raw_get(raw, "ASSIGNED_BY_NAME"),
                                    "fromdt": dt_from_str,
                                    "date_create": dt_create_str,
                                })

                            if branch_is_comrat and len(comrat_debug) < 10:
                                raw = d.get("raw") if isinstance(d.get("raw"), dict) else {}
                                dt_from = _deal_dt_from_any(d)
                                dt_create = _to_dt(_raw_get(raw, "DATE_CREATE") or _raw_get(raw, "date_create"))
                                comrat_debug.append({
                                    "id": d.get("id"),
                                    "match": _deal_matches_assigned_filter(d, assigned_ids_list, allowed_names_norm),
                                    "assigned_by_id": d.get("assigned_by_id") or _raw_get(raw, "ASSIGNED_BY_ID") or _raw_get(raw, "assigned_by_id"),
                                    "assigned_by_name": d.get("assigned_by_name") or _raw_get(raw, "ASSIGNED_BY_NAME"),
                                    "fromdt": dt_from.isoformat() if dt_from else None,
                                    "date_create": dt_create.isoformat() if dt_create else None,
                                })

                        print(
                            f"DEBUG: send_stock_auto_reports: Filtered to {len(deals_for_pdf)} deals for '{display_name}' (strict by ids/names)",
                            file=sys.stderr,
                            flush=True,
                        )
                        if unmatched_debug:
                            print(
                                f"DEBUG: send_stock_auto_reports: First unmatched deals for '{display_name}': {unmatched_debug}",
                                file=sys.stderr,
                                flush=True,
                            )
                        if branch_is_comrat and comrat_debug:
                            print(
                                f"DEBUG: send_stock_auto_reports: Comrat detailed deals (first 10): {comrat_debug}",
                                file=sys.stderr,
                                flush=True,
                            )
                    else:
                        # Если assigned_ids_list = None или пустой, показываем все сделки
                        # Это нормально для филиалов, у которых нет assigned_by_ids в PDF_FILTERS_ASSIGNED_BY_IDS
                        deals_for_pdf = all_deals
                        print(
                            f"DEBUG: send_stock_auto_reports: No assigned filter for '{display_name}' - showing all {len(deals_for_pdf)} deals (assigned_ids_list={assigned_ids_list})",
                            file=sys.stderr,
                            flush=True,
                        )

                except Exception as e:
                    errors.append({"branch": display_name, "where": "deals_fetch", "error": str(e)})
                    deals_for_pdf = []
                    print(f"ERROR: send_stock_auto_reports: Failed to fetch/filter deals: {e}", file=sys.stderr, flush=True)

                # Таблицы "Auto Date" и "Auto Primite"
                # Если DEALS_TABLES_BRANCHES пусто - для всех филиалов, иначе только для указанных
                branch_name_lower = str(display_name).strip().lower()
                is_allowed_branch = True
                if DEALS_TABLES_BRANCHES:
                    is_allowed_branch = any(b.strip().lower() == branch_name_lower for b in DEALS_TABLES_BRANCHES)
                    # Также проверяем по ID (для обратной совместимости)
                    if not is_allowed_branch:
                        is_allowed_branch = str(fv) == "1668"  # Centru ID
                
                # Для не-разрешенных филиалов не показываем таблицы сделок
                if not is_allowed_branch:
                    deals_for_pdf = []  # Пустой список вместо None, чтобы таблица все равно генерировалась
                    print(
                        f"DEBUG: send_stock_auto_reports: Skipping Auto Date table for '{display_name}' (not in DEALS_TABLES_BRANCHES: {DEALS_TABLES_BRANCHES})",
                        file=sys.stderr,
                        flush=True,
                    )
                
                # Убеждаемся, что deals_for_pdf не None (для генерации таблиц)
                if deals_for_pdf is None:
                    deals_for_pdf = []

                # Получаем данные для второй таблицы
                deals_second_table = None
                if is_allowed_branch:
                    try:
                        deals_second_table = pg_list_deals_second_table(
                            conn=conn,
                            table=DEALS_TABLE,
                            limit=5000,
                            branch_name=display_name,
                        )
                        print(
                            f"DEBUG: send_stock_auto_reports: Got {len(deals_second_table)} deals for second table for '{display_name}' (allowed branch)",
                            file=sys.stderr,
                            flush=True,
                        )
                    except Exception as e:
                        print(f"WARNING: send_stock_auto_reports: Failed to fetch second table deals: {e}", file=sys.stderr, flush=True)
                        deals_second_table = None
                else:
                    print(
                        f"DEBUG: send_stock_auto_reports: Skipping Auto Primite table for '{display_name}' (not in DEALS_TABLES_BRANCHES: {DEALS_TABLES_BRANCHES})",
                        file=sys.stderr,
                        flush=True,
                    )

                # Получаем данные для третьей таблицы (Prelungire)
                # Если DEALS_THIRD_TABLE_BRANCHES пусто - для всех филиалов, иначе только для указанных
                deals_third_table = []  # Инициализируем как пустой список, чтобы таблица всегда показывалась
                should_show_third_table = True
                if DEALS_THIRD_TABLE_BRANCHES:
                    branch_name_lower_third = str(display_name).strip().lower()
                    should_show_third_table = any(b.strip().lower() == branch_name_lower_third for b in DEALS_THIRD_TABLE_BRANCHES)
                
                print(f"DEBUG: send_stock_auto_reports: should_show_third_table={should_show_third_table} for '{display_name}', DEALS_THIRD_TABLE_BRANCHES={DEALS_THIRD_TABLE_BRANCHES}", file=sys.stderr, flush=True)
                
                if should_show_third_table:
                    print(f"DEBUG: send_stock_auto_reports: Calling pg_list_deals_third_table for '{display_name}'", file=sys.stderr, flush=True)
                    try:
                        # Получаем assigned_by_ids для филиала
                        assigned_ids_list = None
                        if branch_name in PDF_FILTERS_ASSIGNED_BY_IDS:
                            assigned_ids_list = PDF_FILTERS_ASSIGNED_BY_IDS[branch_name]
                        else:
                            # Пробуем найти по lowercase
                            bn = branch_name.lower()
                            for k in PDF_FILTERS_ASSIGNED_BY_IDS.keys():
                                if k.lower() == bn:
                                    assigned_ids_list = PDF_FILTERS_ASSIGNED_BY_IDS[k]
                                    break
                        
                        print(f"DEBUG: send_stock_auto_reports: Calling pg_list_deals_third_table with assigned_ids_list={assigned_ids_list}", file=sys.stderr, flush=True)
                        deals_third_table = pg_list_deals_third_table(
                            conn=conn,
                            table=DEALS_TABLE,
                            branch_field=DEALS_F_BRANCH,
                            branch_id=str(fv),
                            limit=5000,
                            branch_name=branch_name,
                            assigned_by_ids=assigned_ids_list,
                        )
                        print(
                            f"DEBUG: send_stock_auto_reports: Got {len(deals_third_table)} deals for third table for '{display_name}'",
                            file=sys.stderr,
                            flush=True,
                        )
                    except Exception as e:
                        print(f"WARNING: send_stock_auto_reports: Failed to fetch third table deals: {e}", file=sys.stderr, flush=True)
                        deals_third_table = []  # Оставляем пустым списком, чтобы таблица показывалась
                else:
                    print(
                        f"DEBUG: send_stock_auto_reports: Skipping Prelungire table for '{display_name}' (not in DEALS_THIRD_TABLE_BRANCHES: {DEALS_THIRD_TABLE_BRANCHES})",
                        file=sys.stderr,
                        flush=True,
                    )

                # Финальная проверка: убеждаемся, что deals_for_pdf не None перед генерацией PDF
                # Это гарантирует, что таблицы будут генерироваться даже если данных нет
                if deals_for_pdf is None:
                    deals_for_pdf = []
                    print(
                        f"DEBUG: send_stock_auto_reports: Fixed deals_for_pdf=None to [] for '{display_name}' before PDF generation",
                        file=sys.stderr,
                        flush=True,
                    )
                
                print(
                    f"DEBUG: send_stock_auto_reports: Generating PDF for '{display_name}' (raw_items={len(raw_items)}, deals_auto_date={len(deals_for_pdf) if deals_for_pdf else 0}, deals_second={len(deals_second_table) if deals_second_table else 0}, deals_third={len(deals_third_table) if deals_third_table else 0})",
                    file=sys.stderr,
                    flush=True,
                )
                
                # Особое внимание к Ungheni
                if "ungheni" in str(display_name).lower() or str(fv) == "1670":
                    print(
                        f"DEBUG: send_stock_auto_reports: *** UNGHENI PDF GENERATION START *** raw_items={len(raw_items)}, deals={len(deals_for_pdf) if deals_for_pdf else 0}",
                        file=sys.stderr,
                        flush=True,
                    )
                    print(
                        f"DEBUG: send_stock_auto_reports: *** UNGHENI FORCING PDF GENERATION *** Even if no data, PDF will be generated!",
                        file=sys.stderr,
                        flush=True,
                    )
                
                # ВАЖНО: PDF должен генерироваться ВСЕГДА, даже если данных нет
                # Это гарантирует, что каждый филиал получит свой PDF
                try:
                    pdf = generate_pdf_stock_auto_split(
                        raw_items,  # Может быть пустым списком - это нормально
                        branch_name=display_name,
                        branch_id=str(fv),
                        branch_field=STOCK_F_BRANCH,
                        branch_id_name_map=branch_id_name_map,
                        enum_map_brand=enum_brand,
                        enum_map_model=enum_model,
                        deals_auto_date=deals_for_pdf if deals_for_pdf is not None else [],  # Гарантируем список
                        enum_map_sursa=enum_sursa,
                        deals_second_table=deals_second_table if deals_second_table is not None else [],  # Гарантируем список
                        deals_third_table=deals_third_table if deals_third_table is not None else [],  # Гарантируем список
                    )
                    
                    print(
                        f"DEBUG: send_stock_auto_reports: PDF generated successfully for '{display_name}' (size={len(pdf)} bytes)",
                        file=sys.stderr,
                        flush=True,
                    )
                    
                    # Особое внимание к Ungheni
                    if "ungheni" in str(display_name).lower() or str(fv) == "1670":
                        print(
                            f"DEBUG: send_stock_auto_reports: *** UNGHENI PDF GENERATED *** Size: {len(pdf)} bytes",
                            file=sys.stderr,
                            flush=True,
                        )
                except Exception as pdf_error:
                    error_msg = f"PDF generation failed: {str(pdf_error)}"
                    print(
                        f"ERROR: send_stock_auto_reports: {error_msg} for '{display_name}'",
                        file=sys.stderr,
                        flush=True,
                    )
                    import traceback
                    print(
                        f"ERROR: send_stock_auto_reports: PDF generation traceback:\n{traceback.format_exc()}",
                        file=sys.stderr,
                        flush=True,
                    )
                    
                    # Определяем, является ли это Centru или Ungheni
                    is_centru = "centru" in str(display_name).lower() or str(fv) == "1668"
                    is_ungheni = "ungheni" in str(display_name).lower() or str(fv) == "1670"
                    
                    # Особое внимание к ошибкам Centru и Ungheni
                    if is_centru:
                        print(
                            f"ERROR: send_stock_auto_reports: *** CENTRU PDF GENERATION FAILED! *** {error_msg}",
                            file=sys.stderr,
                            flush=True,
                        )
                    if is_ungheni:
                        print(
                            f"ERROR: send_stock_auto_reports: *** UNGHENI PDF GENERATION FAILED! *** {error_msg}",
                            file=sys.stderr,
                            flush=True,
                        )
                    
                    # Для Centru и Ungheni пытаемся сгенерировать PDF с реальными данными даже при ошибке
                    if is_centru or is_ungheni:
                        try:
                            recovery_name = "CENTRU" if is_centru else "UNGHENI"
                            # Используем реальные данные, которые были загружены до ошибки
                            recovery_raw_items = raw_items if raw_items is not None else []
                            recovery_deals_auto = deals_for_pdf if deals_for_pdf is not None else []
                            recovery_deals_second = deals_second_table if deals_second_table is not None else []
                            recovery_deals_third = deals_third_table if deals_third_table is not None else []
                            
                            print(
                                f"DEBUG: send_stock_auto_reports: *** {recovery_name} RECOVERY ATTEMPT *** Trying to generate PDF with data (raw_items={len(recovery_raw_items)}, deals_auto={len(recovery_deals_auto)}, deals_second={len(recovery_deals_second)}, deals_third={len(recovery_deals_third)}) despite error",
                                file=sys.stderr,
                                flush=True,
                            )
                            # Пытаемся использовать только WeasyPrint для Centru (чтобы избежать ошибок ReportLab)
                            if is_centru and WEASYPRINT_AVAILABLE:
                                print(
                                    f"DEBUG: send_stock_auto_reports: *** CENTRU RECOVERY *** Using WeasyPrint only (no ReportLab fallback) with REAL DATA",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                try:
                                    pdf = _generate_pdf_stock_auto_split_weasyprint(
                                        recovery_raw_items,  # РЕАЛЬНЫЕ данные
                                        branch_name=display_name,
                                        branch_id=str(fv),
                                        branch_field=STOCK_F_BRANCH,
                                        branch_id_name_map=branch_id_name_map,
                                        enum_map_brand=enum_brand,
                                        enum_map_model=enum_model,
                                        deals_auto_date=recovery_deals_auto,  # РЕАЛЬНЫЕ данные
                                        enum_map_sursa=enum_sursa,
                                        deals_second_table=recovery_deals_second,  # РЕАЛЬНЫЕ данные
                                        deals_third_table=recovery_deals_third,  # РЕАЛЬНЫЕ данные
                                    )
                                except Exception as weasy_error:
                                    print(
                                        f"ERROR: send_stock_auto_reports: *** CENTRU RECOVERY WeasyPrint failed: {weasy_error}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    import traceback
                                    print(
                                        f"ERROR: send_stock_auto_reports: *** CENTRU RECOVERY WeasyPrint traceback:\n{traceback.format_exc()}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    # Если WeasyPrint не работает, пробуем еще раз с теми же данными (может быть временная проблема)
                                    print(
                                        f"ERROR: send_stock_auto_reports: *** CENTRU RECOVERY *** WeasyPrint failed, cannot use ReportLab (only WeasyPrint allowed)",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    # Пробрасываем ошибку дальше - не используем ReportLab
                                    raise weasy_error
                            else:
                                # Для Ungheni или если WeasyPrint недоступен, пробуем обычный метод
                                print(
                                    f"DEBUG: send_stock_auto_reports: *** {recovery_name} RECOVERY *** Trying generate_pdf_stock_auto_split",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                try:
                                    pdf = generate_pdf_stock_auto_split(
                                        recovery_raw_items,  # РЕАЛЬНЫЕ данные
                                        branch_name=display_name,
                                        branch_id=str(fv),
                                        branch_field=STOCK_F_BRANCH,
                                        branch_id_name_map=branch_id_name_map,
                                        enum_map_brand=enum_brand,
                                        enum_map_model=enum_model,
                                        deals_auto_date=recovery_deals_auto,  # РЕАЛЬНЫЕ данные
                                        enum_map_sursa=enum_sursa,
                                        deals_second_table=recovery_deals_second,  # РЕАЛЬНЫЕ данные
                                        deals_third_table=recovery_deals_third,  # РЕАЛЬНЫЕ данные
                                    )
                                except Exception as gen_error:
                                    # Если обычный метод тоже падает, пробрасываем ошибку (не используем ReportLab)
                                    print(
                                        f"ERROR: send_stock_auto_reports: *** {recovery_name} RECOVERY *** generate_pdf_stock_auto_split failed: {gen_error}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    print(
                                        f"ERROR: send_stock_auto_reports: *** {recovery_name} RECOVERY *** Cannot use ReportLab (only WeasyPrint allowed)",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    # Пробрасываем ошибку дальше
                                    raise gen_error
                            safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(display_name)).strip("_") or "branch"
                            filename = f"stock_auto_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                            
                            # Используем реальные данные для caption (только сделки с номером авто)
                            def filter_deals_with_car_no_recovery(deals_list):
                                """Фильтрует сделки, оставляя только те, у которых есть номер авто"""
                                if not deals_list:
                                    return []
                                filtered = []
                                for deal in deals_list:
                                    raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
                                    car_no = deal.get("carno_val") or _row_get_any(deal, raw, DEALS_F_CARNO) or ""
                                    if car_no and str(car_no).strip():
                                        filtered.append(deal)
                                return filtered
                            
                            recovery_deals_auto_filtered = filter_deals_with_car_no_recovery(recovery_deals_auto)
                            recovery_deals_second_filtered = filter_deals_with_car_no_recovery(recovery_deals_second)
                            recovery_deals_third_filtered = filter_deals_with_car_no_recovery(recovery_deals_third)
                            
                            total_auto = len(recovery_raw_items)
                            total_venit = sum(deal.get("dohod", 0) or 0 for deal in recovery_deals_auto_filtered)
                            masini_date = len(recovery_deals_auto_filtered)
                            masini_primite = len(recovery_deals_second_filtered)
                            masini_prelungite = len(recovery_deals_third_filtered)
                            in_chirie = sum(1 for item in recovery_raw_items if item.get("stage_name", "").lower() in ["în chirie", "in chirie", "în chirii"])
                            incarcare = round((in_chirie / total_auto * 100) if total_auto > 0 else 0, 1)
                            
                            today_str = _today_in_report_tz().strftime("%d.%m.%Y")
                            caption = f"<b>{display_name}</b> - {today_str}\n\n<b>Total venit astăzi - {total_venit} MDL</b>\n<b>Auto - {total_auto}</b>\n\nMașini date - <b>{masini_date}</b>\nMașini primite - <b>{masini_primite}</b>\nMașini prelungite - <b>{masini_prelungite}</b>\n\nÎncărcare filială - <b>{incarcare}%</b> (În chirie - <b>{in_chirie}</b> auto)"
                            
                            send_pdf_to_telegram(pdf, filename=filename, caption=caption)
                            sent += 1
                            results.append(
                                {
                                    "branch": display_name,
                                    "rows_total": total_auto,  # РЕАЛЬНОЕ количество
                                    "filter": str(fv),
                                }
                            )
                            # Дополнительно отправляем тот же PDF в Bitrix-чат
                            try:
                                send_pdf_to_bitrix(pdf, filename=filename, caption=caption)
                                print(
                                    f"DEBUG: send_stock_auto_reports: Successfully sent PDF for '{display_name}' to Bitrix (filtered Centru/Ungheni)",
                                    file=sys.stderr,
                                    flush=True,
                                )
                            except Exception as bitrix_error:
                                error_msg = f"Failed to send filtered PDF to Bitrix for '{display_name}': {bitrix_error}"
                                print(f"ERROR: send_stock_auto_reports: {error_msg}", file=sys.stderr, flush=True)
                            print(
                                f"DEBUG: send_stock_auto_reports: *** {recovery_name} RECOVERY SUCCESS *** PDF with {total_auto} items sent to Telegram",
                                file=sys.stderr,
                                flush=True,
                            )
                            continue  # Переходим к следующему филиалу
                        except Exception as recovery_error:
                            print(
                                f"ERROR: send_stock_auto_reports: *** {recovery_name} RECOVERY FAILED *** {str(recovery_error)}",
                                file=sys.stderr,
                                flush=True,
                            )
                            import traceback
                            print(
                                f"ERROR: send_stock_auto_reports: *** {recovery_name} RECOVERY TRACEBACK ***\n{traceback.format_exc()}",
                                file=sys.stderr,
                                flush=True,
                            )
                    
                    raise  # Пробрасываем ошибку дальше, если recovery не сработал

                safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(display_name)).strip("_") or "branch"
                filename = f"stock_auto_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                
                # Подсчитываем количество сделок из разных таблиц (только с номером авто)
                # Фильтруем пустые сделки (без номера авто) для правильного подсчета
                def filter_deals_with_car_no(deals_list):
                    """Фильтрует сделки, оставляя только те, у которых есть номер авто"""
                    if not deals_list:
                        return []
                    filtered = []
                    for deal in deals_list:
                        raw = deal.get("raw") if isinstance(deal.get("raw"), dict) else {}
                        car_no = deal.get("carno_val") or _row_get_any(deal, raw, DEALS_F_CARNO) or ""
                        if car_no and str(car_no).strip():
                            filtered.append(deal)
                    return filtered
                
                # Фильтруем пустые сделки перед подсчетом
                deals_auto_date_filtered = filter_deals_with_car_no(deals_for_pdf) if deals_for_pdf else []
                deals_second_filtered = filter_deals_with_car_no(deals_second_table) if deals_second_table else []
                deals_third_filtered = filter_deals_with_car_no(deals_third_table) if deals_third_table else []
                
                deals_auto_date_count = len(deals_auto_date_filtered)
                deals_primite_count = len(deals_second_filtered)
                deals_prelungire_count = len(deals_third_filtered)
                
                # Подсчитываем Total venit astăzi (просто суммируем все суммы) - безопасный расчет
                total_venit = 0.0
                
                # Тот же расчет, что и в отчете: используем calculate_responsible_totals
                caption_total = 0
                try:
                    third_rows_for_caption = _build_deals_third_table_rows(deals_third_table) if deals_third_table else None
                except Exception:
                    third_rows_for_caption = None
                try:
                    responsible_totals_caption = calculate_responsible_totals_global(
                        deals_for_pdf,
                        deals_third_table,
                        deals_second_table,
                        third_table_rows=third_rows_for_caption,
                    )
                    caption_total = int(round(sum(total for _, total in responsible_totals_caption))) if responsible_totals_caption else 0
                except Exception as e:
                    print(f"WARNING: send_stock_auto_reports: failed to calc caption_total: {e}", file=sys.stderr, flush=True)
                
                # Суммируем все продления из deals_third_table
                if deals_third_table:
                    try:
                        for deal in deals_third_table:
                            if not deal:
                                continue
                            try:
                                for pret_key in ["prel1_pret_val", "prel2_pret_val", "prel3_pret_val", "prel4_pret_val", "prel5_pret_val"]:
                                    try:
                                        pret_val = deal.get(pret_key) if deal else None
                                        if pret_val:
                                            if isinstance(pret_val, str):
                                                cleaned = re.sub(r'[^0-9.,-]', '', str(pret_val))
                                                cleaned = cleaned.replace(',', '.')
                                                val = float(cleaned) if cleaned else 0
                                            else:
                                                val = float(pret_val)
                                            if val > 0:
                                                total_venit += val
                                    except:
                                        pass
                            except:
                                pass
                    except:
                        pass
                
                # Суммируем аменду и вычитаем рамбурсаре из deals_second_table
                if deals_second_table:
                    try:
                        for deal in deals_second_table:
                            if not deal:
                                continue
                            try:
                                # Аменда
                                try:
                                    amenda_val = deal.get("amenda_val") if deal else None
                                    if amenda_val:
                                        if isinstance(amenda_val, str):
                                            cleaned = re.sub(r'[^0-9.,-]', '', amenda_val)
                                            cleaned = cleaned.replace(',', '.')
                                            val = float(cleaned) if cleaned else 0
                                        else:
                                            val = float(amenda_val)
                                        if val > 0:
                                            total_venit += val
                                except:
                                    pass
                                # Рамбурсаре
                                try:
                                    suma_ramb_val = deal.get("suma_ramb_val") if deal else None
                                    if suma_ramb_val:
                                        if isinstance(suma_ramb_val, str):
                                            cleaned = re.sub(r'[^0-9.,-]', '', suma_ramb_val)
                                            cleaned = cleaned.replace(',', '.')
                                            val = float(cleaned) if cleaned else 0
                                        else:
                                            val = float(suma_ramb_val)
                                        if val > 0:
                                            total_venit -= val
                                except:
                                    pass
                            except:
                                pass
                    except:
                        pass
                
                # Подсчитываем машины в прокате (CHIRIE) для расчета загрузки
                now = datetime.now(timezone.utc)
                chirie_count = 0
                for raw_obj in raw_items:
                    fields = _extract_fields_from_raw(raw_obj)
                    bucket, _ = stock_classify_default(fields, now)
                    if bucket == "CHIRIE":
                        chirie_count += 1
                
                # Рассчитываем загрузку филиала
                total_auto = len(raw_items)
                loading_percent = 0
                if total_auto > 0:
                    loading_percent = round((chirie_count / total_auto) * 100, 1)
                
                # Форматируем дату в формате DD.MM.YYYY
                today_str = _today_in_report_tz().strftime("%d.%m.%Y")
                
                # Тот же расчет, что и в отчете: используем calculate_responsible_totals
                caption_total = 0
                try:
                    third_rows_for_caption = _build_deals_third_table_rows(deals_third_table) if deals_third_table else None
                except Exception:
                    third_rows_for_caption = None
                try:
                    responsible_totals_caption = calculate_responsible_totals_global(
                        deals_for_pdf,
                        deals_third_table,
                        deals_second_table,
                        third_table_rows=third_rows_for_caption,
                    )
                    caption_total = int(round(sum(total for _, total in responsible_totals_caption))) if responsible_totals_caption else 0
                except Exception as e:
                    print(f"WARNING: send_stock_auto_reports: failed to calc caption_total: {e}", file=sys.stderr, flush=True)
                
                # Формируем caption в новом формате (румынский язык) с HTML форматированием
                caption = f"<b>{display_name}</b> - {today_str}\n\n"
                caption += f"<b>Total venit astăzi - {caption_total} MDL</b>\n"
                caption += f"<b>Auto - {total_auto}</b>\n\n"
                caption += f"Mașini date - <b>{deals_auto_date_count}</b>\n"
                caption += f"Mașini primite - <b>{deals_primite_count}</b>\n"
                caption += f"Mașini prelungite - <b>{deals_prelungire_count}</b>\n\n"
                caption += f"Încărcare filială - <b>{loading_percent}%</b> (În chirie - <b>{chirie_count}</b> auto)"

                print(
                    f"DEBUG: send_stock_auto_reports: Sending PDF to Telegram for '{display_name}' (filename={filename}, total_auto={total_auto})",
                    file=sys.stderr,
                    flush=True,
                )
                
                try:
                    send_pdf_to_telegram(pdf, filename=filename, caption=caption)
                    sent += 1
                    results.append({"branch": display_name, "rows_total": len(raw_items), "filter": str(filter_value)})
                    print(
                        f"DEBUG: send_stock_auto_reports: Successfully sent PDF for '{display_name}' (PDF #{sent})",
                        file=sys.stderr,
                        flush=True,
                    )
                    # Дополнительно отправляем тот же PDF в Bitrix-чат
                    try:
                        send_pdf_to_bitrix(pdf, filename=filename, caption=caption)
                        print(
                            f"DEBUG: send_stock_auto_reports: Successfully sent PDF for '{display_name}' to Bitrix",
                            file=sys.stderr,
                            flush=True,
                        )
                    except Exception as bitrix_error:
                        error_msg = f"Failed to send PDF to Bitrix for '{display_name}': {bitrix_error}"
                        print(f"ERROR: send_stock_auto_reports: {error_msg}", file=sys.stderr, flush=True)
                    # Особое внимание к Ungheni
                    if "ungheni" in str(display_name).lower() or str(fv) == "1670":
                        print(
                            f"DEBUG: send_stock_auto_reports: *** UNGHENI SENT SUCCESSFULLY *** PDF #{sent}",
                            file=sys.stderr,
                            flush=True,
                        )
                except Exception as telegram_error:
                    # Отдельно обрабатываем ошибки отправки в Telegram
                    error_msg = f"Telegram send failed: {str(telegram_error)}"
                    print(
                        f"ERROR: send_stock_auto_reports: {error_msg} for '{display_name}'",
                        file=sys.stderr,
                        flush=True,
                    )
                    errors.append({"branch": display_name, "where": "telegram_send", "error": error_msg})
                    # Особое внимание к ошибкам Ungheni
                    if "ungheni" in str(display_name).lower() or str(fv) == "1670":
                        print(
                            f"ERROR: send_stock_auto_reports: *** UNGHENI TELEGRAM SEND FAILED! *** {error_msg}",
                            file=sys.stderr,
                            flush=True,
                        )
                    # Не увеличиваем sent, но продолжаем обработку других филиалов

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                error_msg = str(e)
                import traceback
                tb_str = traceback.format_exc()
                print(
                    f"ERROR: send_stock_auto_reports: Exception processing branch '{display_name}': {error_msg}",
                    file=sys.stderr,
                    flush=True,
                )
                print(
                    f"ERROR: send_stock_auto_reports: Traceback for '{display_name}':\n{tb_str}",
                    file=sys.stderr,
                    flush=True,
                )
                
                # Особое внимание к ошибкам Ungheni
                if is_ungheni:
                    print(
                        f"ERROR: send_stock_auto_reports: *** UNGHENI ERROR *** This is critical! Error: {error_msg}",
                        file=sys.stderr,
                        flush=True,
                    )
                    # Для Ungheni пытаемся сгенерировать пустой PDF даже при ошибке
                    try:
                        print(
                            f"DEBUG: send_stock_auto_reports: *** UNGHENI RECOVERY ATTEMPT *** Trying to generate empty PDF despite error",
                            file=sys.stderr,
                            flush=True,
                        )
                        pdf = generate_pdf_stock_auto_split(
                            [],  # Пустой список
                            branch_name=display_name,
                            branch_id=str(fv),
                            branch_field=STOCK_F_BRANCH,
                            branch_id_name_map=branch_id_name_map,
                            enum_map_brand=enum_brand,
                            enum_map_model=enum_model,
                            deals_auto_date=[],
                            enum_map_sursa=enum_sursa,
                            deals_second_table=[],
                            deals_third_table=[],
                        )
                        safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(display_name)).strip("_") or "branch"
                        filename = f"stock_auto_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                        today_str = _today_in_report_tz().strftime("%d.%m.%Y")
                        caption = (
                            f"<b>{display_name}</b> - {today_str}\n\n"
                            f"<b>Total venit astăzi - 0 MDL</b>\n"
                            f"<b>Auto - 0</b>\n\n"
                            f"Mașini date - <b>0</b>\n"
                            f"Mașini primite - <b>0</b>\n"
                            f"Mașini prelungite - <b>0</b>\n\n"
                            f"Încărcare filială - <b>0%</b> (În chirie - <b>0</b> auto)"
                        )
                        send_pdf_to_telegram(pdf, filename=filename, caption=caption)
                        sent += 1
                        results.append({"branch": display_name, "rows_total": 0, "filter": str(filter_value)})
                        print(
                            f"DEBUG: send_stock_auto_reports: *** UNGHENI RECOVERY SUCCESS *** Empty PDF sent successfully!",
                            file=sys.stderr,
                            flush=True,
                        )
                        # Дополнительно отправляем пустой PDF в Bitrix-чат
                        try:
                            send_pdf_to_bitrix(pdf, filename=filename, caption=caption)
                            print(
                                "DEBUG: send_stock_auto_reports: *** UNGHENI RECOVERY SUCCESS *** Empty PDF sent to Bitrix successfully!",
                                file=sys.stderr,
                                flush=True,
                            )
                        except Exception as bitrix_error:
                            error_msg = f"Failed to send empty UNGHENI PDF to Bitrix: {bitrix_error}"
                            print(f"ERROR: send_stock_auto_reports: {error_msg}", file=sys.stderr, flush=True)
                    except Exception as recovery_error:
                        print(
                            f"ERROR: send_stock_auto_reports: *** UNGHENI RECOVERY FAILED *** {str(recovery_error)}",
                            file=sys.stderr,
                            flush=True,
                        )
                
                errors.append({"branch": display_name, "where": "branch_processing", "error": error_msg})
            
            # Логируем завершение обработки каждого филиала
            # Проверяем, был ли этот филиал успешно отправлен
            was_this_branch_sent = any(r.get("branch") == display_name for r in results)
            print(
                f"DEBUG: send_stock_auto_reports: ===== END Processing branch '{display_name}' (total_sent={sent}, this_branch_sent={was_this_branch_sent}, errors_count={len(errors)}) =====",
                file=sys.stderr,
                flush=True,
            )
            
            # Особое внимание к завершению обработки Ungheni
            if "ungheni" in str(display_name).lower() or str(fv) == "1670":
                print(
                    f"DEBUG: send_stock_auto_reports: *** UNGHENI PROCESSING COMPLETE *** display_name='{display_name}', was_sent={was_this_branch_sent}, total_sent={sent}",
                    file=sys.stderr,
                    flush=True,
                )
                if not was_this_branch_sent:
                    print(
                        f"WARNING: send_stock_auto_reports: *** UNGHENI WAS NOT SENT! *** Check errors above for details.",
                        file=sys.stderr,
                        flush=True,
                    )

        # Финальная проверка: убеждаемся, что Ungheni был обработан
        ungheni_processed = False
        ungheni_sent = False
        for r in results:
            if "ungheni" in str(r.get("branch", "")).lower() or "1670" in str(r.get("filter", "")):
                ungheni_processed = True
                ungheni_sent = True
                break
        
        # Проверяем, был ли Ungheni в списке branches, но не был отправлен
        if not ungheni_processed:
            for display_name, filter_value in branches:
                if "ungheni" in str(display_name).lower() or "1670" in str(filter_value):
                    print(
                        f"ERROR: send_stock_auto_reports: *** UNGHENI WAS IN BRANCHES BUT NOT PROCESSED! ***",
                        file=sys.stderr,
                        flush=True,
                    )
                    print(
                        f"ERROR: send_stock_auto_reports: Ungheni branch: name='{display_name}', id='{filter_value}'",
                        file=sys.stderr,
                        flush=True,
                    )
                    print(
                        f"ERROR: send_stock_auto_reports: Total sent: {sent}, Results: {results}",
                        file=sys.stderr,
                        flush=True,
                    )
                    # Добавляем ошибку в список
                    errors.append({
                        "branch": display_name,
                        "where": "final_check",
                        "error": "Ungheni was in BRANCHES but was not processed or sent"
                    })
                    break
        
        resp: Dict[str, Any] = {
            "ok": True,
            "sent": sent,
            "branches": results,
            "only_today": DEALS_ONLY_TODAY,
            "report_tz": REPORT_TZ,
            "today": str(_today_in_report_tz()),
        }
        if errors:
            resp["errors"] = errors
        
        # Добавляем информацию об Ungheni в ответ
        resp["ungheni_status"] = {
            "in_branches": ungheni_found,
            "processed": ungheni_processed,
            "sent": ungheni_sent,
        }
        
        if send_lock_file and os.path.exists(send_lock_file):
            try:
                os.remove(send_lock_file)
            except Exception:
                pass
        _clear_override()
        return resp

    except HTTPException:
        raise
    except Exception as e:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        try:
            if mark_file and os.path.exists(mark_file):
                os.remove(mark_file)
                print(f"REPORT send: removed mark on error: {mark_file}", file=sys.stderr, flush=True)
        except Exception:
            pass
        try:
            if send_lock_file and os.path.exists(send_lock_file):
                os.remove(send_lock_file)
        except Exception:
            pass
        _clear_override()
        raise HTTPException(status_code=500, detail=str(e))