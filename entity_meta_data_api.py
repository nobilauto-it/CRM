"""
API значений полей сущностей: тип сущности + пагинация на вход, записи (значения полей) на выход.
Соответствует полям из /api/entity-meta-fields/ — ключи в каждой записи = human_title.
Значения расшифровываются: Ответственный/Кем создана — имя пользователя, Контакт — имя контакта,
Лид — название лида, Источник — название из классификатора (и т.д.).
GET /api/entity-meta-data/?type=deal&limit=10&offset=0
GET /api/entity-meta-data/?type=smart_process&entity_key=sp:1114&limit=10&offset=0
"""
import sys
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException, Query

from api_data import pg_conn
from entity_meta_fields_api import (
    table_name_for_entity,
    normalize_string,
    _human_title_from_row,
)


router = APIRouter(prefix="/api/entity-meta-data", tags=["entity-meta-data"])


def _normalize_value(value: Any) -> Any:
    """Нормализует значение для ответа (строки, вложенные dict/list)."""
    if value is None:
        return None
    if isinstance(value, str):
        return normalize_string(value)
    if isinstance(value, dict):
        return {k: _normalize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    return value


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """, (table_name, column_name))
        return cur.fetchone() is not None


def _get_category_column_from_table(conn, table_name: str) -> Optional[str]:
    """Возвращает имя колонки воронки (category_id и т.п.) в таблице, если есть."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """, (table_name,))
        for row in cur.fetchall() or []:
            col = row[0] if isinstance(row, (list, tuple)) else row.get("column_name")
            if col and _is_category_column(col):
                return col
    return None


def _col_to_human_title_map(conn, entity_key: str) -> Dict[str, str]:
    """
    Возвращает маппинг column_name -> human_title для сущности.
    Для сделок: если есть колонка assigned_by_name, используем её для «Ответственный» вместо assigned_by_id.
    """
    table_name = table_name_for_entity(entity_key)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT b24_field, column_name, b24_type, is_multiple,
                   b24_title, b24_labels, settings
            FROM b24_meta_fields
            WHERE entity_key = %s
            ORDER BY b24_field
        """, (entity_key,))
        rows = cur.fetchall()

    if rows:
        col_to_title: Dict[str, str] = {}
        use_assigned_by_name = (
            entity_key == "deal"
            and _table_has_column(conn, table_name, "assigned_by_name")
        )
        for row in rows:
            col = row.get("column_name") or row.get("b24_field") or ""
            if not col:
                continue
            title = _human_title_from_row(row)
            if use_assigned_by_name and col == "assigned_by_id":
                continue
            if use_assigned_by_name and col == "assigned_by_name":
                title = "Ответственный"
            col_to_title[col] = title
        return col_to_title

    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            cols = [row[0] for row in cur.fetchall()] if cur.rowcount else []
        except Exception:
            cols = []
    base_titles = {
        "id": "ID", "raw": "Данные (JSON)", "created_at": "Дата создания", "updated_at": "Дата обновления",
        "title": "Название", "name": "Имя", "last_name": "Фамилия", "second_name": "Отчество",
        "phone": "Телефон", "email": "Email", "company_id": "ID компании", "assigned_by_id": "Ответственный",
        "assigned_by_name": "Ответственный", "status_id": "Статус", "source_id": "Источник",
        "opportunity": "Сумма", "currency_id": "Валюта",
    }
    return {col: base_titles.get(col, col.replace("_", " ").title()) for col in cols}


def _infer_column_type(column_name: str) -> Optional[str]:
    """По имени колонки подсказать тип для расшифровки (если нет в meta)."""
    c = (column_name or "").strip().lower()
    if not c:
        return None
    if c in ("assigned_by_id", "assigned_by_name", "created_by_id", "modified_by_id", "moved_by_id",
             "last_activity_by", "last_activity_by_id", "created_by", "modified_by"):
        return "user"
    if c in ("contact_id", "contact", "contact_ids"):
        return "crm_contact"
    if c in ("lead_id", "lead", "lead_id"):
        return "crm_lead"
    if c in ("company_id", "company"):
        return "crm_company"
    if c == "source_id":
        return "source_id"
    return None


def _load_meta_column_types(conn, entity_key: str) -> Dict[str, str]:
    """Маппинг column_name -> b24_type для сущности (для расшифровки значений)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name, b24_type
            FROM b24_meta_fields
            WHERE entity_key = %s
        """, (entity_key,))
        rows = cur.fetchall()
    out = {}
    for row in rows or []:
        col = row.get("column_name")
        if not col:
            continue
        t = (row.get("b24_type") or "").strip().lower()
        if t:
            out[col] = t
    return out


def _col_types_with_infer(conn, entity_key: str, columns: List[str], col_types: Dict[str, str]) -> Dict[str, str]:
    """Дополняет col_types выведенным типом по имени колонки для колонок без meta."""
    result = dict(col_types)
    for col in columns:
        if col in result:
            continue
        inferred = _infer_column_type(col)
        if inferred:
            result[col] = inferred
    return result


def _load_sources_classifier(conn) -> Dict[str, str]:
    """source_id -> source_name из b24_classifier_sources. Добавляем ключи в верхнем регистре для поиска без учёта регистра."""
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT source_id, source_name FROM b24_classifier_sources")
            for row in cur.fetchall() or []:
                sid = row.get("source_id")
                if sid is not None:
                    s = str(sid).strip()
                    name = normalize_string(row.get("source_name") or "")
                    out[s] = name
                    if s.upper() != s:
                        out[s.upper()] = name
    except Exception:
        pass
    return out


def _load_contact_names(conn, ids: List[int]) -> Dict[str, str]:
    """id -> имя контакта (NAME LAST_NAME из raw)."""
    if not ids:
        return {}
    out: Dict[str, str] = {}
    tbl = table_name_for_entity("contact")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f'SELECT id, raw FROM "{tbl}" WHERE id = ANY(%s)',
            (ids,),
        )
        for row in cur.fetchall() or []:
            uid = row.get("id")
            if uid is None:
                continue
            raw = row.get("raw") or {}
            name = (raw.get("NAME") or raw.get("name") or "").strip()
            last = (raw.get("LAST_NAME") or raw.get("last_name") or "").strip()
            out[str(uid)] = normalize_string(f"{name} {last}".strip() or raw.get("TITLE") or raw.get("title") or str(uid))
    return out


def _load_lead_titles(conn, ids: List[int]) -> Dict[str, str]:
    """id -> название лида (TITLE из raw)."""
    if not ids:
        return {}
    out: Dict[str, str] = {}
    tbl = table_name_for_entity("lead")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f'SELECT id, raw FROM "{tbl}" WHERE id = ANY(%s)',
            (ids,),
        )
        for row in cur.fetchall() or []:
            uid = row.get("id")
            if uid is None:
                continue
            raw = row.get("raw") or {}
            title = raw.get("TITLE") or raw.get("title") or raw.get("NAME") or raw.get("name") or str(uid)
            out[str(uid)] = normalize_string(str(title))
    return out


def _load_company_titles(conn, ids: List[int]) -> Dict[str, str]:
    """id -> название компании из b24_crm_company (справочник, синхронизируется из Bitrix)."""
    if not ids:
        return {}
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, title FROM b24_crm_company WHERE id = ANY(%s)",
                (ids,),
            )
            for row in cur.fetchall() or []:
                cid = row.get("id")
                if cid is not None:
                    out[str(cid)] = normalize_string(row.get("title") or str(cid))
    except Exception as e:
        print(f"WARNING: _load_company_titles: {e}", file=sys.stderr, flush=True)
    return out


def _load_company_data(conn, ids: List[int]) -> Dict[str, Dict[str, Any]]:
    """id -> {title, raw} из b24_crm_company для построения объекта с полями компании."""
    if not ids:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, title, raw FROM b24_crm_company WHERE id = ANY(%s)",
                (ids,),
            )
            for row in cur.fetchall() or []:
                cid = row.get("id")
                if cid is None:
                    continue
                raw = row.get("raw") or {}
                title = row.get("title") or str(cid)
                out[str(cid)] = {"title": normalize_string(title), "raw": raw}
    except Exception as e:
        print(f"WARNING: _load_company_data: {e}", file=sys.stderr, flush=True)
    return out


def _load_company_field_to_human_title(conn) -> Dict[str, str]:
    """b24_field -> human_title для entity_key=company (из b24_meta_fields)."""
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT b24_field, column_name, b24_type, is_multiple,
                       b24_title, b24_labels, settings
                FROM b24_meta_fields
                WHERE entity_key = %s
                ORDER BY b24_field
            """, ("company",))
            for row in cur.fetchall() or []:
                b24_f = (row.get("b24_field") or "").strip()
                if not b24_f:
                    continue
                human = _human_title_from_row(row)
                if human:
                    out[b24_f] = normalize_string(human)
                    out[b24_f.upper()] = out[b24_f]
                    out[b24_f.lower()] = out[b24_f]
    except Exception as e:
        print(f"WARNING: _load_company_field_to_human_title: {e}", file=sys.stderr, flush=True)
    return out


def _build_company_object(
    company_row: Optional[Dict[str, Any]],
    field_to_title: Dict[str, str],
    company_field_enum_map: Optional[Dict[Tuple[str, str], str]] = None,
) -> Dict[str, Any]:
    """
    Строит объект { human_title: value } по полям компании из raw.
    company_row = { title, raw }; raw — объект из Bitrix (ID, TITLE, ADDRESS, UF_CRM_* ...).
    Значения enum/списков расшифровываются через company_field_enum_map.
    """
    result: Dict[str, Any] = {}
    if not company_row:
        return result
    company_field_enum_map = company_field_enum_map or {}
    raw = company_row.get("raw") or {}
    title_fallback = company_row.get("title") or ""
    seen_human: set = set()
    for b24_key in raw:
        human_title = (
            field_to_title.get(b24_key)
            or field_to_title.get((b24_key or "").upper())
            or field_to_title.get((b24_key or "").lower())
        )
        if not human_title or human_title in seen_human:
            continue
        seen_human.add(human_title)
        val = raw[b24_key]
        if val is None and (b24_key or "").upper() == "TITLE":
            val = title_fallback
        if val is not None and company_field_enum_map:
            decoded = _enum_value_to_title(val, company_field_enum_map, b24_key or "")
            if decoded is not val:
                val = decoded
        try:
            result[human_title] = _normalize_value(val)
        except Exception:
            result[human_title] = val
    if not result and title_fallback:
        result["Название"] = title_fallback
    return result


def _load_user_names(conn, ids: List[str]) -> Dict[str, str]:
    """id -> name из таблицы b24_users (кэш заполняется в app.py по вебхуку/крон)."""
    int_ids: List[int] = []
    for x in ids:
        try:
            int_ids.append(int(str(x).strip()))
        except (TypeError, ValueError):
            pass
    if not int_ids:
        return {}
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name FROM b24_users WHERE id = ANY(%s)", (int_ids,))
            for row in cur.fetchall() or []:
                uid = row.get("id")
                if uid is not None:
                    out[str(uid)] = normalize_string(row.get("name") or str(uid))
    except Exception as e:
        print(f"WARNING: _load_user_names: {e}", file=sys.stderr, flush=True)
    return out


def _load_col_to_b24_field(conn, entity_key: str) -> Dict[str, str]:
    """column_name -> b24_field для сущности (из b24_meta_fields)."""
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT column_name, b24_field FROM b24_meta_fields WHERE entity_key = %s
            """, (entity_key,))
            for row in cur.fetchall() or []:
                col = row.get("column_name")
                b24 = row.get("b24_field")
                if col and b24:
                    out[col] = b24
    except Exception:
        pass
    return out


def _load_deal_categories(conn) -> Dict[str, str]:
    """category_id -> name из b24_deal_categories."""
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name FROM b24_deal_categories")
            for row in cur.fetchall() or []:
                cid = row.get("id")
                if cid is not None:
                    out[str(cid)] = normalize_string(row.get("name") or str(cid))
    except Exception:
        pass
    return out


def _load_sp_categories(conn, entity_type_id: str) -> Dict[str, str]:
    """category_id -> name из b24_sp_categories для смарт-процесса (воронки)."""
    out: Dict[str, str] = {}
    if not entity_type_id:
        return out
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT category_id, name FROM b24_sp_categories WHERE entity_type_id = %s",
                (str(entity_type_id).strip(),),
            )
            for row in cur.fetchall() or []:
                cid = row.get("category_id")
                if cid is not None:
                    out[str(cid)] = normalize_string(row.get("name") or str(cid))
    except Exception:
        pass
    return out


def _load_deal_stages(conn) -> Dict[str, str]:
    """stage_id -> name из b24_deal_stages."""
    out: Dict[str, str] = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT stage_id, name FROM b24_deal_stages")
            for row in cur.fetchall() or []:
                sid = row.get("stage_id")
                if sid is not None:
                    out[str(sid)] = normalize_string(row.get("name") or str(sid))
    except Exception:
        pass
    return out


def _load_field_enum_map(conn, entity_key: str, b24_fields: List[str]) -> Dict[Tuple[str, str], str]:
    """(b24_field, value_id) -> value_title из b24_field_enum для указанных полей.
    Заголовки сохраняем через NFC без normalize_string, чтобы не портить диакритику (e.g. ţ)."""
    out: Dict[Tuple[str, str], str] = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8'")
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if (entity_key or "").startswith("sp:"):
                cur.execute("""
                    SELECT b24_field, value_id, value_title FROM b24_field_enum
                    WHERE entity_key = %s
                """, (entity_key,))
            else:
                if not b24_fields:
                    return out
                cur.execute("""
                    SELECT b24_field, value_id, value_title FROM b24_field_enum
                    WHERE entity_key = %s AND b24_field = ANY(%s)
                """, (entity_key, b24_fields))
            for row in cur.fetchall() or []:
                fld = row.get("b24_field")
                vid = row.get("value_id")
                title = row.get("value_title")
                if fld is not None and vid is not None:
                    raw = title if title is not None else str(vid)
                    out[(str(fld), str(vid))] = unicodedata.normalize("NFC", str(raw))
    except Exception:
        pass
    return out


def _source_value_to_title(val: Any, sources: Dict[str, str]) -> Any:
    """Подставляет название источника по ID; значение может быть 'id' или 'id|TYPE' (например UC_Z315Y5 или 60|TELEGRAM)."""
    if val is None:
        return val
    s = str(val).strip()
    if not s:
        return val
    if s in sources:
        return sources[s]
    if s.upper() in sources:
        return sources[s.upper()]
    if "|" in s:
        key = s.split("|")[0].strip()
        if key and key in sources:
            return sources[key]
        if key and key.upper() in sources:
            return sources[key.upper()]
    return val


def _is_category_column(col: Optional[str]) -> bool:
    """Проверяет, что колонка — это воронка/категория (deal или smart_process)."""
    if not col:
        return False
    c = (col or "").replace("_", "").replace(" ", "").upper()
    return c == "CATEGORYID"


def _category_id_to_name(val: Any, name_map: Dict[str, str]) -> Any:
    """Подставляет название воронки по id; name_map ключи — строки (id)."""
    if val is None or not name_map:
        return val
    s = str(val).strip()
    if s in name_map:
        return name_map[s]
    try:
        n = int(val)
        if str(n) in name_map:
            return name_map[str(n)]
    except (TypeError, ValueError):
        pass
    return val


def _enum_value_to_title(val: Any, field_enum_map: Dict[Tuple[str, str], str], b24_field: str) -> Any:
    """Подставляет название по value_id; значение может быть 'id' или 'id|extra' (например 0|MDL)."""
    if val is None:
        return val
    s = str(val).strip()
    if not s:
        return val
    key_id = s.split("|")[0].strip() if "|" in s else s
    if key_id and (b24_field, key_id) in field_enum_map:
        return field_enum_map[(b24_field, key_id)]
    if (b24_field, s) in field_enum_map:
        return field_enum_map[(b24_field, s)]
    # На случай если в БД value пришёл числом (126), а в мапе ключ строкой "126"
    try:
        n = int(key_id)
        if (b24_field, str(n)) in field_enum_map:
            return field_enum_map[(b24_field, str(n))]
    except (TypeError, ValueError):
        pass
    return val


def _decode_record(
    record: Dict[str, Any],
    src_row: Dict[str, Any],
    entity_key: str,
    col_to_title: Dict[str, str],
    output_to_col: Dict[str, str],
    col_types: Dict[str, str],
    sources: Dict[str, str],
    contact_names: Dict[str, str],
    lead_titles: Dict[str, str],
    user_names_map: Dict[str, str],
    categories_map: Optional[Dict[str, str]] = None,
    stages_map: Optional[Dict[str, str]] = None,
    field_enum_map: Optional[Dict[Tuple[str, str], str]] = None,
    col_to_b24_field: Optional[Dict[str, str]] = None,
    company_titles: Optional[Dict[str, str]] = None,
    company_data: Optional[Dict[str, Dict[str, Any]]] = None,
    company_field_to_title: Optional[Dict[str, str]] = None,
    company_field_enum_map: Optional[Dict[Tuple[str, str], str]] = None,
    sp_categories_map: Optional[Dict[str, str]] = None,
) -> None:
    """
    Подставляет в record человекочитаемые значения вместо ID для полей типа user, crm_contact, crm_lead,
    crm_company (полный объект с полями компании и человекочитаемыми названиями/значениями), source,
    category_id, stage_id и enum/списочных полей (UF_CRM_* и др.).
    Все данные только из БД (b24_users, b24_crm_contact, b24_crm_lead, b24_crm_company, b24_classifier_sources,
    b24_deal_categories, b24_deal_stages, b24_field_enum).
    """
    categories_map = categories_map or {}
    stages_map = stages_map or {}
    field_enum_map = field_enum_map or {}
    col_to_b24_field = col_to_b24_field or {}
    company_titles = company_titles or {}
    company_data = company_data or {}
    company_field_to_title = company_field_to_title or {}
    company_field_enum_map = company_field_enum_map or {}
    sp_categories_map = sp_categories_map or {}
    for title, col in output_to_col.items():
        val = record.get(title)
        if val is None and title not in record:
            continue
        t = (col_types.get(col) or "").strip().lower()
        if entity_key == "deal" and _is_category_column(col):
            record[title] = _category_id_to_name(val, categories_map)
            continue
        if (entity_key or "").startswith("sp:") and _is_category_column(col):
            record[title] = _category_id_to_name(val, sp_categories_map)
            continue
        if (entity_key == "deal" or (entity_key or "").startswith("sp:")) and (
            col in ("stage_id", "stageid") or (col and col.upper() in ("STAGE_ID", "STAGEID"))
        ):
            if val is not None and str(val).strip():
                record[title] = stages_map.get(str(val).strip(), val)
            continue
        if t in ("user", "crm_user", "assigned_by"):
            # Для user-полей всегда предпочитаем raw ID (ASSIGNED_BY_ID/assigned_by_id),
            # даже если в текущей колонке лежит имя (assigned_by_name).
            b24_f = (col_to_b24_field or {}).get(col) or ""
            raw_id_val = None
            candidates = [
                "assigned_by_id",
                "ASSIGNED_BY_ID",
                b24_f,
                str(b24_f).lower(),
                str(b24_f).upper(),
                col,
                str(col).lower(),
                str(col).upper(),
            ]
            for ck in candidates:
                if not ck:
                    continue
                v = src_row.get(ck)
                if v not in (None, "", 0, "0"):
                    raw_id_val = v
                    break
            if raw_id_val in (None, "", 0, "0"):
                raw_obj = src_row.get("raw")
                if isinstance(raw_obj, dict):
                    for rk in ("ASSIGNED_BY_ID", "assigned_by_id", b24_f, str(b24_f).lower()):
                        if not rk:
                            continue
                        v = raw_obj.get(rk)
                        if v not in (None, "", 0, "0"):
                            raw_id_val = v
                            break

            # Если id не найден — fallback на текущее значение, но не пустую строку.
            raw_val = raw_id_val if raw_id_val not in (None, "", 0, "0") else val
            if raw_val in (None, "", 0, "0"):
                record[title] = ""
                continue

            key = str(raw_val).strip()
            # Для тех. ключа assigned_by_id всегда возвращаем raw id.
            if str(title).strip().lower() == "assigned_by_id":
                record[title] = key
            else:
                # Для человекочитаемых ключей (Ответственный/assigned_by_name) отдаем имя, fallback на id.
                record[title] = user_names_map.get(key) or key
            # Тех.ключ assigned_by_id добавляем только если он был явно запрошен фронтом.
            if (
                "assigned_by_id" in output_to_col
                and "assigned_by_id" not in record
                and title == "Ответственный"
            ):
                record["assigned_by_id"] = key
        elif t in ("crm_contact", "contact"):
            if val is None:
                continue
            record[title] = contact_names.get(str(val).strip(), val)
        elif t in ("crm_lead", "lead"):
            if val is None:
                continue
            record[title] = lead_titles.get(str(val).strip(), val)
        elif t in ("crm_company", "company"):
            if val is None:
                continue
            key = str(val).strip()
            company_row = company_data.get(key)
            if company_row and company_field_to_title:
                obj = _build_company_object(
                    company_row, company_field_to_title, company_field_enum_map
                )
                record[title] = obj if obj else company_titles.get(key, val)
            else:
                record[title] = company_titles.get(key, val)
        elif (col == "source_id" or (col and col.upper() == "SOURCE_ID")) and (
            entity_key in ("deal", "lead", "contact") or (entity_key or "").startswith("sp:")
        ):
            if val is None:
                continue
            decoded = _source_value_to_title(val, sources)
            if decoded == val and field_enum_map and col_to_b24_field.get(col):
                decoded = _enum_value_to_title(val, field_enum_map, col_to_b24_field[col])
            if decoded is not val:
                record[title] = decoded
        elif t in ("enum", "enumeration", "list") or (field_enum_map and col_to_b24_field.get(col)):
            b24_f = col_to_b24_field.get(col)
            if b24_f and val is not None:
                decoded = _enum_value_to_title(val, field_enum_map, b24_f)
                if decoded is not val:
                    record[title] = decoded


@router.get("/debug-enum-raw")
def debug_enum_raw_value(
    entity_key: str = Query("sp:1114"),
    b24_field: str = Query("ufCrm34_1748431272"),
    value_id: str = Query("128"),
) -> Dict[str, Any]:
    """Что именно приходит из БД для одной записи enum (value_title)."""
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8'")
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT value_title FROM b24_field_enum WHERE entity_key = %s AND b24_field = %s AND value_id = %s",
                (entity_key, b24_field, value_id),
            )
            row = cur.fetchone()
        if not row:
            return {"ok": True, "found": False}
        val = row.get("value_title")
        return {
            "ok": True,
            "found": True,
            "value_title": val,
            "repr": repr(val) if val is not None else None,
            "codepoints": [ord(c) for c in str(val)] if val is not None else None,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/debug-enum")
def debug_entity_enum(
    entity_key: str = Query(..., description="Например sp:1114 или deal"),
) -> Dict[str, Any]:
    """
    Отладка: сколько enum-значений в БД для сущности и пример маппинга колонка -> b24_field.
    Вызов: GET /api/entity-meta-data/debug-enum?entity_key=sp:1114
    """
    conn = pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT b24_field, value_id, value_title FROM b24_field_enum WHERE entity_key = %s LIMIT 50",
                (entity_key,),
            )
            enum_rows = cur.fetchall() or []
        col_to_b24 = _load_col_to_b24_field(conn, entity_key)
        # Найти колонки, похожие на Transmisie/Tractiune (по b24_title в meta)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT column_name, b24_field, b24_title FROM b24_meta_fields WHERE entity_key = %s",
                (entity_key,),
            )
            meta_rows = cur.fetchall() or []
        return {
            "entity_key": entity_key,
            "enum_count": len(enum_rows),
            "enum_sample": [dict(r) for r in enum_rows[:20]],
            "col_to_b24_count": len(col_to_b24),
            "col_to_b24_sample": dict(list(col_to_b24.items())[:15]),
            "meta_fields_sample": [{"column_name": r.get("column_name"), "b24_field": r.get("b24_field"), "b24_title": r.get("b24_title")} for r in meta_rows[:20]],
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/")
def get_entity_meta_data(
    type: str = Query(..., description="Тип сущности: deal, contact, lead, company, smart_process"),
    entity_key: Optional[str] = Query(None, description="Для smart_process обязателен, например sp:1114"),
    limit: int = Query(100, ge=1, le=10000, description="Максимум записей"),
    offset: int = Query(0, ge=0, description="Смещение"),
    id: Optional[int] = Query(None, description="Фильтр по одному ID записи"),
    ids: Optional[str] = Query(None, description="Фильтр по нескольким ID (через запятую)"),
    fields: Optional[str] = Query(
        None,
        description="Список полей (human_title через запятую). Если задан — в ответе только эти поля; иначе все.",
    ),
    category_id: Optional[str] = Query(
        None,
        description="Фильтр по воронке/категории (для deal и smart_process). В ответе только записи этой категории; total считается по отфильтрованным.",
    ),
) -> Dict[str, Any]:
    """
    Возвращает значения полей сущности: массив записей, ключи в каждой записи = human_title
    (как в /api/entity-meta-fields/), значения — из БД.
    Параметр fields — только запрошенные поля в каждой записи.
    Параметр category_id — фильтр по воронке (deal/smart_process); total — по отфильтрованным записям.
    """
    if type not in ("smart_process", "deal", "contact", "lead", "company"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type: '{type}'. Must be smart_process, deal, contact, lead or company",
        )

    if type == "deal":
        final_entity_key = "deal"
    elif type == "contact":
        final_entity_key = "contact"
    elif type == "lead":
        final_entity_key = "lead"
    elif type == "company":
        final_entity_key = "company"
    else:
        if not entity_key or not entity_key.startswith("sp:"):
            raise HTTPException(
                status_code=400,
                detail="entity_key is required for type=smart_process (e.g. sp:1114)",
            )
        final_entity_key = entity_key

    table_name = table_name_for_entity(final_entity_key)
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8'")
        col_to_title = _col_to_human_title_map(conn, final_entity_key)
        if not col_to_title:
            return {
                "ok": True,
                "entity_key": final_entity_key,
                "type": type,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "data": [],
                "fields": [],
            }

        all_columns = list(col_to_title.keys())
        category_col = next((c for c in all_columns if _is_category_column(c)), None)
        cid = (str(category_id).strip() if category_id is not None else "") or ""
        if cid and not category_col:
            category_col = _get_category_column_from_table(conn, table_name)

        # Универсальные фильтры: category_id + id/ids
        where_parts: List[str] = []
        where_params: List[Any] = []
        if cid and category_col:
            safe_col = category_col.replace('"', '""')
            where_parts.append(f'"{safe_col}"::text = %s')
            where_params.append(cid)

        id_values: List[int] = []
        if id is not None:
            try:
                iv = int(id)
                if iv > 0:
                    id_values.append(iv)
            except Exception:
                pass
        if ids:
            for s in [x.strip() for x in str(ids).split(",") if x.strip()]:
                try:
                    iv = int(s)
                    if iv > 0:
                        id_values.append(iv)
                except Exception:
                    continue
        id_values = list(dict.fromkeys(id_values))
        if id_values:
            where_parts.append("id = ANY(%s)")
            where_params.append(id_values)

        where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        count_params: List[Any] = list(where_params)
        select_params: List[Any] = list(where_params) + [limit, offset]

        with conn.cursor() as cur:
            if where_sql:
                cur.execute(f'SELECT COUNT(*) AS cnt FROM "{table_name}"{where_sql}', count_params)
            else:
                cur.execute(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
            total = cur.fetchone()[0] if cur.rowcount else 0

        columns = list(all_columns)
        title_to_col = {title: col for col, title in col_to_title.items()}
        col_to_b24 = _load_col_to_b24_field(conn, final_entity_key)
        # Алиасы для fields: human_title / column_name / b24_field (регистронезависимо)
        field_alias_to_col: Dict[str, str] = {}
        for col, title in col_to_title.items():
            for k in (title, str(title).lower(), col, str(col).lower()):
                if k:
                    field_alias_to_col[k] = col
            b24_f = col_to_b24.get(col)
            if b24_f:
                field_alias_to_col[str(b24_f)] = col
                field_alias_to_col[str(b24_f).lower()] = col
        # Для deal: даже если "Ответственный" показывается через assigned_by_name,
        # запрос fields=assigned_by_id должен явно маппиться в raw id колонку.
        if final_entity_key == "deal":
            has_assigned_id = _table_has_column(conn, table_name, "assigned_by_id")
            has_assigned_name = _table_has_column(conn, table_name, "assigned_by_name")
            if has_assigned_id:
                field_alias_to_col["assigned_by_id"] = "assigned_by_id"
                field_alias_to_col["ASSIGNED_BY_ID"] = "assigned_by_id"
            # Критично для фронта: эти ключи должны маппиться всегда.
            # Если нет физической assigned_by_name — используем assigned_by_id и резолвим имя в _decode_record.
            if has_assigned_name:
                field_alias_to_col["assigned_by_name"] = "assigned_by_name"
                field_alias_to_col["ASSIGNED_BY_NAME"] = "assigned_by_name"
                field_alias_to_col["Ответственный"] = "assigned_by_name"
                field_alias_to_col["ответственный"] = "assigned_by_name"
            elif has_assigned_id:
                field_alias_to_col["assigned_by_name"] = "assigned_by_id"
                field_alias_to_col["ASSIGNED_BY_NAME"] = "assigned_by_id"
                field_alias_to_col["Ответственный"] = "assigned_by_id"
                field_alias_to_col["ответственный"] = "assigned_by_id"
        requested_output_pairs: List[Tuple[str, Optional[str]]] = []
        if fields:
            requested_titles = [s.strip() for s in fields.split(",") if s.strip()]
            if requested_titles:
                requested_cols: List[str] = []
                for t in requested_titles:
                    c = (
                        title_to_col.get(t)
                        or field_alias_to_col.get(t)
                        or field_alias_to_col.get(str(t).lower())
                    )
                    requested_output_pairs.append((t, c))
                    if c:
                        requested_cols.append(c)
                requested_cols = list(dict.fromkeys(c for c in requested_cols if c))
                # strict contract: если fields передан, в output только запрошенные ключи
                columns = requested_cols

        # Для user-полей иногда нужен сырой id из assigned_by_id, даже если фронт просит "Ответственный".
        query_columns = list(columns)
        has_user_requested = False
        col_types_preview = _col_types_with_infer(
            conn, final_entity_key, query_columns, _load_meta_column_types(conn, final_entity_key)
        )
        for c in query_columns:
            t = (col_types_preview.get(c) or "").strip().lower()
            if t in ("user", "crm_user", "assigned_by"):
                has_user_requested = True
                break
            if c == "assigned_by_id" or (col_to_b24.get(c, "").upper() == "ASSIGNED_BY_ID"):
                has_user_requested = True
                break
        if has_user_requested and _table_has_column(conn, table_name, "assigned_by_id") and "assigned_by_id" not in query_columns:
            query_columns.append("assigned_by_id")

        if not query_columns:
            # Технический минимум для валидного SELECT, если ни один requested key не сматчился с колонкой.
            query_columns = ["id"]

        columns_str = ", ".join(f'"{c}"' for c in query_columns)
        col_types = _col_types_with_infer(
            conn, final_entity_key, query_columns, _load_meta_column_types(conn, final_entity_key)
        )

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if where_sql:
                cur.execute(
                    f'SELECT {columns_str} FROM "{table_name}"{where_sql} ORDER BY id DESC LIMIT %s OFFSET %s',
                    tuple(select_params),
                )
            else:
                cur.execute(
                    f'SELECT {columns_str} FROM "{table_name}" ORDER BY id DESC LIMIT %s OFFSET %s',
                    (limit, offset),
                )
            rows = cur.fetchall()

        contact_ids: List[int] = []
        lead_ids: List[int] = []
        company_ids: List[int] = []
        user_ids: List[str] = []
        for col, t in col_types.items():
            if t in ("crm_contact", "contact"):
                for row in rows:
                    v = row.get(col)
                    if v is not None and str(v).strip():
                        try:
                            contact_ids.append(int(v))
                        except (TypeError, ValueError):
                            pass
            elif t in ("crm_lead", "lead"):
                for row in rows:
                    v = row.get(col)
                    if v is not None and str(v).strip():
                        try:
                            lead_ids.append(int(v))
                        except (TypeError, ValueError):
                            pass
            elif t in ("crm_company", "company"):
                for row in rows:
                    v = row.get(col)
                    if v is not None and str(v).strip():
                        try:
                            company_ids.append(int(v))
                        except (TypeError, ValueError):
                            pass
            elif t in ("user", "crm_user", "assigned_by"):
                for row in rows:
                    v = row.get(col)
                    if v is not None and str(v).strip():
                        user_ids.append(str(v).strip())

        sources_map = (
            _load_sources_classifier(conn)
            if final_entity_key in ("deal", "lead", "contact") or (final_entity_key or "").startswith("sp:")
            else {}
        )
        contact_names_map = _load_contact_names(conn, list(dict.fromkeys(contact_ids))) if contact_ids else {}
        lead_titles_map = _load_lead_titles(conn, list(dict.fromkeys(lead_ids))) if lead_ids else {}
        company_ids_unique = list(dict.fromkeys(company_ids))
        company_titles_map = _load_company_titles(conn, company_ids_unique) if company_ids else {}
        company_data_map = _load_company_data(conn, company_ids_unique) if company_ids else {}
        company_field_to_title_map = _load_company_field_to_human_title(conn) if company_ids else {}
        company_b24_fields = list(company_field_to_title_map.keys()) if company_field_to_title_map else []
        company_field_enum_map = (
            _load_field_enum_map(conn, "company", company_b24_fields) if company_b24_fields else {}
        )
        user_ids_unique = list(dict.fromkeys(user_ids))
        user_names_map = _load_user_names(conn, user_ids_unique) if user_ids_unique else {}

        categories_map = _load_deal_categories(conn) if final_entity_key == "deal" else {}
        sp_entity_type_id = (final_entity_key or "").split(":")[-1] if (final_entity_key or "").startswith("sp:") else ""
        sp_categories_map = _load_sp_categories(conn, sp_entity_type_id) if sp_entity_type_id else {}
        stages_map = (
            _load_deal_stages(conn)
            if final_entity_key == "deal" or (final_entity_key or "").startswith("sp:")
            else {}
        )
        b24_fields_for_enum = list(dict.fromkeys(col_to_b24.values())) if col_to_b24 else []
        field_enum_map = _load_field_enum_map(conn, final_entity_key, b24_fields_for_enum) if b24_fields_for_enum else {}

        output_pairs: List[Tuple[str, Optional[str]]]
        if requested_output_pairs:
            # Сохраняем запрошенные ключи фронта как ключи output row
            output_pairs = requested_output_pairs
        else:
            output_pairs = [(col_to_title.get(c, c), c) for c in columns]

        output_to_col: Dict[str, str] = {}
        for out_key, c in output_pairs:
            if out_key and c:
                output_to_col[out_key] = c

        data: List[Dict[str, Any]] = []
        for row in rows:
            record: Dict[str, Any] = {}
            for out_key, col in output_pairs:
                value = row.get(col) if col else None
                try:
                    record[out_key] = _normalize_value(value)
                except Exception as e:
                    print(f"WARNING: entity-meta-data normalize {col}: {e}", file=sys.stderr, flush=True)
                    record[out_key] = value
            _decode_record(
                record,
                row,
                final_entity_key,
                col_to_title,
                output_to_col,
                col_types,
                sources_map,
                contact_names_map,
                lead_titles_map,
                user_names_map,
                categories_map=categories_map,
                stages_map=stages_map,
                field_enum_map=field_enum_map,
                col_to_b24_field=col_to_b24,
                company_titles=company_titles_map,
                company_data=company_data_map,
                company_field_to_title=company_field_to_title_map,
                company_field_enum_map=company_field_enum_map,
                sp_categories_map=sp_categories_map,
            )
            data.append(record)

        out: Dict[str, Any] = {
            "ok": True,
            "entity_key": final_entity_key,
            "type": type,
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": data,
        }
        if requested_output_pairs:
            out["fields"] = [k for k, _ in output_pairs]
        else:
            out["fields"] = [col_to_title.get(c, c) for c in columns]
        return out
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@router.get("/by-ids")
def get_entity_meta_data_by_ids(
    type: str = Query(..., description="Тип сущности: contact, lead, company"),
    ids: str = Query(..., description="Comma-separated IDs, e.g. 10,11,12"),
    fields: Optional[str] = Query(
        None,
        description="Список полей через запятую. Если задан — строгий output только по requested keys.",
    ),
) -> Dict[str, Any]:
    if type not in ("contact", "lead", "company"):
        raise HTTPException(status_code=400, detail="type must be contact, lead or company")

    id_list: List[int] = []
    for s in [x.strip() for x in (ids or "").split(",") if x.strip()]:
        try:
            v = int(s)
            if v > 0:
                id_list.append(v)
        except Exception:
            continue
    id_list = list(dict.fromkeys(id_list))
    if not id_list:
        return {"ok": True, "type": type, "ids": [], "data": [], "fields": []}

    final_entity_key = type
    table_name = table_name_for_entity(final_entity_key)
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8'")

        col_to_title = _col_to_human_title_map(conn, final_entity_key)
        all_columns = list(col_to_title.keys())
        title_to_col = {title: col for col, title in col_to_title.items()}
        col_to_b24 = _load_col_to_b24_field(conn, final_entity_key)

        field_alias_to_col: Dict[str, str] = {}
        for col, title in col_to_title.items():
            for k in (title, str(title).lower(), col, str(col).lower()):
                if k:
                    field_alias_to_col[k] = col
            b24_f = col_to_b24.get(col)
            if b24_f:
                field_alias_to_col[str(b24_f)] = col
                field_alias_to_col[str(b24_f).lower()] = col

        columns = list(all_columns)
        requested_output_pairs: List[Tuple[str, Optional[str]]] = []
        if fields:
            requested = [s.strip() for s in fields.split(",") if s.strip()]
            for t in requested:
                c = title_to_col.get(t) or field_alias_to_col.get(t) or field_alias_to_col.get(str(t).lower())
                requested_output_pairs.append((t, c))
            columns = list(dict.fromkeys(c for _, c in requested_output_pairs if c))

        query_columns = list(columns)
        if "id" not in query_columns:
            query_columns.append("id")
        if not query_columns:
            query_columns = ["id"]

        columns_str = ", ".join(f'"{c}"' for c in query_columns)
        col_types = _col_types_with_infer(
            conn, final_entity_key, query_columns, _load_meta_column_types(conn, final_entity_key)
        )

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f'SELECT {columns_str} FROM "{table_name}" WHERE id = ANY(%s)',
                (id_list,),
            )
            rows = cur.fetchall() or []

        # decode helpers
        contact_ids: List[int] = []
        lead_ids: List[int] = []
        company_ids: List[int] = []
        user_ids: List[str] = []
        for col, t in col_types.items():
            for row in rows:
                v = row.get(col)
                if v is None or not str(v).strip():
                    continue
                if t in ("crm_contact", "contact"):
                    try:
                        contact_ids.append(int(v))
                    except Exception:
                        pass
                elif t in ("crm_lead", "lead"):
                    try:
                        lead_ids.append(int(v))
                    except Exception:
                        pass
                elif t in ("crm_company", "company"):
                    try:
                        company_ids.append(int(v))
                    except Exception:
                        pass
                elif t in ("user", "crm_user", "assigned_by"):
                    user_ids.append(str(v).strip())

        contact_names_map = _load_contact_names(conn, list(dict.fromkeys(contact_ids))) if contact_ids else {}
        lead_titles_map = _load_lead_titles(conn, list(dict.fromkeys(lead_ids))) if lead_ids else {}
        company_ids_unique = list(dict.fromkeys(company_ids))
        company_titles_map = _load_company_titles(conn, company_ids_unique) if company_ids_unique else {}
        company_data_map = _load_company_data(conn, company_ids_unique) if company_ids_unique else {}
        company_field_to_title_map = _load_company_field_to_human_title(conn) if company_ids_unique else {}
        company_b24_fields = list(company_field_to_title_map.keys()) if company_field_to_title_map else []
        company_field_enum_map = _load_field_enum_map(conn, "company", company_b24_fields) if company_b24_fields else {}
        user_names_map = _load_user_names(conn, list(dict.fromkeys(user_ids))) if user_ids else {}
        field_enum_map = _load_field_enum_map(conn, final_entity_key, list(dict.fromkeys(col_to_b24.values()))) if col_to_b24 else {}

        if requested_output_pairs:
            output_pairs: List[Tuple[str, Optional[str]]] = requested_output_pairs
        else:
            output_pairs = [(col_to_title.get(c, c), c) for c in columns]

        output_to_col: Dict[str, str] = {}
        for out_key, c in output_pairs:
            if out_key and c:
                output_to_col[out_key] = c

        by_id: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            rid = row.get("id")
            try:
                rid_int = int(rid)
            except Exception:
                continue
            record: Dict[str, Any] = {"id": rid_int}
            for out_key, col in output_pairs:
                value = row.get(col) if col else None
                record[out_key] = _normalize_value(value)
            _decode_record(
                record,
                row,
                final_entity_key,
                col_to_title,
                output_to_col,
                col_types,
                {},
                contact_names_map,
                lead_titles_map,
                user_names_map,
                categories_map={},
                stages_map={},
                field_enum_map=field_enum_map,
                col_to_b24_field=col_to_b24,
                company_titles=company_titles_map,
                company_data=company_data_map,
                company_field_to_title=company_field_to_title_map,
                company_field_enum_map=company_field_enum_map,
                sp_categories_map={},
            )
            by_id[rid_int] = record

        ordered = [by_id[i] for i in id_list if i in by_id]
        out_fields = ["id"] + ([k for k, _ in output_pairs] if requested_output_pairs else [col_to_title.get(c, c) for c in columns])
        return {
            "ok": True,
            "type": type,
            "ids": id_list,
            "fields": out_fields,
            "data": ordered,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
