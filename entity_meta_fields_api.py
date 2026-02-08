"""
API метаданных полей сущностей: тип сущности на вход, поля из БД на выход.
GET /api/entity-meta-fields/?type=deal
GET /api/entity-meta-fields/?type=smart_process&entity_key=sp:1114
"""
import os
import re
import sys
import unicodedata
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query

import psycopg2
import psycopg2.extras

from api_data import pg_conn

router = APIRouter(prefix="/api/entity-meta-fields", tags=["entity-meta-fields"])


def table_name_for_entity(entity_key: str) -> str:
    if entity_key == "deal":
        return "b24_crm_deal"
    elif entity_key == "contact":
        return "b24_crm_contact"
    elif entity_key == "lead":
        return "b24_crm_lead"
    elif entity_key == "company":
        return "b24_crm_company"
    elif entity_key.startswith("sp:"):
        entity_type_id = entity_key.split(":")[1]
        return f"b24_sp_f_{entity_type_id}"
    else:
        raise ValueError(f"Unknown entity_key: {entity_key}")


# Румынские и кириллические буквы — валидные, не трогать при «исправлении» кодировки.
# Румынские: и с запятой снизу (ȘȚ), и с седилью (ŞŢ) — Bitrix/БД могут отдавать любой вариант.
_ALLOWED_EXTENDED = (
    "ĂăÂâÎî"
    "ȘșȚț"   # S/T с запятой снизу (Unicode ș, ț)
    "ŞşŢţ"   # S/T с седилью (часто в Windows-1250 / старых данных)
    "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя"
)


def _count_weird_extended(s: str) -> int:
    """Число символов с кодом > 127, не входящих в разрешённый набор (диакритики, кириллица)."""
    return sum(1 for c in s if ord(c) > 127 and c not in _ALLOWED_EXTENDED)


def normalize_string(value: Any) -> str:
    """Нормализует строку; сохраняет румынские/кириллические диакритики, приводит к NFC."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            s = value.decode("utf-8")
        except UnicodeDecodeError:
            try:
                s = value.decode("latin-1").encode("latin-1").decode("utf-8", errors="ignore")
            except Exception:
                s = value.decode("utf-8", errors="ignore")
        return unicodedata.normalize("NFC", s)
    if isinstance(value, str):
        try:
            check_str = value[:500] if len(value) > 500 else value
            if any(ord(c) > 127 for c in check_str):
                fixed = value.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
                if _count_weird_extended(fixed) < _count_weird_extended(value):
                    return unicodedata.normalize("NFC", fixed)
        except Exception:
            pass
        return unicodedata.normalize("NFC", value)
    return unicodedata.normalize("NFC", str(value))


def _label_to_str(val: Any) -> str:
    """Извлекает строку из label: строка как есть, dict — ru/en/first."""
    if val is None:
        return ""
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        for k in ("ru", "en", "de", "ua"):
            v = val.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        v = next(iter(val.values()), None) if val else None
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _human_title_from_row(row: Dict[str, Any]) -> str:
    """Человеко-читаемое название из b24_title, b24_labels, settings или fallback."""
    b24_title = row.get("b24_title")
    b24_labels = row.get("b24_labels")
    b24_field = row.get("b24_field")
    col_name = row.get("column_name")
    settings = row.get("settings")

    if b24_title:
        return normalize_string(b24_title)

    if b24_labels:
        if isinstance(b24_labels, dict):
            title_raw = (
                b24_labels.get("title")
                or b24_labels.get("label")
                or b24_labels.get("listLabel")
                or b24_labels.get("formLabel")
                or b24_labels.get("filterLabel")
            )
            s = _label_to_str(title_raw)
            if s:
                return normalize_string(s)
        elif isinstance(b24_labels, str):
            try:
                import json
                labels_dict = json.loads(b24_labels) if isinstance(b24_labels, str) else b24_labels
                if isinstance(labels_dict, dict):
                    title_raw = (
                        labels_dict.get("title")
                        or labels_dict.get("label")
                        or labels_dict.get("listLabel")
                        or labels_dict.get("formLabel")
                    )
                    s = _label_to_str(title_raw)
                    if s:
                        return normalize_string(s)
            except Exception:
                pass

    if settings:
        if isinstance(settings, dict):
            title_raw = (
                settings.get("title") or settings.get("label")
                or settings.get("listLabel") or settings.get("editFormLabel") or settings.get("formLabel")
            )
            s = _label_to_str(title_raw)
            if s:
                return normalize_string(s)
        elif isinstance(settings, str):
            try:
                import json
                settings_dict = json.loads(settings) if isinstance(settings, str) else settings
                if isinstance(settings_dict, dict):
                    title_raw = (
                        settings_dict.get("title") or settings_dict.get("label")
                        or settings_dict.get("listLabel") or settings_dict.get("editFormLabel") or settings_dict.get("formLabel")
                    )
                    s = _label_to_str(title_raw)
                    if s:
                        return normalize_string(s)
            except Exception:
                pass

    # UF_CRM_* / ufCrm: человекочитаемый fallback из имени колонки (даже если b24_field пустой)
    code = (b24_field or col_name or "").strip()
    if code and (code.startswith("UF_CRM_") or code.startswith("uf_crm_") or code.startswith("ufCrm")):
        if col_name:
            readable = col_name.replace("ufcrm", "").replace("_", " ").strip()
            if readable:
                return readable.title()
        return b24_field or col_name
    return b24_field or col_name or "Неизвестное поле"


def _field_type_display(b24_type: Optional[str], is_multiple: bool) -> str:
    """Сырой тип для простых; для enum/multiple — «Списочное поле»."""
    if is_multiple:
        return "Списочное поле"
    if not b24_type:
        return "string"
    t = (b24_type or "").strip().lower()
    if t in ("enum", "enumeration", "list"):
        return "Списочное поле"
    return b24_type


def _fetch_entity_fields_flat(conn, entity_key: str) -> List[Dict[str, Any]]:
    """
    Возвращает список полей сущности в формате {id, b24_field, column_name, human_title, field_type}.
    Один уровень, без вложенных nested_fields.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                b24_field,
                column_name,
                b24_type,
                is_multiple,
                b24_title,
                b24_labels,
                settings
            FROM b24_meta_fields
            WHERE entity_key = %s
            ORDER BY b24_field
        """, (entity_key,))
        rows = cur.fetchall()

    if not rows:
        table_name = table_name_for_entity(entity_key)
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                column_rows = cur.fetchall()
                columns = [row[0] for row in column_rows] if column_rows else []
            except Exception:
                columns = []
        base_titles = {
            "id": "ID", "raw": "Данные (JSON)", "created_at": "Дата создания", "updated_at": "Дата обновления",
            "title": "Название", "name": "Имя", "last_name": "Фамилия", "second_name": "Отчество",
            "phone": "Телефон", "email": "Email", "company_id": "ID компании", "assigned_by_id": "Ответственный",
            "status_id": "Статус", "source_id": "Источник", "opportunity": "Сумма", "currency_id": "Валюта",
        }
        return [
            {
                "id": i,
                "b24_field": col,
                "column_name": col,
                "human_title": base_titles.get(col, col.replace("_", " ").title()),
                "field_type": "string",
            }
            for i, col in enumerate(columns, start=1)
        ]

    result = []
    for idx, row in enumerate(rows, start=1):
        b24_field = row.get("b24_field") or ""
        column_name = row.get("column_name") or b24_field
        human_title = _human_title_from_row(row)
        b24_type = row.get("b24_type")
        is_multiple = bool(row.get("is_multiple", False))
        field_type = _field_type_display(b24_type, is_multiple)
        result.append({
            "id": idx,
            "b24_field": b24_field,
            "column_name": column_name,
            "human_title": human_title,
            "field_type": field_type,
        })
    return result


def _entity_key_from_parent_id(name: str) -> Optional[str]:
    """
    Из b24_field/column_name вида parentId1114, parentId2, parentid1094 извлекает entity_key.
    Bitrix: 2=Deal, 3=Contact, 4=Lead; остальные — смарт-процесс sp:N.
    """
    if not name:
        return None
    m = re.match(r"parentid(\d+)$", name.strip().lower())
    if not m:
        return None
    num = int(m.group(1))
    if num == 2:
        return "deal"
    if num == 3:
        return "contact"
    if num == 4:
        return "lead"
    return f"sp:{num}"


def _resolve_nested_entity_key(
    conn,
    field_type: str,
    human_title: str,
    settings: Any,
    b24_field: str = "",
    column_name: str = "",
) -> Optional[str]:
    """
    Для поля crm_contact/crm_lead/crm_entity возвращает entity_key сущности, чьи поля подставлять.
    Для crm_entity использует parentId из b24_field/column_name (parentId1114 → sp:1114, parentId2 → deal).
    """
    if field_type == "crm_contact":
        return "contact"
    if field_type == "crm_lead":
        return "lead"
    if field_type == "crm_company":
        return "company"
    if field_type == "crm_entity":
        # 1) parentId в имени поля — самый надёжный признак
        for name in (b24_field, column_name):
            key = _entity_key_from_parent_id(name)
            if key:
                return key
        # 2) settings.entityTypeId
        if settings and isinstance(settings, dict):
            eid = settings.get("entityTypeId") or settings.get("entity_type_id")
            if eid is not None:
                return f"sp:{eid}"
        # 3) поиск по human_title в b24_meta_entities
        if human_title:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT entity_key
                    FROM b24_meta_entities
                    WHERE entity_kind = 'smart_process'
                      AND TRIM(LOWER(title)) = TRIM(LOWER(%s))
                    LIMIT 1
                """, (human_title.strip(),))
                r = cur.fetchone()
                if r:
                    return r.get("entity_key")
        return None
    return None


@router.get("/")
def get_entity_meta_fields(
    type: str = Query(..., description="Тип сущности: deal, contact, lead, smart_process"),
    entity_key: Optional[str] = Query(None, description="Для smart_process обязателен, например sp:1114"),
) -> Dict[str, Any]:
    """
    Возвращает поля сущности: id, b24_field, column_name, человеческое название, тип поля.
    """
    if type not in ("smart_process", "deal", "contact", "lead"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type: '{type}'. Must be smart_process, deal, contact or lead",
        )

    if type == "deal":
        final_entity_key = "deal"
    elif type == "contact":
        final_entity_key = "contact"
    elif type == "lead":
        final_entity_key = "lead"
    else:
        if not entity_key:
            raise HTTPException(
                status_code=400,
                detail="entity_key is required for type=smart_process",
            )
        if not entity_key.startswith("sp:"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid entity_key for smart_process: '{entity_key}'. Must start with sp:",
            )
        final_entity_key = entity_key

    conn = pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    b24_field,
                    column_name,
                    b24_type,
                    is_multiple,
                    is_required,
                    is_readonly,
                    b24_title,
                    b24_labels,
                    settings
                FROM b24_meta_fields
                WHERE entity_key = %s
                ORDER BY b24_field
            """, (final_entity_key,))
            rows = cur.fetchall()

        if not rows:
            table_name = table_name_for_entity(final_entity_key)
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))
                    column_rows = cur.fetchall()
                    columns = [row[0] for row in column_rows] if column_rows else []
                except Exception as e:
                    print(f"WARNING: entity-meta-fields: {e}", file=sys.stderr, flush=True)
                    columns = []

            base_field_titles = {
                "id": "ID",
                "raw": "Данные (JSON)",
                "created_at": "Дата создания",
                "updated_at": "Дата обновления",
                "title": "Название",
                "name": "Имя",
                "last_name": "Фамилия",
                "second_name": "Отчество",
                "phone": "Телефон",
                "email": "Email",
                "company_id": "ID компании",
                "assigned_by_id": "Ответственный",
                "status_id": "Статус",
                "source_id": "Источник",
                "opportunity": "Сумма",
                "currency_id": "Валюта",
            }

            fields: List[Dict[str, Any]] = []
            for idx, col in enumerate(columns, start=1):
                human = base_field_titles.get(col, col.replace("_", " ").title())
                fields.append({
                    "id": idx,
                    "b24_field": col,
                    "column_name": col,
                    "human_title": human,
                    "field_type": "string",
                })
            return {
                "ok": True,
                "entity_key": final_entity_key,
                "type": type,
                "fields_count": len(fields),
                "fields": fields,
            }

        fields = []
        for idx, row in enumerate(rows, start=1):
            b24_field = row.get("b24_field") or ""
            column_name = row.get("column_name") or b24_field
            human_title = _human_title_from_row(row)
            b24_type = row.get("b24_type")
            is_multiple = bool(row.get("is_multiple", False))
            field_type = _field_type_display(b24_type, is_multiple)

            field_item = {
                "id": idx,
                "b24_field": b24_field,
                "column_name": column_name,
                "human_title": human_title,
                "field_type": field_type,
            }

            ft_lower = (field_type or "").strip().lower()
            is_crm_ref = ft_lower in ("crm_contact", "crm_lead", "crm_company", "crm_entity")
            nested_key = None
            if is_crm_ref:
                nested_key = _resolve_nested_entity_key(
                    conn,
                    ft_lower,
                    human_title,
                    row.get("settings"),
                    b24_field=b24_field,
                    column_name=column_name,
                )
            if not nested_key and (_entity_key_from_parent_id(b24_field) or _entity_key_from_parent_id(column_name)):
                nested_key = _entity_key_from_parent_id(b24_field) or _entity_key_from_parent_id(column_name)
            if is_crm_ref or nested_key:
                if nested_key:
                    field_item["nested_fields"] = _fetch_entity_fields_flat(conn, nested_key)
                else:
                    field_item["nested_fields"] = []

            fields.append(field_item)

        return {
            "ok": True,
            "entity_key": final_entity_key,
            "type": type,
            "fields_count": len(fields),
            "fields": fields,
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
