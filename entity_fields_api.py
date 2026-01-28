import os
import sys
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query

import psycopg2
import psycopg2.extras

# Импортируем функции из api_data.py (избегаем циклического импорта)
from api_data import pg_conn

router = APIRouter(prefix="/api/entity-fields", tags=["entity-fields"])


def table_name_for_entity(entity_key: str) -> str:
    """Возвращает имя таблицы для entity_key"""
    if entity_key == "deal":
        return "b24_crm_deal"
    elif entity_key == "contact":
        return "b24_crm_contact"
    elif entity_key == "lead":
        return "b24_crm_lead"
    elif entity_key.startswith("sp:"):
        entity_type_id = entity_key.split(":")[1]
        return f"b24_sp_f_{entity_type_id}"
    else:
        raise ValueError(f"Unknown entity_key: {entity_key}")


def normalize_string(value: Any) -> str:
    """
    Нормализует строку из базы данных, исправляя проблемы с кодировкой.
    Обрабатывает случаи double-encoded UTF-8 и другие проблемы с кодировкой.
    """
    if value is None:
        return ""
    
    if isinstance(value, bytes):
        # Если это bytes, пробуем декодировать как UTF-8
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # Если не получается, пробуем latin-1 (1:1 mapping) и затем UTF-8
            try:
                decoded = value.decode('latin-1')
                # Пробуем исправить double-encoded
                return decoded.encode('latin-1').decode('utf-8', errors='ignore')
            except:
                return value.decode('utf-8', errors='ignore')
    
    # Если это уже строка
    if isinstance(value, str):
        # Проверяем, не является ли это double-encoded UTF-8
        try:
            # Пробуем перекодировать через latin-1 -> UTF-8
            check_str = value[:100] if len(value) > 100 else value
            if any(ord(c) > 127 for c in check_str):
                fixed = value.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore')
                # Если исправленная версия выглядит лучше
                if len([c for c in fixed if ord(c) > 127 and c not in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюяĂăÂâÎîȘșȚț']) < len([c for c in value if ord(c) > 127 and c not in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюяĂăÂâÎîȘșȚț']):
                    return fixed
        except:
            pass
        return value
    
    return str(value)


@router.get("/")
def get_entity_fields(
    type: str = Query(..., description="Тип сущности: 'smart_process', 'deal', 'contact' или 'lead'"),
    entity_key: Optional[str] = Query(None, description="Ключ сущности (обязателен для smart_process): например 'sp:1114'"),
) -> Dict[str, Any]:
    """
    Возвращает список полей для выбранной сущности.
    
    Параметры:
    - type: тип сущности ("smart_process", "deal", "contact" или "lead")
    - entity_key: ключ сущности (обязателен для smart_process, для остальных не нужен)
    
    Для сделок (type="deal"):
    - entity_key не требуется, используется "deal"
    - Поля всегда одинаковые
    
    Для контактов (type="contact"):
    - entity_key не требуется, используется "contact"
    - Поля всегда одинаковые
    
    Для лидов (type="lead"):
    - entity_key не требуется, используется "lead"
    - Поля всегда одинаковые
    
    Для смарт-процессов (type="smart_process"):
    - entity_key обязателен (например "sp:1114")
    - Поля зависят от конкретного типа смарт-процесса
    """
    conn = pg_conn()
    try:
        # Валидация параметров
        if type not in ["smart_process", "deal", "contact", "lead"]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid type: '{type}'. Must be 'smart_process', 'deal', 'contact' or 'lead'"
            )
        
        # Определяем entity_key
        if type == "deal":
            final_entity_key = "deal"
        elif type == "contact":
            final_entity_key = "contact"
        elif type == "lead":
            final_entity_key = "lead"
        else:  # smart_process
            if not entity_key:
                raise HTTPException(
                    status_code=400,
                    detail="entity_key is required for type='smart_process'"
                )
            # Проверяем формат entity_key для смарт-процесса
            if not entity_key.startswith("sp:"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid entity_key format for smart_process: '{entity_key}'. Must start with 'sp:'"
                )
            final_entity_key = entity_key
        
        # Получаем поля из базы данных
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
        
        # Если метаданных нет, используем fallback - получаем колонки напрямую из таблицы
        if not rows:
            table_name = table_name_for_entity(final_entity_key)
            
            # Получаем список колонок из таблицы
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
                    print(f"WARNING: Could not get columns for table {table_name}: {e}", file=sys.stderr, flush=True)
                    columns = []
            
            if not columns:
                return {
                    "ok": True,
                    "entity_key": final_entity_key,
                    "type": type,
                    "fields_count": 0,
                    "fields": []
                }
            
            # Создаем базовые поля из колонок таблицы
            fields: List[Dict[str, Any]] = []
            
            # Базовые названия для стандартных полей
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
                "currency_id": "Валюта"
            }
            
            for col in columns:
                # Используем базовое название или делаем колонку более читаемой
                if col in base_field_titles:
                    title = base_field_titles[col]
                else:
                    # Пытаемся сделать название более читаемым
                    title = col.replace("_", " ").title()
                
                fields.append({
                    "title": title,
                    "name": title,
                    "type": None,  # Тип неизвестен без метаданных
                    "is_multiple": False,
                    "is_required": False,
                    "is_readonly": False
                })
            
            return {
                "ok": True,
                "entity_key": final_entity_key,
                "type": type,
                "fields_count": len(fields),
                "fields": fields
            }
        
        # Формируем список полей
        # ВАЖНО: Возвращаем только человеко-читаемые названия, технические имена скрыты
        fields: List[Dict[str, Any]] = []
        for row in rows:
            # Всегда добавляем человеко-читаемое название с приоритетом
            title = None
            b24_title = row.get("b24_title")
            b24_labels = row.get("b24_labels")
            b24_field = row.get("b24_field")
            col_name = row.get("column_name")
            
            # 1. Пробуем b24_title (основной источник) с нормализацией
            if b24_title:
                title = normalize_string(b24_title)
            
            # 2. Пробуем из labels (разные варианты) с нормализацией
            if not title and b24_labels:
                if isinstance(b24_labels, dict):
                    title_raw = (b24_labels.get("title") or 
                            b24_labels.get("label") or 
                            b24_labels.get("listLabel") or
                            b24_labels.get("formLabel") or
                            b24_labels.get("filterLabel"))
                    if title_raw:
                        title = normalize_string(title_raw)
                elif isinstance(b24_labels, str):
                    # Если labels - строка, пробуем распарсить как JSON
                    try:
                        import json
                        labels_dict = json.loads(b24_labels) if isinstance(b24_labels, str) else b24_labels
                        if isinstance(labels_dict, dict):
                            title_raw = (labels_dict.get("title") or 
                                    labels_dict.get("label") or 
                                    labels_dict.get("listLabel") or
                                    labels_dict.get("formLabel"))
                            if title_raw:
                                title = normalize_string(title_raw)
                    except:
                        pass
            
            # 3. Если title все еще нет, используем fallback
            if not title:
                if b24_field and (b24_field.startswith("UF_CRM_") or b24_field.startswith("ufCrm")):
                    # Для UF полей используем column_name, но делаем более читаемым
                    if col_name:
                        readable = col_name.replace("ufcrm", "").replace("_", " ").strip()
                        if readable:
                            title = readable.title()
                        else:
                            title = b24_field
                    else:
                        title = b24_field
                else:
                    title = b24_field or col_name or "Неизвестное поле"
            
            # Возвращаем только человеко-читаемую информацию
            field_data = {
                "title": title,
                "name": title,  # Дубликат title для удобства
                "type": row.get("b24_type"),  # Тип поля (для информации)
                "is_multiple": bool(row.get("is_multiple", False)),
                "is_required": bool(row.get("is_required", False)),
                "is_readonly": bool(row.get("is_readonly", False)),
            }
            
            # Добавляем labels и settings только если они есть (опционально)
            if row.get("b24_labels"):
                field_data["labels"] = row.get("b24_labels")
            
            if row.get("settings"):
                field_data["settings"] = row.get("settings")
            
            fields.append(field_data)
        
        return {
            "ok": True,
            "entity_key": final_entity_key,
            "type": type,
            "fields_count": len(fields),
            "fields": fields
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
