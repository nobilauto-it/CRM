import os
import sys
import threading
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query

import psycopg2
import psycopg2.extras

# Импортируем функции из api_data.py (избегаем циклического импорта)
from api_data import pg_conn

router = APIRouter(prefix="/api/entity-data", tags=["entity-data"])


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


def normalize_nested_data(value: Any) -> Any:
    """
    Рекурсивно нормализует строки в словарях и списках.
    """
    if isinstance(value, dict):
        return {k: normalize_nested_data(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [normalize_nested_data(item) for item in value]
    elif isinstance(value, str):
        return normalize_string(value)
    elif isinstance(value, bytes):
        return normalize_string(value)
    else:
        return value


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


@router.get("/")
def get_entity_data(
    type: str = Query(..., description="Тип сущности: 'smart_process', 'deal', 'contact' или 'lead'"),
    entity_key: Optional[str] = Query(None, description="Ключ сущности (обязателен для smart_process): например 'sp:1114'"),
    limit: int = Query(100, ge=1, le=10000, description="Максимальное количество записей"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    fields: Optional[str] = Query(None, description="Список полей через запятую (если не указано - все поля)"),
) -> Dict[str, Any]:
    """
    Возвращает данные (значения) по полям для выбранной сущности.
    
    Параметры:
    - type: тип сущности ("smart_process", "deal", "contact" или "lead")
    - entity_key: ключ сущности (обязателен для smart_process, для остальных не нужен)
    - limit: максимальное количество записей (по умолчанию 100, максимум 10000)
    - offset: смещение для пагинации (по умолчанию 0)
    - fields: список полей через запятую для выборки (если не указано - все поля)
    
    Возвращает данные из соответствующей таблицы с указанными полями.
    
    Примеры вызовов:
    1. Все сделки (первые 100 записей, все поля):
       GET /api/entity-data/?type=deal
    
    2. Все контакты (первые 100 записей, все поля):
       GET /api/entity-data/?type=contact
    
    3. Все лиды (первые 100 записей, все поля):
       GET /api/entity-data/?type=lead
    
    4. Сделки с пагинацией (следующие 100 записей):
       GET /api/entity-data/?type=deal&limit=100&offset=100
    
    5. Смарт-процесс STOCK AUTO (все поля, первые 100 записей):
       GET /api/entity-data/?type=smart_process&entity_key=sp:1114
    
    6. Сделки с определенными полями:
       GET /api/entity-data/?type=deal&fields=id,title,opportunity,assigned_by_id
    """
    # Быстрая синхронизация перед возвратом данных (чтобы данные были свежими)
    # Запускаем в фоне, чтобы не блокировать ответ API
    import threading
    from app import sync_data, AUTO_SYNC_DEAL_LIMIT, AUTO_SYNC_SMART_LIMIT, AUTO_SYNC_CONTACT_LIMIT, AUTO_SYNC_LEAD_LIMIT
    
    def quick_sync_background():
        """Быстрая синхронизация в фоне (1-2 минуты) для обновления данных"""
        try:
            print(f"INFO: get_entity_data: Starting quick sync for fresh data (type={type})", file=sys.stderr, flush=True)
            # Быстрая синхронизация с ограниченным временем (90 секунд)
            sync_data(
                deal_limit=AUTO_SYNC_DEAL_LIMIT,
                smart_limit=AUTO_SYNC_SMART_LIMIT,
                contact_limit=AUTO_SYNC_CONTACT_LIMIT,
                lead_limit=AUTO_SYNC_LEAD_LIMIT,
                time_budget_sec=90  # 1.5 минуты на быструю синхронизацию
            )
            print(f"INFO: get_entity_data: Quick sync completed for type={type}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: get_entity_data: Quick sync failed (non-critical): {e}", file=sys.stderr, flush=True)
    
    # Запускаем быструю синхронизацию в отдельном потоке (не блокирует ответ)
    sync_thread = threading.Thread(target=quick_sync_background, daemon=True)
    sync_thread.start()
    
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
        
        # Получаем имя таблицы
        table_name = table_name_for_entity(final_entity_key)
        
        # Получаем список ВСЕХ колонок из таблицы
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            all_columns = [row[0] for row in cur.fetchall()]
        
        if not all_columns:
            return {
                "ok": True,
                "entity_key": final_entity_key,
                "type": type,
                "table_name": table_name,
                "total": 0,
                "count": 0,
                "fields": [],
                "data": []
            }
        
        # Получаем мета-информацию о полях (title, b24_field, column_name)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    b24_field,
                    column_name,
                    b24_title,
                    b24_type,
                    b24_labels,
                    settings
                FROM b24_meta_fields
                WHERE entity_key = %s
            """, (final_entity_key,))
            meta_fields = cur.fetchall()
        
        # Создаем маппинг column_name -> {b24_field, title, b24_type}
        # И также маппинг b24_field -> title для поиска по любому имени
        field_meta_map = {}
        b24_field_to_title = {}
        for meta in meta_fields:
            col_name = meta.get("column_name")
            b24_field = meta.get("b24_field")
            b24_title = meta.get("b24_title")
            b24_labels = meta.get("b24_labels")
            settings = meta.get("settings")
            
            # Пытаемся получить title из разных источников (приоритет важен!)
            title = None
            
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
            
            # 3. Пробуем из settings (для некоторых полей title может быть там) с нормализацией
            if not title and settings:
                if isinstance(settings, dict):
                    title_raw = settings.get("title") or settings.get("label")
                    if title_raw:
                        title = normalize_string(title_raw)
                elif isinstance(settings, str):
                    try:
                        import json
                        settings_dict = json.loads(settings) if isinstance(settings, str) else settings
                        if isinstance(settings_dict, dict):
                            title_raw = settings_dict.get("title") or settings_dict.get("label")
                            if title_raw:
                                title = normalize_string(title_raw)
                    except:
                        pass
            
            # 4. Если title все еще нет, создаем понятное имя из b24_field
            if not title:
                if b24_field:
                    if b24_field.startswith("UF_CRM_") or b24_field.startswith("ufCrm"):
                        if col_name:
                            readable = col_name.replace("ufcrm", "").replace("_", " ").strip()
                            if readable:
                                title = readable.title()
                            else:
                                title = b24_field
                        else:
                            title = b24_field
                    else:
                        title = b24_field
                else:
                    title = col_name or "Неизвестное поле"
            
            if col_name:
                field_meta_map[col_name] = {
                    "b24_field": b24_field,
                    "column_name": col_name,
                    "title": title,
                    "name": title,
                    "b24_type": meta.get("b24_type")
                }
            
            if b24_field:
                b24_field_to_title[b24_field] = title
        
        # Формируем список колонок для SELECT
        selected_fields = []
        if fields:
            requested_fields = [f.strip() for f in fields.split(",")]
            for req_field in requested_fields:
                if req_field in all_columns:
                    selected_fields.append(req_field)
        else:
            selected_fields = all_columns.copy()
        
        base_fields = ["id", "raw"]
        for bf in base_fields:
            if bf in all_columns and bf not in selected_fields:
                selected_fields.insert(0, bf)
        
        fields_info = []
        for col in selected_fields:
            if col in field_meta_map:
                meta = field_meta_map[col]
                fields_info.append({
                    "title": meta.get("title"),
                    "name": meta.get("name"),
                    "type": meta.get("b24_type")
                })
            else:
                base_field_titles = {
                    "id": "ID",
                    "raw": "Данные (JSON)",
                    "created_at": "Дата создания",
                    "updated_at": "Дата обновления"
                }
                title = base_field_titles.get(col, col)
                fields_info.append({
                    "title": title,
                    "name": title,
                    "type": None
                })
        
        columns_str = ", ".join([f'"{col}"' for col in selected_fields])
        
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
            total_count = cur.fetchone()[0] if cur.rowcount > 0 else 0
        
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = f"""
                SELECT {columns_str}
                FROM "{table_name}"
                ORDER BY id DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, (limit, offset))
            rows = cur.fetchall()
        
        data = []
        for row in rows:
            record = {}
            for idx, col in enumerate(selected_fields):
                value = row.get(col)
                field_info = fields_info[idx] if idx < len(fields_info) else None
                title = field_info.get("title") if field_info else col
                
                try:
                    if isinstance(value, str):
                        value = normalize_string(value)
                    elif isinstance(value, (dict, list)):
                        value = normalize_nested_data(value)
                except Exception as e:
                    print(f"WARNING: Error normalizing value for field {col}: {e}", file=sys.stderr, flush=True)
                
                record[title] = value
            data.append(record)
        
        return {
            "ok": True,
            "entity_key": final_entity_key,
            "type": type,
            "table_name": table_name,
            "total": total_count,
            "count": len(data),
            "limit": limit,
            "offset": offset,
            "fields": fields_info,
            "data": data
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
