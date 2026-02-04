import os
import sys
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query

import psycopg2
import psycopg2.extras

# Импортируем функции из api_data.py (избегаем циклического импорта)
from api_data import pg_conn

router = APIRouter(prefix="/api/processes-deals", tags=["processes-deals"])


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
        # Если строка содержит типичные кракозябры, пробуем исправить
        try:
            # Пробуем перекодировать через latin-1 -> UTF-8
            # Это исправляет случаи, когда UTF-8 был прочитан как latin-1
            check_str = value[:100] if len(value) > 100 else value
            if any(ord(c) > 127 for c in check_str):
                # Пробуем исправить double-encoding
                fixed = value.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore')
                # Если исправленная версия выглядит лучше (меньше нечитаемых символов)
                if len([c for c in fixed if ord(c) > 127 and c not in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюяĂăÂâÎîȘșȚț']) < len([c for c in value if ord(c) > 127 and c not in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюяĂăÂâÎîȘșȚț']):
                    return fixed
        except:
            pass
        return value
    
    return str(value)


def sanitize_ident(name: str) -> str:
    """Безопасно экранирует идентификатор для PostgreSQL"""
    return name.replace('"', '""')


def table_name_for_entity(entity_key: str) -> str:
    """Возвращает имя таблицы для entity_key"""
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


@router.get("/")
def get_processes_and_deals() -> Dict[str, Any]:
    """
    Возвращает список всех доступных типов сущностей (смарт-процессы, сделки, контакты и лиды).
    Используется фронтендом для отображения списка сущностей для выбора.
    """
    conn = pg_conn()
    try:
        entities: List[Dict[str, Any]] = []
        
        # Получаем список всех смарт-процессов из b24_meta_entities
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Получаем все смарт-процессы из мета-таблицы
            cur.execute("""
                SELECT 
                    entity_key,
                    entity_type_id,
                    title
                FROM b24_meta_entities
                WHERE entity_kind = 'smart_process'
                ORDER BY entity_type_id
            """)
            smart_rows = cur.fetchall()
            
            # Для каждого смарт-процесса получаем количество записей
            for row in smart_rows:
                entity_key = row.get("entity_key")
                entity_type_id = row.get("entity_type_id")
                title_raw = row.get("title")
                
                # Нормализуем title с исправлением кодировки
                if title_raw is None:
                    title = f"SmartProcess {entity_type_id}"
                else:
                    title = normalize_string(title_raw)
                
                # Получаем имя таблицы
                table_name = table_name_for_entity(entity_key)
                
                # Подсчитываем количество записей в таблице
                total = 0
                try:
                    cur.execute(f"""
                        SELECT COUNT(*) as cnt
                        FROM {table_name}
                    """)
                    count_row = cur.fetchone()
                    if count_row:
                        total = int(count_row.get("cnt", 0) or 0)
                except Exception as e:
                    # Если таблица не существует или ошибка - пропускаем
                    print(f"WARNING: Could not count records in {table_name}: {e}", file=sys.stderr, flush=True)
                    continue
                
                entities.append({
                    "type": "smart_process",
                    "entity_key": entity_key or f"sp:{entity_type_id}",
                    "entity_type_id": entity_type_id,
                    "title": title,
                    "total": total
                })
            
            # Добавляем сделки, контакты, лиды (только type, title, total)
            for etype, ekey, etitle in [
                ("deal", "deal", "CRM Deal"),
                ("contact", "contact", "CRM Contact"),
                ("lead", "lead", "CRM Lead"),
            ]:
                total = 0
                try:
                    tbl = table_name_for_entity(ekey)
                    cur.execute(f"SELECT COUNT(*) as cnt FROM {tbl}")
                    row = cur.fetchone()
                    if row:
                        total = int(row.get("cnt", 0) or 0)
                except Exception:
                    pass
                entities.append({
                    "type": etype,
                    "entity_key": ekey,
                    "entity_type_id": None,
                    "title": etitle,
                    "total": total
                })
        
        return {
            "ok": True,
            "entities": entities
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
