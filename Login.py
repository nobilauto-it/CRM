"""
API входа: проверка Username/Password по таблице crm_users.
Таблица crm_users создаётся при вызове POST /sync/schema (ensure_meta_tables в app.py).
"""
import os
from typing import Any, Dict

import psycopg2
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Те же переменные окружения, что и в app.py
PG_HOST = os.getenv("PG_HOST", "194.33.40.197")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crm")
PG_USER = os.getenv("PG_USER", "crm")
PG_PASS = os.getenv("PG_PASS", "crm")

router = APIRouter(prefix="/api", tags=["login"])


def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


class LoginBody(BaseModel):
    Username: str
    Password: str


@router.post("/login")
def login(body: LoginBody) -> Dict[str, Any]:
    """
    Принимает JSON с полями Username и Password.
    Проверяет наличие пользователя в таблице crm_users (совпадение по username и password).
    Возвращает success: true и user_id при успехе, иначе 401.
    """
    username = (body.Username or "").strip()
    password = (body.Password or "").strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and Password are required")

    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM crm_users WHERE username = %s AND password = %s",
                (username, password),
            )
            row = cur.fetchone()
        if row:
            return {"success": True, "user_id": row[0]}
        raise HTTPException(status_code=401, detail="Invalid username or password")
    finally:
        conn.close()
