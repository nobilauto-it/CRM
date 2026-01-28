#!/usr/bin/env python3
"""
Скрипт для отправки 6 отчетов в Telegram.
Использование: python send_reports_telegram.py
"""

import os
import requests
import json
from typing import Dict, Any

# Конфигурация
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Маппинг филиалов
FILIALA_MAP = {
    1668: "Centru",
    1666: "Buiucani",
    1670: "Ungheni",
    1672: "Comrat",
    1674: "Cahul",
    1676: "Mezon"
}


def get_report(filiala_id: int) -> Dict[str, Any]:
    """Получить отчет для филиала"""
    url = f"{API_BASE_URL}/api/data/reports/automobile"
    params = {"filiala_id": filiala_id}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Ошибка при получении отчета для филиала {filiala_id}: {e}")
        return None


def format_report_message(report_data: Dict[str, Any]) -> str:
    """Форматировать отчет в сообщение для Telegram"""
    if not report_data or not report_data.get("ok"):
        return f"❌ Ошибка получения отчета"
    
    filiala_name = report_data.get("filiala_name", "Unknown")
    count = report_data.get("count", 0)
    data = report_data.get("data", [])
    
    message = f"📊 <b>Automobile date, {filiala_name}</b>\n\n"
    message += f"Всего записей: {count}\n\n"
    
    if not data:
        message += "Нет данных для отображения"
        return message
    
    # Заголовки таблицы
    message += "<pre>"
    message += f"{'Номер сделки':<15} {'Responsabil':<15} {'Numar auto':<12} {'Marca':<12} {'Model':<12} {'Zile':<6} {'Total':<10}\n"
    message += "-" * 90 + "\n"
    
    # Данные (первые 20 строк для читаемости)
    for item in data[:20]:
        deal_id = str(item.get('deal_id', ''))[:10]
        responsabil = str(item.get('responsabil', ''))[:13]
        numar_auto = str(item.get('numar_auto', 'N/A'))[:10]
        marca = str(item.get('marca', 'N/A'))[:10]
        model = str(item.get('model', 'N/A'))[:10]
        zile = str(item.get('zile', 'N/A'))[:4]
        total = str(item.get('total_suma', 0))[:8]
        
        message += f"{deal_id:<15} {responsabil:<15} {numar_auto:<12} {marca:<12} {model:<12} {zile:<6} {total:<10}\n"
    
    if len(data) > 20:
        message += f"\n... и еще {len(data) - 20} записей\n"
    
    # Итоговая сумма
    total_sum = sum(float(item.get('total_suma', 0) or 0) for item in data)
    message += "-" * 90 + "\n"
    message += f"{'Итого:':<75} {total_sum:<10.2f}\n"
    message += "</pre>"
    
    return message


def send_telegram_message(message: str) -> bool:
    """Отправить сообщение в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не установлены")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")
        return False


def main():
    """Основная функция"""
    print("🚀 Начинаем отправку отчетов в Telegram...")
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Ошибка: Установите переменные окружения:")
        print("   export TELEGRAM_BOT_TOKEN='your_bot_token'")
        print("   export TELEGRAM_CHAT_ID='your_chat_id'")
        return
    
    success_count = 0
    error_count = 0
    
    # Отправляем отчеты для всех 6 филиалов
    for filiala_id, filiala_name in FILIALA_MAP.items():
        print(f"📊 Получаем отчет для {filiala_name} (ID: {filiala_id})...")
        
        report_data = get_report(filiala_id)
        
        if report_data:
            message = format_report_message(report_data)
            if send_telegram_message(message):
                print(f"✅ Отчет для {filiala_name} отправлен")
                success_count += 1
            else:
                print(f"❌ Ошибка отправки отчета для {filiala_name}")
                error_count += 1
        else:
            print(f"❌ Не удалось получить отчет для {filiala_name}")
            error_count += 1
    
    print(f"\n📈 Итого: {success_count} успешно, {error_count} ошибок")


if __name__ == "__main__":
    main()










