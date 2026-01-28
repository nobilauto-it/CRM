#!/bin/bash
# Скрипт для проверки логов и диагностики проблем с PDF

echo "=== Проверка статуса сервиса ==="
sudo systemctl status bitrix-sync.service --no-pager -l

echo ""
echo "=== Последние 100 строк логов (с ошибками) ==="
sudo journalctl -u bitrix-sync.service -n 100 --no-pager | grep -i "error\|exception\|traceback\|failed\|weasyprint\|reportlab\|pdf\|telegram" || echo "Нет ошибок в последних 100 строках"

echo ""
echo "=== Последние 50 строк всех логов ==="
sudo journalctl -u bitrix-sync.service -n 50 --no-pager

echo ""
echo "=== Логи за последний час ==="
sudo journalctl -u bitrix-sync.service --since "1 hour ago" --no-pager | tail -50

echo ""
echo "=== Проверка процесса ==="
ps aux | grep -E "uvicorn|gunicorn|python.*app.py" | grep -v grep || echo "Процесс не найден"

echo ""
echo "=== Проверка порта 7070 ==="
netstat -tulpn | grep 7070 || ss -tulpn | grep 7070 || echo "Порт 7070 не слушается"
