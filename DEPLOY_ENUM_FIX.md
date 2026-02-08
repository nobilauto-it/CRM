# Деплой: расшифровка списочных полей (Transmisie, Tracțiune, Filiala) для смарт-процессов

## Что сделано

- Синк enum для смарт-процессов теперь **всегда** пробует загрузить варианты списка через `crm.status.entity.items` по `entityId` из `settings` поля, если в самом поле нет `items`/`options`. Раньше это делалось только для полей с типом list/enum — из‑за этого Transmisie, Tracțiune, Filiala не подтягивались.
- Добавлено логирование в stderr: по каким полям и с каким `entityId` вызывается `crm.status.entity.items` и сколько значений получено.

---

## 1. Файлы для замены на сервере

Заменить **два файла** (скопировать из репозитория поверх существующих):

| Файл | Назначение |
|------|------------|
| **`app.py`** | Синк enum: попытка `entityId` для всех полей смарт-процесса при отсутствии items, логи, альтернативные ключи `listEntityId` |
| **`entity_meta_data_api.py`** | Без изменений логики для этого фикса (уже были правки ранее: загрузка всех enum для sp, debug-enum). При деплое лучше подменить одной версией. |

Путь на сервере — тот же, откуда запускается приложение (например `/home/.../CRM/` или `d:\Projects\Git\CRM\`).

---

## 2. Копирование на сервер

### Вариант A: с локальной машины (Git уже на сервере)

На сервере:

```bash
cd /path/to/CRM   # каталог проекта на сервере
git fetch
git checkout main -- app.py entity_meta_data_api.py
# или указать нужную ветку вместо main
```

### Вариант B: копирование файлов вручную

- Скопировать `app.py` и `entity_meta_data_api.py` из репозитория в каталог приложения на сервере, заменив старые версии.

---

## 3. Перезапуск приложения

Выполнять **на сервере** из каталога проекта (или с указанием полного пути к приложению).

### Если приложение запущено через systemd

```bash
sudo systemctl restart crm
# или как у вас называется сервис, например:
# sudo systemctl restart uvicorn
# sudo systemctl restart gunicorn
```

Имя сервиса можно посмотреть:

```bash
sudo systemctl list-units --type=service | grep -E 'crm|uvicorn|gunicorn'
```

### Если через Docker / docker-compose

```bash
cd /path/to/CRM
docker-compose restart
# или пересобрать и запустить:
# docker-compose up -d --build
```

### Если через supervisor

```bash
sudo supervisorctl restart crm
# или имя программы из конфига supervisor
```

### Если процесс запущен вручную (uvicorn/gunicorn)

Найти процесс и перезапустить:

```bash
# Найти PID (пример для uvicorn)
ps aux | grep uvicorn

# Убить процесс (подставить свой PID)
kill <PID>

# Запустить снова из каталога проекта, например:
cd /path/to/CRM
uvicorn app:app --host 0.0.0.0 --port 7070
# или с nohup для фона:
# nohup uvicorn app:app --host 0.0.0.0 --port 7070 &
```

Порт и хост подставьте свои (у вас был 7070).

---

## 4. После перезапуска: запустить синк справочников

Чтобы подтянуть enum для Transmisie, Tracțiune, Filiala и остальных полей:

```bash
curl -X POST "http://194.33.40.197:7070/sync/reference-data"
```

Или из браузера/Postman: **POST** `http://194.33.40.197:7070/sync/reference-data`.

В логах приложения (journal, docker logs или вывод uvicorn) должны появиться строки вида:

- `INFO: sync_field_enums(sp:1114): ufCrm34_1748348015 <- crm.status.entity.items entityId=... (N values)`
- и аналогично для других полей.

После успешного синка проверка:

```bash
curl -s "http://194.33.40.197:7070/api/entity-meta-data/debug-enum?entity_key=sp:1114"
```

В ответе в `enum_sample` должны появиться записи с `b24_field`: `ufCrm34_1748348015` (Transmisie), `ufCrm34_1748431272` (Tracțiune), плюс Filiala — и в гриде вместо ID будут отображаться названия.

---

## 5. Краткий чеклист

1. Заменить на сервере: `app.py`, `entity_meta_data_api.py`.
2. Перезапустить приложение (systemd / docker / supervisor / вручную).
3. Вызвать **POST** `/sync/reference-data`.
4. Проверить **GET** `/api/entity-meta-data/debug-enum?entity_key=sp:1114` и обновить грид в клиенте.

Если после синка в debug-enum по-прежнему нет записей для Transmisie/Tracțiune/Filiala — прислать фрагмент логов с вызовами `sync_field_enums(sp:1114)` и при необходимости ответ `crm.item.fields` для entityTypeId 1114 по полям ufCrm34_1748348015, ufCrm34_1748431272 и полю Filiala.
