# Big Pump - Руководство Пользователя

**Версия:** 2.0 (Обновлено 28.11.2025)  
**Система:** Анализ торговых сигналов для крипто-рынка (Binance/Bybit)

---

## 📋 Содержание

1. [Обзор Системы](#обзор-системы)
2. [Быстрый Старт](#быстрый-старт)
3. [Развертывание](#развертывание)
4. [Описание Компонентов](#описание-компонентов)
5. [Ежедневные Операции](#ежедневные-операции)
6. [Обслуживание](#обслуживание)
7. [Решение Проблем](#решение-проблем)

---

## Обзор Системы

### Назначение
Система автоматически обнаруживает торговые сигналы на крипто-парах, анализирует их качество и отправляет уведомления в Telegram.

### Архитектура

```
┌─────────────────────┐
│ fas_v2.             │  Основная БД сигналов
│ scoring_history     │  и паттернов
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌──────────────────┐
│ populate_signal     │────▶│ web.signal       │  
│ _analysis.py        │     │ _analysis        │  Обработанные сигналы
└─────────────────────┘     └────────┬─────────┘
                                     │
           ┌─────────────────────────┼─────────────────────┐
           │                         │                     │
           ▼                         ▼                     ▼
    ┌────────────┐         ┌─────────────────┐    ┌──────────────┐
    │ pump       │         │ high_score      │    │ Telegram     │
    │ _scanner   │ Cron 4x │ _signal_server  │    │ Bot          │
    │ .py        │  /hour  │ .py (WebSocket) │    │ Alerts       │
    └────────────┘         └─────────────────┘    └──────────────┘
```

### Ключевые Возможности
- ✅ Обнаружение сигналов с паттернами SQUEEZE_IGNITION, OI_EXPLOSION
- ✅ Real-time уведомления через WebSocket
- ✅ Telegram alerts каждые 15 минут
- ✅ Дедупликация сигналов (12h cooldown)
- ✅ Поддержка Binance и Bybit
- ✅ Backtesting и оптимизация параметров

---

## Быстрый Старт

### Первый Запуск (Локально)

```bash
# 1. Клонировать репозиторий
cd ~/PycharmProjects
git clone <repo_url> big_pump

# 2. Создать виртуальное окружение
cd big_pump
python3 -m venv .venv
source .venv/bin/activate

# 3. Установить зависимости
pip install -r requirements.txt

# 4. Настроить .env
cp .env.example .env
nano .env    # Указать DB credentials, Telegram token

# 5. Проверить подключение к БД
python3 -c "from config.settings import DATABASE; print(DATABASE)"

# 6. Запустить тест
python3 scripts_v2/populate_signal_analysis.py --days 1 --cooldown 12
```

### Первый Запуск (Сервер)

```bash
# SSH на сервер
ssh elcrypto@foxcrypto

# Перейти в проект
cd ~/big_pump
source .venv/bin/activate

# Проверить cron
crontab -l | grep scanner

# Ожидаемый вывод:
# 2,17,32,47 * * * * /home/elcrypto/big_pump/scripts_v2/run_scanner_cron.sh

# Проверить WebSocket service
sudo systemctl status high-score-signal-websocket.service

# Запустить вручную (тест)
python3 scripts_v2/pump_scanner.py
```

---

## Развертывание

### Требования
- **OS:** Ubuntu 20.04+ / macOS
- **Python:** 3.10+
- **PostgreSQL:** 12+
- **RAM:** 2GB+ (для backtesting 4GB+)

### Установка на Новый Сервер

#### 1. Подготовка Системы
```bash
# Обновить пакеты
sudo apt update && sudo apt upgrade -y

# Установить зависимости
sudo apt install -y python3 python3-pip python3-venv postgresql-client git

# Создать пользователя (если нужно)
sudo adduser elcrypto
sudo usermod -aG sudo elcrypto
```

#### 2. Развертывание Проекта
```bash
# От имени пользователя elcrypto
cd ~
git clone <repo_url> big_pump
cd big_pump

# Создать venv
python3 -m venv .venv
source .venv/bin/activate

# Установить пакеты
pip install asyncpg psycopg[binary] requests python-dotenv

# Настроить .env
cp .env.example .env
nano .env
```

**Пример .env:**
```bash
DB_NAME=fox_crypto_new
DB_USER=elcrypto
DB_PASSWORD=your_secure_password
DB_HOST=localhost
DB_PORT=5433

TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

#### 3. База Данных
```bash
# Применить миграции
psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new < migrations/001_create_signal_analysis.sql
psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new < migrations/002_create_minute_candles.sql

# Проверить таблицы
psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -c "\dt web.*"
```

#### 4. Настройка Cron
```bash
# Редактировать crontab
crontab -e

# Добавить строку:
2,17,32,47 * * * * /home/elcrypto/big_pump/scripts_v2/run_scanner_cron.sh >> /home/elcrypto/big_pump/logs/scanner_cron.log 2>&1

# Создать директорию для логов
mkdir -p ~/big_pump/logs
```

#### 5. WebSocket Сервис (systemd)
```bash
# Создать service файл
sudo nano /etc/systemd/system/high-score-signal-websocket.service
```

**Содержимое:**
```ini
[Unit]
Description=High Score Signal WebSocket Server
After=network.target postgresql.service

[Service]
Type=simple
User=elcrypto
WorkingDirectory=/home/elcrypto/big_pump
Environment="PATH=/home/elcrypto/big_pump/.venv/bin"
ExecStart=/home/elcrypto/big_pump/.venv/bin/python3 /home/elcrypto/big_pump/scripts_v2/high_score_signal_server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Активировать сервис
sudo systemctl daemon-reload
sudo systemctl enable high-score-signal-websocket.service
sudo systemctl start high-score-signal-websocket.service

# Проверить статус
sudo systemctl status high-score-signal-websocket.service
```

---

## Описание Компонентов

### Основные Скрипты

#### `pump_analysis_lib.py` (Библиотека)
**Назначение:** Центральная библиотека с общими функциями

**Ключевые функции:**
- `fetch_signals(conn, days=30)` - Выборка сигналов из БД через sh_patterns
- `deduplicate_signals(signals, cooldown_hours=12)` - Удаление дубликатов
- `get_entry_price_and_candles(conn, signal)` - Получение цены входа через API

**Константы:**
- `SCORE_THRESHOLD = 250` - Минимальный score для сигнала
- `TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']`
- `EXCHANGE_FILTER = 'BINANCE'` - Фильтр биржи (ALL/BINANCE/BYBIT)

**Критическое исправление (28.11.2025):**  
Исправлена логика JOIN паттернов - теперь использует sh_patterns вместо time-based JOIN.

---

#### `populate_signal_analysis.py` (Обработчик)
**Назначение:** Заполнение таблицы web.signal_analysis

**Параметры:**
```bash
python3 populate_signal_analysis.py [options]

--days DAYS           Загрузить сигналы за N дней (default: 30)
--limit LIMIT         Ограничить количество сигналов
--cooldown HOURS      Кулдаун дедупликации в часах (default: 12)
--force               Пересоздать все (удалить существующие)
```

**Процесс:**
1. Fetch signals через pump_analysis_lib
2. Дедупликация (12h cooldown)
3. Проверка существующих в web.signal_analysis  
4. Получение entry_price через API (Binance/Bybit)
5. Загрузка 1-min candles для анализа
6. Сохранение в БД

**Пример:**
```bash
# Обновить за последние 7 дней
python3 scripts_v2/populate_signal_analysis.py --days 7 --cooldown 12

# Полное пересоздание за 30 дней
python3 scripts_v2/populate_signal_analysis.py --days 30 --force
```

---

#### `pump_scanner.py` (Cron Scanner)
**Назначение:** Главный entry point для cron, отправка Telegram alerts

**Логика:**
1. Вызывает `populate_signal_analysis(days=0.042)` (1 час)
2. Получает список новых сигналов
3. Отправляет Telegram уведомления
4. Логирует результаты

**Запуск:** Автоматически через `run_scanner_cron.sh` каждые 15 минут

**Ручной запуск:**
```bash
python3 scripts_v2/pump_scanner.py
```

---

#### `high_score_signal_server.py` (WebSocket)
**Назначение:** Real-time broadcasting сигналов по WebSocket

**Функции:**
- LISTEN/NOTIFY PostgreSQL для real-time событий
- Authentication (password-protected)
- Deduplication с 12h cooldown
- Polling fallback (если NOTIFY не работает)

**Запуск:**
```bash
# Через systemd
sudo systemctl start high-score-signal-websocket.service

# Вручную (для отладки)
python3 scripts_v2/high_score_signal_server.py
```

**Подключение клиента:**
```bash
python3 scripts_v2/test_ws_client.py
```

---

### Анализ и Отчеты

#### `backtest_portfolio_realistic.py`
**Назначение:** Реалистичный backtest портфеля

**Учитывает:**
- Комиссии (0.05% вход + выход)
- Slippage (0.1%)
- Ликвидации (при -8% на 10x плече)
- Фандинг
- Trailing Stop логика

**Пример:**
```bash
python3 scripts_v2/backtest_portfolio_realistic.py \
    --sl -5 \
    --activation 15 \
    --callback 2 \
    --timeout 12
```

---

#### `optimize_advanced.py`
**Назначение:** Поиск оптимальных параметров SL/TS

**Процесс:**
- Grid search по параметрам
- Расчёт Sharpe ratio, Win Rate, PnL
- Вывод TOP 3 конфигураций

**Пример:**
```bash
python3 scripts_v2/optimize_advanced.py
```

---

#### `report_signals_30d.py`
**Назначение:** Отчёт по сигналам за 30 дней

**Метрики:**
- Win Rate (% сигналов с ростом >5%, >10%)
- Средний рост и просадка
- Время до пика

**Пример:**
```bash
python3 scripts_v2/report_signals_30d.py --days 30
```

---

### Утилиты

#### `fetch_minute_candles.py`
**Назначение:** Загрузка 1-минутных свечей с Binance

**Пример:**
```bash
python3 scripts_v2/fetch_minute_candles.py --days 30
```

---

## Ежедневные Операции

### Утренний Чек-лист

```bash
# 1. Проверить статус WebSocket
sudo systemctl status high-score-signal-websocket.service

# 2. Проверить cron логи
tail -n 50 ~/big_pump/logs/scanner_cron.log

# 3. Обновить сигналы (при необходимости)
cd ~/big_pump
source .venv/bin/activate
python3 scripts_v2/populate_signal_analysis.py --days 1 --cooldown 12

# 4. Проверить отчёт за 24h
python3 scripts_v2/report_detailed_24h.py
```

### Мониторинг

**Логи Cron:**
```bash
tail -f ~/big_pump/logs/scanner_cron.log
```

**Логи WebSocket:**
```bash
sudo journalctl -u high-score-signal-websocket.service -f
```

**Проверка последних сигналов:**
```bash
psql -d fox_crypto_new -c "SELECT pair_symbol, signal_timestamp, total_score FROM web.signal_analysis ORDER BY created_at DESC LIMIT 10"
```

---

## Обслуживание

### Еженедельное

**1. Пересоздание сигналов за 7 дней:**
```bash
python3 scripts_v2/populate_signal_analysis.py --days 7 --force --cooldown 12
```

**2. Проверка пропущенных сигналов:**
```bash
python3 scripts_v2/audit_test_tradoor.py
```

### Ежемесячное

**1. Оптимизация параметров:**
```bash
python3 scripts_v2/optimize_advanced.py
```

**2. Полный backtest:**
```bash
python3 scripts_v2/backtest_portfolio_realistic.py --sl -5 --activation 15 --callback 2 --timeout 12
```

**3. Отчёты:**
```bash
python3 scripts_v2/report_signals_30d.py --days 30
```

### Обновление Кода

```bash
# На сервере
cd ~/big_pump
git pull origin main

# Перезапустить сервисы
sudo systemctl restart high-score-signal-websocket.service

# Проверить
sudo systemctl status high-score-signal-websocket.service
```

---

## Решение Проблем

### WebSocket не запускается

**Проблема:** `sudo systemctl status high-score-signal-websocket.service` показывает failed

**Решение:**
```bash
# Проверить логи
sudo journalctl -u high-score-signal-websocket.service -n 50

# Частые причины:
# 1. Порт 8765 занят
sudo lsof -i :8765

# 2. Неправильный путь к venv
# Проверить в /etc/systemd/system/high-score-signal-websocket.service

# 3. Нет доступа к БД
# Проверить .env credentials
```

### Cron не запускается

**Проблема:** Нет Telegram alerts

**Решение:**
```bash
# Проверить cron
crontab -l | grep scanner

# Проверить логи
tail -n 100 ~/big_pump/logs/scanner_cron.log

# Запустить вручную
cd ~/big_pump
source .venv/bin/activate
python3 scripts_v2/pump_scanner.py

# Проверить права на файлы
chmod +x scripts_v2/run_scanner_cron.sh
```

### Сигналы не появляются

**Проблема:** `populate_signal_analysis` возвращает 0 signals

**Решение:**
```bash
# Проверить настройки фильтров
cat scripts_v2/pump_analysis_lib.py | grep "EXCHANGE_FILTER\|SCORE_THRESHOLD"

# Проверить сырые сигналы в БД
psql -d fox_crypto_new -c "
SELECT COUNT(*) FROM fas_v2.scoring_history 
WHERE total_score > 250 
AND timestamp >= NOW() - INTERVAL '1 day'
"

# Проверить паттерны
psql -d fox_crypto_new -c "
SELECT sp.pattern_type, COUNT(*) 
FROM fas_v2.signal_patterns sp
WHERE sp.pattern_type IN ('SQUEEZE_IGNITION', 'OI_EXPLOSION')
GROUP BY sp.pattern_type
"
```

### Ошибка подключения к БД

**Проблема:** `psycopg.OperationalError: connection failed`

**Решение:**
```bash
# Проверить .env
cat .env | grep DB_

# Проверить .pgpass (если используется)
cat ~/.pgpass

# Тест подключения
psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -c "SELECT 1"

# Проверить PostgreSQL
sudo systemctl status postgresql
```

---

## Приложение A: Файловая Структура

```
big_pump/
├── .env                    # Конфигурация (НЕ коммитить!)
├── .gitignore
├── USER_MANUAL_RU.md       # Это руководство
├── FILE_INVENTORY.md       # Инвентаризация файлов
│
├── config/
│   └── settings.py         # Настройки системы
│
├── migrations/
│   ├── 001_create_signal_analysis.sql
│   └── 002_create_minute_candles.sql
│
├── scripts_v2/             # Рабочие скрипты
│   ├── pump_analysis_lib.py          # Библиотека
│   ├── populate_signal_analysis.py   # Обработчик
│   ├── pump_scanner.py               # Cron scanner
│   ├── high_score_signal_server.py   # WebSocket
│   ├── backtest_*.py                 # Backtesting
│   ├── optimize_advanced.py          # Оптимизация
│   ├── report_*.py                   # Отчёты
│   ├── fetch_minute_candles.py       # Загрузка свечей
│   ├── run_scanner_cron.sh           # Cron wrapper
│   ├── audit_test_*.py               # Audit скрипты
│   └── test_ws_client.py             # WebSocket клиент
│
└── archive/                # Старые версии (не использовать!)
    ├── scripts_legacy/
    └── unused/
```

---

## Приложение B: База Данных

### Таблицы

**fas_v2.scoring_history**
- Сырые сигналы
- Поля: id, trading_pair_id, timestamp, total_score, created_at

**fas_v2.signal_patterns**
- Обнаруженные паттерны
- Поля: id, trading_pair_id, pattern_type, timeframe, timestamp

**fas_v2.sh_patterns**
- Связь сигналов и паттернов (CRITICAL!)
- Поля: scoring_history_id, signal_patterns_id

**web.signal_analysis**
- Обработанные сигналы
- Поля: pair_symbol, signal_timestamp, total_score, entry_price, max_gain_15m, created_at

**public.trading_pairs**
- Торговые пары
- Поля: id, pair_symbol, exchange_id, contract_type_id, is_active

---

## Приложение C: Контакты и Поддержка

**Разработчик:** Evgeniy Yanvarskiy  
**Системные логи:** `~/big_pump/logs/`  
**Audit отчёты:** Artifacts в `/.gemini/`

**Полезные ссылки:**
- FILE_INVENTORY.md - Полный список файлов
- AUDIT_REPORT.md - Результаты аудита
- implementation_plan.md - Планы исправлений

---

**Версия документа:** 2.0  
**Последнее обновление:** 28.11.2025  
**Статус:** ✅ Актуально
