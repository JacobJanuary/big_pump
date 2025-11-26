#!/usr/bin/env python3
"""
WebSocket сервер для трансляции высококачественных торговых сигналов
Транслирует сигналы с total_score > 250 и паттернами SQUEEZE_IGNITION, OI_EXPLOSION
Порт: 25370
"""

import asyncio
import json
import logging
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Set, Dict, Optional, List
import signal
import sys
import os
from pathlib import Path

# Add config directory to path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
config_dir = project_root / 'config'
sys.path.append(str(config_dir))

import settings
import asyncpg
import websockets

# Import unified constants
# Since we are in scripts_v2, this import works naturally
from pump_analysis_lib import (
    EXCHANGE_FILTER, 
    EXCHANGE_IDS, 
    SCORE_THRESHOLD, 
    TARGET_PATTERNS
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('high_score_signal_ws_server.log')
    ]
)
logger = logging.getLogger('HighScoreSignalWSServer')

# ... (class definition)

class HighScoreSignalWebSocketServer:
    """
    WebSocket сервер для стриминга высококачественных торговых сигналов
    Поддерживает гибридный режим работы:
    - PostgreSQL LISTEN/NOTIFY (event-driven, <10ms latency)
    - Lightweight polling (fallback, 1 sec interval)
    
    Фильтры:
    - total_score > SCORE_THRESHOLD (250)
    - Паттерны: TARGET_PATTERNS
    - Timeframes: 15m, 1h, 4h
    - contract_type_id = 1
    - exchange_id: Respects EXCHANGE_FILTER
    - Время жизни сигнала: 32 минуты (настраиваемое)
    """

    def __init__(self, config: dict):
        # Настройки сервера
        self.host = config.get('WS_SERVER_HOST', '0.0.0.0')
        self.port = int(config.get('WS_SERVER_PORT', 25370))
        self.auth_token = config.get('WS_AUTH_TOKEN')  # Хешированный токен

        # Настройки БД из settings.py (единый источник правды)
        db_settings = settings.DATABASE
        self.db_config = {
            'host': db_settings['host'],
            'port': int(db_settings['port']),
            'database': db_settings['dbname'],
            'user': db_settings['user']
        }
        
        # Only add password if explicitly provided (supports .pgpass)
        if db_settings.get('password'):
            self.db_config['password'] = db_settings['password']

        # Настройки запроса
        self.query_interval = int(config.get('QUERY_INTERVAL_SECONDS', 3))
        self.signal_window_minutes = int(config.get('SIGNAL_WINDOW_MINUTES', 30))

        # Гибридный режим: NOTIFY + Polling
        self.use_notify = config.get('USE_NOTIFY', 'true').lower() == 'true'
        self.notify_channel = config.get('NOTIFY_CHANNEL', 'new_signals')
        self.lightweight_check_interval = int(config.get('LIGHTWEIGHT_CHECK_INTERVAL', 1))
        self.notify_fallback_interval = int(config.get('NOTIFY_FALLBACK_INTERVAL', 60))

        # Параметры по умолчанию для high-score сигналов
        self.default_params = {
            'recommended_action': 'BUY',
            'score_week_filter': 100,
            'score_month_filter': 100,
            'max_trades_filter': 100,
            'stop_loss_filter': 4.0,
            'trailing_activation_filter': 48.0,
            'trailing_distance_filter': 1.0
        }

        # Состояние NOTIFY
        self.notify_available = False
        self.notify_connection: Optional[asyncpg.Connection] = None

        # Отслеживание изменений для lightweight проверок
        self.last_max_id = 0
        self.last_check_timestamp = None

        # Управление подключениями
        self.connected_clients: Set = set()
        self.authenticated_clients: Set = set()
        self.client_info: Dict = {}
        
        # Deduplication state
        self.seen_signals = {} # symbol -> timestamp
        self.dedup_cooldown_hours = 24

        # Состояние
        self.db_pool: Optional[asyncpg.Pool] = None
        self.running = False
        self.last_signals: List[dict] = []
        self.stats = {
            'queries_executed': 0,
            'signals_sent': 0,
            'errors': 0,
            'start_time': datetime.now()
        }

        logger.info(f"High-Score Signal WebSocket Server initialized on {self.host}:{self.port}")
        logger.info(f"Hybrid mode: NOTIFY={'enabled' if self.use_notify else 'disabled'}, "
                   f"Lightweight check interval={self.lightweight_check_interval}s")
        
        # Check auth status
        default_hash = hashlib.sha256(b'change_me_please').hexdigest()
        is_default = self.auth_token == default_hash
        logger.info(f"Auth Status: {'⚠️ USING DEFAULT PASSWORD' if is_default else '✅ Custom password loaded'}")
        
        logger.info(f"Filters: total_score > {SCORE_THRESHOLD}, patterns={TARGET_PATTERNS}, "
                   f"Exchange Filter: {EXCHANGE_FILTER}")

    def hash_token(self, token: str) -> str:
        """Хеширование токена для безопасного сравнения"""
        return hashlib.sha256(token.encode()).hexdigest()

    def build_signal_query(self) -> str:
        """
        Формирует SQL запрос для высококачественных сигналов
        Возвращает запрос с placeholder для signal_window_minutes
        """
        placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
        
        # Exchange filter logic
        exchange_filter_clause = ""
        if EXCHANGE_FILTER == 'BINANCE':
            exchange_filter_clause = f"AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']}"
        elif EXCHANGE_FILTER == 'BYBIT':
            exchange_filter_clause = f"AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']}"
        # If ALL, no extra clause needed (assuming we want all active exchanges)
        
        query = f"""
-- Запрос высококачественных сигналов с паттернами {TARGET_PATTERNS}
SELECT
    sh.id,
    sh.trading_pair_id,
    tp.pair_symbol,
    sh.total_score,
    sh.score_week,
    sh.score_month,
    sh.timestamp,
    sh.created_at,
    tp.exchange_id,
    tp.contract_type_id,
    
    -- Собираем информацию о паттернах
    array_agg(DISTINCT sp.pattern_type) FILTER (WHERE sp.pattern_type IS NOT NULL) as patterns,
    array_agg(DISTINCT sp.timeframe) FILTER (WHERE sp.timeframe IS NOT NULL) as timeframes
    
FROM fas_v2.scoring_history sh
JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
-- Добавляем JOIN для получения паттернов
LEFT JOIN fas_v2.sh_patterns shp ON shp.scoring_history_id = sh.id
LEFT JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id
    AND sp.pattern_type IN ({placeholders})
    AND sp.timeframe IN ('15m', '1h', '4h')

WHERE sh.total_score > {SCORE_THRESHOLD}
    AND tp.contract_type_id = 1
    AND tp.is_active = TRUE
    AND sh.is_active = TRUE
    {exchange_filter_clause}
    AND sh.timestamp >= now() - INTERVAL '%s minutes'
    -- Проверяем что есть хотя бы один нужный паттерн
    AND EXISTS (
        SELECT 1
        FROM fas_v2.sh_patterns shp2
        JOIN fas_v2.signal_patterns sp2 ON shp2.signal_patterns_id = sp2.id
        WHERE shp2.scoring_history_id = sh.id
            AND sp2.pattern_type IN ({placeholders})
            AND sp2.timeframe IN ('15m', '1h', '4h')
    )

GROUP BY
    sh.id,
    sh.trading_pair_id,
    tp.pair_symbol,
    sh.total_score,
    sh.score_week,
    sh.score_month,
    sh.timestamp,
    sh.created_at,
    tp.exchange_id,
    tp.contract_type_id

ORDER BY 
    sh.total_score DESC,
    sh.timestamp DESC;
"""
        return query

    async def init_db(self):
        """Инициализация пула соединений с БД"""
        try:
            self.db_pool = await asyncpg.create_pool(
                **self.db_config,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info("Database pool created successfully")

            # Тестовый запрос
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result == 1:
                    logger.info("Database connection verified")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def init_notify_listener(self):
        """
        Инициализация PostgreSQL LISTEN/NOTIFY
        Создает отдельное соединение для получения событий из БД
        """
        if not self.use_notify:
            logger.info("PostgreSQL NOTIFY disabled in configuration")
            return False

        try:
            # Создаем отдельное соединение для LISTEN
            self.notify_connection = await asyncpg.connect(**self.db_config)

            # Подписываемся на канал уведомлений
            await self.notify_connection.add_listener(
                self.notify_channel,
                self.on_notify_received
            )

            self.notify_available = True
            logger.info(f"✓ PostgreSQL NOTIFY listener active on channel '{self.notify_channel}'")
            logger.info(f"  Mode: Event-driven (real-time <10ms)")
            return True

        except Exception as e:
            logger.warning(f"Failed to setup NOTIFY listener: {e}")
            logger.info(f"  Falling back to polling mode (interval: {self.lightweight_check_interval}s)")
            self.notify_available = False
            return False

    async def on_notify_received(self, connection, pid, channel, payload):
        """
        Callback вызывается при получении NOTIFY от PostgreSQL
        Обеспечивает мгновенную реакцию на новые сигналы (<10ms)
        С поддержкой Smart Retry для обработки гонки данных
        """
        target_signal_id = None
        target_score = 0
        
        try:
            # Парсим payload от триггера
            if payload:
                data = json.loads(payload)
                target_signal_id = data.get('id')
                target_score = data.get('total_score', 0)
                
                logger.info(f"⚡ NOTIFY received: event={data.get('event')}, "
                          f"id={target_signal_id}, symbol={data.get('pair_symbol')}, "
                          f"total_score={target_score}")
            else:
                logger.info(f"⚡ NOTIFY received from PID {pid}")

            # Smart Retry Logic
            # Если мы знаем ID сигнала и он подходит по скору, мы должны его найти.
            # Если не находим сразу - повторяем попытки (ждем завершения транзакции записи паттернов)
            
            max_retries = 10
            retry_delay = 1.0 # секунд
            
            for attempt in range(1, max_retries + 1):
                # Выполняем полный запрос
                signals = await self.do_full_query_and_broadcast()
                
                # Если у нас нет конкретного ID (пустой payload), одного прохода достаточно
                if not target_signal_id:
                    break
                    
                # Проверяем, нашли ли мы целевой сигнал
                found = any(s['id'] == target_signal_id for s in signals)
                
                if found:
                    if attempt > 1:
                        logger.info(f"✅ Signal {target_signal_id} found on attempt {attempt}!")
                    break
                else:
                    # Если сигнал подходит по фильтру, но мы его не нашли - значит паттерны еще не записались
                    if target_score > SCORE_THRESHOLD:
                        logger.warning(f"⏳ Signal {target_signal_id} (Score: {target_score}) not found in query results (Attempt {attempt}/{max_retries}). "
                                     f"Waiting {retry_delay}s for patterns to sync...")
                        await asyncio.sleep(retry_delay)
                    else:
                        # Если скор ниже порога, искать нет смысла
                        break
            
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in NOTIFY payload: {payload[:100]}")
            # Все равно делаем запрос (один раз)
            await self.do_full_query_and_broadcast()
        except Exception as e:
            logger.error(f"Error in on_notify_received: {e}")
            logger.error(f"Error processing NOTIFY: {e}")
            self.stats['errors'] += 1

    def is_duplicate(self, signal: dict) -> bool:
        """
        Check if signal is a duplicate within cooldown period
        """
        symbol = signal['pair_symbol']
        signal_ts_str = signal['timestamp'] # ISO format string
        
        try:
            signal_ts = datetime.fromisoformat(signal_ts_str)
        except:
            # If parsing fails, assume it's new but log error
            logger.error(f"Failed to parse timestamp for {symbol}: {signal_ts_str}")
            return False
            
        if symbol in self.seen_signals:
            last_ts = self.seen_signals[symbol]
            
            # Если это тот же самый сигнал (тот же timestamp), мы его оставляем
            # Это важно для поддержания списка активных сигналов при поллинге
            if signal_ts == last_ts:
                return False
                
            # Check cooldown for NEW signals
            if (signal_ts - last_ts).total_seconds() < self.dedup_cooldown_hours * 3600:
                return True
        
        # Update seen
        self.seen_signals[symbol] = signal_ts
        return False

    def clean_seen_signals(self):
        """Remove old entries from seen_signals"""
        now = datetime.now()
        to_remove = []
        for symbol, ts in self.seen_signals.items():
            if (now - ts).total_seconds() > self.dedup_cooldown_hours * 3600:
                to_remove.append(symbol)
        
        for symbol in to_remove:
            del self.seen_signals[symbol]

    async def fetch_signals(self) -> List[dict]:
        """Получение высококачественных сигналов из БД"""
        try:
            async with self.db_pool.acquire() as conn:
                # Формируем запрос
                query = self.build_signal_query()

                # Выполняем запрос
                rows = await conn.fetch(
                    query % self.signal_window_minutes
                )

                # Преобразуем в словари
                signals = []
                for row in rows:
                    signal = {
                        'id': row['id'],
                        'trading_pair_id': row['trading_pair_id'],
                        'pair_symbol': row['pair_symbol'],
                        'total_score': float(row['total_score']) if row['total_score'] else 0,
                        'score_week': float(row['score_week']) if row['score_week'] else 0,
                        'score_month': float(row['score_month']) if row['score_month'] else 0,
                        'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                        'exchange_id': row['exchange_id'],
                        'contract_type_id': row['contract_type_id'],
                        'patterns': row['patterns'] if row['patterns'] else [],
                        'timeframes': row['timeframes'] if row['timeframes'] else [],
                        
                        # Добавляем параметры по умолчанию
                        'recommended_action': self.default_params['recommended_action'],
                        'score_week_filter': self.default_params['score_week_filter'],
                        'score_month_filter': self.default_params['score_month_filter'],
                        'max_trades_filter': self.default_params['max_trades_filter'],
                        'stop_loss_filter': self.default_params['stop_loss_filter'],
                        'trailing_activation_filter': self.default_params['trailing_activation_filter'],
                        'trailing_distance_filter': self.default_params['trailing_distance_filter']
                    }
                    signals.append(signal)

                self.stats['queries_executed'] += 1
                logger.debug(f"Fetched {len(signals)} high-score signals from database")

                return signals

        except Exception as e:
            logger.error(f"Error fetching signals: {e}")
            self.stats['errors'] += 1
            return []

    async def check_for_changes_lightweight(self) -> bool:
        """
        Легковесная проверка: появились ли новые высококачественные сигналы?
        Запрос выполняется за ~1-2ms вместо ~50-100ms полного запроса
        Проверяет только MAX(id) и MAX(timestamp)
        """
        try:
            async with self.db_pool.acquire() as conn:
                # Exchange filter logic for lightweight check
                exchange_filter_clause = ""
                if EXCHANGE_FILTER == 'BINANCE':
                    exchange_filter_clause = f"AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']}"
                elif EXCHANGE_FILTER == 'BYBIT':
                    exchange_filter_clause = f"AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']}"

                result = await conn.fetchrow(f"""
                    SELECT
                        MAX(sh.id) as max_id,
                        MAX(sh.timestamp) as max_timestamp,
                        COUNT(*) as total_count
                    FROM fas_v2.scoring_history sh
                    JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
                    WHERE sh.timestamp >= now() - INTERVAL '%s minutes'
                        AND sh.is_active = true
                        AND tp.is_active = true
                        AND sh.total_score > {SCORE_THRESHOLD}
                        AND tp.contract_type_id = 1
                        {exchange_filter_clause}
                """ % self.signal_window_minutes)

                if not result or not result['max_id']:
                    return False

                max_id = result['max_id']
                max_ts = result['max_timestamp']

                # Проверяем изменения
                has_changes = (
                    max_id > self.last_max_id or
                    (max_ts and max_ts != self.last_check_timestamp)
                )

                if has_changes:
                    self.last_max_id = max_id
                    self.last_check_timestamp = max_ts
                    logger.debug(f"Changes detected: max_id={max_id}, count={result['total_count']}")

                return has_changes

        except Exception as e:
            logger.error(f"Error in lightweight check: {e}")
            return True  # При ошибке делаем полный запрос

    async def do_full_query_and_broadcast(self):
        """
        Выполняет полный запрос сигналов и рассылку всем клиентам
        Используется как при NOTIFY, так и при обнаружении изменений в polling mode
        """
        try:
            signals = await self.fetch_signals()
            
            # Deduplicate
            unique_signals = []
            for sig in signals:
                if not self.is_duplicate(sig):
                    unique_signals.append(sig)
            
            # Clean up old seen signals
            self.clean_seen_signals()
            
            if not unique_signals:
                logger.debug("No new unique signals to broadcast")
                return

            # Update last signals (keep all active ones for initial sync, but broadcast only new ones?)
            # Wait, the client expects a list of active signals or a stream of new ones?
            # The original code sent the FULL list of active signals.
            # If we deduplicate, we might filter out active signals that we already sent.
            # If the client is stateless, it needs the full list.
            # If the client is stateful, it wants updates.
            # The user said: "all signals in case of detection are immediately broadcast... check for duplicates"
            # If I filter duplicates, I am suppressing the broadcast of existing signals.
            # This effectively turns it into an event stream of NEW signals.
            # BUT, handle_auth sends self.last_signals.
            # So self.last_signals should probably contain ALL active signals.
            # But broadcast_signals should maybe only send NEW ones?
            # The original code sent `signals` (the full list) to `broadcast_signals`.
            # Let's assume the user wants to filter duplicates from the *stream*.
            # But if we filter them, `self.last_signals` will only have new ones.
            # If a new client connects, they get `self.last_signals`. If that only has new ones, they miss old active ones.
            # So:
            # 1. `signals` = all active signals from DB.
            # 2. `self.last_signals` = `signals` (for new clients).
            # 3. `new_unique_signals` = filter `signals` against `seen_signals`.
            # 4. Broadcast `new_unique_signals`?
            # OR does the user mean "don't send the SAME signal object twice"?
            # The user said "check for duplicates... signals update every 15 minutes".
            # This implies the same signal might be re-detected.
            # If I use `is_duplicate` which checks 24h cooldown, I am effectively saying "Only one signal per pair per 24h".
            # This matches `populate_signal_analysis.py`.
            # So if I filter the list from DB using `is_duplicate`, I get a list of "valid unique signals in the window".
            # If I broadcast this list, it's fine.
            # But if the list is [A, B] and next time it is [A, B, C].
            # If I broadcast [A, B, C], the client receives A and B again.
            # The original code did exactly this: broadcast the full list.
            # If the user wants to avoid duplicates, maybe they mean "don't broadcast if the list hasn't changed"?
            # The `check_for_changes_lightweight` already does this optimization.
            # But if the user explicitly asked for "check for duplicates", they probably mean the 24h cooldown logic.
            # So I will apply the 24h cooldown filter to the list fetched from DB.
            # This ensures that if a pair signals again within 24h, it is NOT included in the list.
            
            # Apply filter to the full list
            filtered_signals = []
            # We need to be careful. `is_duplicate` updates `seen_signals`.
            # If we run this every 3 seconds, we don't want to mark a signal as "seen" and then filter it out next time because it's "seen".
            # We want to filter out *subsequent* signals for the same pair.
            # But the DB query returns the *latest* signal for the pair (ORDER BY timestamp DESC).
            # Wait, the query returns ALL signals in the window.
            # If there are multiple signals for the same pair in the window, we should only keep the first one?
            # `deduplicate_signals` in lib does exactly this.
            # But here we are in a loop.
            # Let's use a local deduplication for the current batch, AND a global one for 24h history.
            
            # Actually, `is_duplicate` as implemented checks if we saw this pair in the last 24h.
            # If we saw it 1 minute ago (in the previous loop), it will return True.
            # This would filter out the signal we just sent!
            # That's bad if we want to maintain a list of "active signals".
            # If the goal is "Broadcast NEW signals only", then filtering is correct.
            # If the goal is "Broadcast ACTIVE signals", then we should NOT filter out signals we just sent, ONLY signals that are "duplicates" of older ones (e.g. double signal in 15 mins).
            
            # Let's look at `deduplicate_signals` in lib again.
            # It takes a list and returns unique ones.
            # It doesn't have state across calls.
            # So I should implement `deduplicate_signals` logic on the `signals` list returned from DB.
            # And NOT use a persistent `seen_signals` that blocks re-sending the same signal object.
            # BUT, if the user wants to prevent "spamming" the same signal every 3 seconds, the `check_for_changes_lightweight` handles that.
            # So the "duplicate" check is likely about the "multiple signals for same pair" issue.
            
            # So: Implement `deduplicate_signals` logic on the fetched list.
            
            unique_signals = []
            seen_pairs = set()
            for sig in signals:
                if sig['pair_symbol'] not in seen_pairs:
                    unique_signals.append(sig)
                    seen_pairs.add(sig['pair_symbol'])
            
            # Check if the unique list is different from last time?
            # `check_for_changes_lightweight` checks max_id/timestamp.
            # If a new signal comes, max_id changes.
            # We fetch all. We dedup.
            # We broadcast the new unique list.
            
            self.last_signals = unique_signals
            await self.broadcast_signals(unique_signals)

            logger.info(f"📡 Broadcast {len(unique_signals)} high-score signals to {len(self.authenticated_clients)} clients")
            
            # Детальная статистика по паттернам
            if unique_signals:
                pattern_counts = {}
                for sig in unique_signals:
                    for pattern in sig.get('patterns', []):
                        pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
                logger.info(f"   Pattern distribution: {pattern_counts}")
                
        except Exception as e:
            logger.error(f"Error in full query and broadcast: {e}")
            self.stats['errors'] += 1

    async def broadcast_signals(self, signals: List[dict]):
        """Отправка сигналов всем аутентифицированным клиентам"""
        if not self.authenticated_clients:
            return

        # Подготовка сообщения
        message = json.dumps({
            'type': 'signals',
            'timestamp': datetime.now().isoformat(),
            'count': len(signals),
            'data': signals
        })

        # Отправка всем аутентифицированным клиентам
        disconnected = set()

        for client in self.authenticated_clients:
            try:
                await client.send(message)
                self.stats['signals_sent'] += 1
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(client)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected.add(client)

        # Удаление отключенных клиентов
        for client in disconnected:
            await self.disconnect_client(client)

    async def handle_client(self, websocket):
        """Обработка подключения клиента"""
        # Регистрация клиента
        self.connected_clients.add(websocket)
        client_ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'

        self.client_info[websocket] = {
            'ip': client_ip,
            'connected_at': datetime.now(),
            'authenticated': False
        }

        logger.info(f"New client connected from {client_ip}")

        try:
            # Отправляем запрос аутентификации
            await websocket.send(json.dumps({
                'type': 'auth_required',
                'message': 'Please authenticate with your token'
            }))

            # Ждем аутентификацию (30 секунд таймаут)
            auth_task = asyncio.create_task(self.wait_for_auth(websocket))

            # Основной цикл обработки сообщений
            async for message in websocket:
                await self.handle_message(websocket, message)

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client {client_ip} disconnected")
        except Exception as e:
            logger.error(f"Error handling client {client_ip}: {e}")
        finally:
            await self.disconnect_client(websocket)
            auth_task.cancel()

    async def wait_for_auth(self, websocket):
        """Ожидание аутентификации с таймаутом"""
        await asyncio.sleep(30)

        if websocket in self.connected_clients and websocket not in self.authenticated_clients:
            logger.warning(f"Client {self.client_info[websocket]['ip']} failed to authenticate in time")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Authentication timeout'
            }))
            await websocket.close()

    async def handle_message(self, websocket, message: str):
        """Обработка сообщения от клиента"""
        try:
            data = json.loads(message)
            msg_type = data.get('type')

            if msg_type == 'auth':
                await self.handle_auth(websocket, data)
            elif msg_type == 'ping':
                await websocket.send(json.dumps({'type': 'pong'}))
            elif msg_type == 'get_stats':
                await self.send_stats(websocket)
            elif msg_type == 'get_signals':
                # Немедленная отправка последних сигналов
                if websocket in self.authenticated_clients:
                    await websocket.send(json.dumps({
                        'type': 'signals',
                        'timestamp': datetime.now().isoformat(),
                        'count': len(self.last_signals),
                        'data': self.last_signals
                    }))
            else:
                logger.warning(f"Unknown message type: {msg_type}")

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON from client: {message[:100]}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def handle_auth(self, websocket, data: dict):
        """Обработка аутентификации"""
        token = data.get('token')

        if not token:
            await websocket.send(json.dumps({
                'type': 'auth_failed',
                'message': 'Token required'
            }))
            return

        # Проверка токена
        if self.hash_token(token) == self.auth_token:
            self.authenticated_clients.add(websocket)
            self.client_info[websocket]['authenticated'] = True

            logger.info(f"Client {self.client_info[websocket]['ip']} authenticated successfully")

            await websocket.send(json.dumps({
                'type': 'auth_success',
                'message': 'Authentication successful',
                'query_interval': self.query_interval,
                'signal_window': self.signal_window_minutes,
                'default_params': self.default_params
            }))

            # Отправляем последние сигналы сразу после аутентификации
            if self.last_signals:
                await websocket.send(json.dumps({
                    'type': 'signals',
                    'timestamp': datetime.now().isoformat(),
                    'count': len(self.last_signals),
                    'data': self.last_signals
                }))
        else:
            logger.warning(f"Authentication failed for {self.client_info[websocket]['ip']}")
            await websocket.send(json.dumps({
                'type': 'auth_failed',
                'message': 'Invalid token'
            }))
            await asyncio.sleep(1)
            await websocket.close()

    async def send_stats(self, websocket):
        """Отправка статистики сервера"""
        if websocket not in self.authenticated_clients:
            return

        uptime = (datetime.now() - self.stats['start_time']).total_seconds()

        await websocket.send(json.dumps({
            'type': 'stats',
            'uptime_seconds': uptime,
            'connected_clients': len(self.connected_clients),
            'authenticated_clients': len(self.authenticated_clients),
            'queries_executed': self.stats['queries_executed'],
            'signals_sent': self.stats['signals_sent'],
            'errors': self.stats['errors'],
            'last_query': self.last_signals[0]['timestamp'] if self.last_signals else None,
            'default_params': self.default_params
        }))

    async def disconnect_client(self, websocket):
        """Отключение клиента"""
        self.connected_clients.discard(websocket)
        self.authenticated_clients.discard(websocket)

        if websocket in self.client_info:
            logger.info(f"Client {self.client_info[websocket]['ip']} disconnected")
            del self.client_info[websocket]

    async def smart_query_loop(self):
        """
        Умный цикл опроса с адаптивной стратегией:
        - Если NOTIFY доступен: fallback проверка раз в 60 сек (safety net)
        - Если NOTIFY недоступен: легковесные проверки каждую секунду
        """
        last_full_query = datetime.now()

        while self.running:
            try:
                if self.notify_available:
                    # ===== NOTIFY MODE =====
                    # NOTIFY обрабатывает события моментально
                    # Здесь только fallback проверка на случай пропуска NOTIFY
                    await asyncio.sleep(self.notify_fallback_interval)

                    logger.debug("Fallback check (NOTIFY mode, safety net)")
                    if await self.check_for_changes_lightweight():
                        await self.do_full_query_and_broadcast()
                        last_full_query = datetime.now()

                else:
                    # ===== POLLING MODE =====
                    # Легковесная проверка на изменения
                    has_changes = await self.check_for_changes_lightweight()

                    # Принудительный полный запрос каждые N секунд (safety net)
                    time_since_last = (datetime.now() - last_full_query).total_seconds()
                    force_full_query = time_since_last >= self.query_interval

                    if has_changes or force_full_query:
                        await self.do_full_query_and_broadcast()
                        last_full_query = datetime.now()
                    else:
                        logger.debug("No changes detected, skipping full query")

                    # Короткая пауза до следующей проверки
                    await asyncio.sleep(self.lightweight_check_interval)

            except Exception as e:
                logger.error(f"Error in smart query loop: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(5)  # Короткая пауза при ошибке

    async def start(self):
        """Запуск сервера с гибридным режимом"""
        logger.info("=" * 70)
        logger.info("Starting High-Score Signal WebSocket Server (Hybrid Mode)")
        logger.info("=" * 70)

        # Инициализация БД
        await self.init_db()

        # Попытка инициализации NOTIFY
        await self.init_notify_listener()

        # Загрузка начальных сигналов
        self.last_signals = await self.fetch_signals()
        logger.info(f"✓ Initial high-score signals loaded: {len(self.last_signals)} signals")

        self.running = True

        # Вывод режима работы
        if self.notify_available:
            logger.info("🚀 Running in NOTIFY mode (event-driven)")
            logger.info(f"   - Latency: <10ms")
            logger.info(f"   - Fallback check: every {self.notify_fallback_interval}s")
        else:
            logger.info("🚀 Running in POLLING mode (lightweight checks)")
            logger.info(f"   - Check interval: {self.lightweight_check_interval}s")
            logger.info(f"   - Full query fallback: every {self.query_interval}s")

        # Вывод параметров по умолчанию
        logger.info("📋 Default parameters:")
        for key, value in self.default_params.items():
            logger.info(f"   - {key}: {value}")

        # Запуск умного цикла опроса
        query_task = asyncio.create_task(self.smart_query_loop())

        # Запуск WebSocket сервера
        async with websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=10
        ) as server:
            logger.info(f"✓ WebSocket Server listening on {self.host}:{self.port}")
            logger.info(f"✓ Signal window: {self.signal_window_minutes} minutes")
            logger.info(f"✓ Filters: total_score > {SCORE_THRESHOLD}, patterns={TARGET_PATTERNS}")
            logger.info("=" * 70)

            try:
                await asyncio.Future()  # Работаем вечно
            except KeyboardInterrupt:
                logger.info("Shutting down server...")
            finally:
                self.running = False
                query_task.cancel()

                # Закрываем все соединения
                if self.connected_clients:
                    await asyncio.gather(
                        *[client.close() for client in self.connected_clients],
                        return_exceptions=True
                    )

                # Закрываем NOTIFY соединение
                if self.notify_connection:
                    try:
                        await self.notify_connection.close()
                        logger.info("NOTIFY connection closed")
                    except:
                        pass

                # Закрываем пул БД
                if self.db_pool:
                    await self.db_pool.close()

                logger.info("Server stopped")


def main():
    """Главная функция запуска"""
    import os
    from dotenv import load_dotenv

    # Загрузка конфигурации
    load_dotenv()

    ws_password = os.getenv('WS_AUTH_PASSWORD')
    if not ws_password:
        logger.critical("❌ SECURITY ERROR: WS_AUTH_PASSWORD not set in environment!")
        logger.critical("Please set WS_AUTH_PASSWORD in .env file.")
        sys.exit(1)

    config = {
        # WebSocket сервер - специальный порт для high-score сигналов
        'WS_SERVER_HOST': os.getenv('HIGH_SCORE_WS_SERVER_HOST', '0.0.0.0'),
        'WS_SERVER_PORT': os.getenv('HIGH_SCORE_WS_SERVER_PORT', '25370'),
        'WS_AUTH_TOKEN': hashlib.sha256(ws_password.encode()).hexdigest(),

        # База данных
        'DB_HOST': os.getenv('DB_HOST', 'localhost'),
        'DB_PORT': os.getenv('DB_PORT', '5432'),
        'DB_NAME': os.getenv('DB_NAME'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),

        # Параметры опроса
        'QUERY_INTERVAL_SECONDS': os.getenv('QUERY_INTERVAL_SECONDS', '30'),
        'SIGNAL_WINDOW_MINUTES': os.getenv('SIGNAL_WINDOW_MINUTES', '32'),

        # Гибридный режим
        'USE_NOTIFY': os.getenv('USE_NOTIFY', 'true'),
        'NOTIFY_CHANNEL': os.getenv('NOTIFY_CHANNEL', 'new_signals'),
        'LIGHTWEIGHT_CHECK_INTERVAL': os.getenv('LIGHTWEIGHT_CHECK_INTERVAL', '1'),
        'NOTIFY_FALLBACK_INTERVAL': os.getenv('NOTIFY_FALLBACK_INTERVAL', '60')
    }

    # Создание и запуск сервера
    server = HighScoreSignalWebSocketServer(config)

    # Обработка сигналов завершения
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server crashed: {e}")
        raise


if __name__ == '__main__':
    main()
