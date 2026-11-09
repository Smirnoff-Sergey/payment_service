# Payment Service

Асинхронный микросервис процессинга платежей. Принимает запрос на оплату, обрабатывает его через эмуляцию внешнего платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

## Архитектура

```
 client ──POST──► api ──┐ одна транзакция
                        ├──► payments
                        └──► outbox
                                │ SELECT … FOR UPDATE SKIP LOCKED
                          outbox-relay ──► [payments] ──► (payments.new) ──► consumer ──► webhook
                                                ▲                │ reject          │ ошибка
                                                │ TTL истёк      ▼                 ▼
                              (payments.retry.N) ◄── [payments.retry]   [payments.dlx] ──► (payments.dlq)
```

| Сервис | Назначение |
|---|---|
| `api` | REST API (`app/api`). Пишет платёж и событие `outbox` в одной транзакции |
| `outbox-relay` | Публикует неотправленные события из `outbox` в RabbitMQ и периодически чистит старые данные (`app/relay`) |
| `consumer` | Один обработчик (`app/consumer`): эмуляция шлюза → статус в БД → webhook |
| `migrations` | Одноразовый `alembic upgrade head` перед стартом остальных сервисов |
| `webhook-mock` | Демонстрационный приёмник webhook с проверкой подписи (`tools/webhook_mock.py`, не часть сервиса) |
| `postgres`, `rabbitmq` | Инфраструктура (management UI RabbitMQ: http://localhost:15672, guest/guest) |

### Гарантии доставки

- **Outbox pattern.** API не публикует сообщения в брокер напрямую: платёж и событие в `outbox` фиксируются одной транзакцией. `outbox-relay` забирает события пачками (`FOR UPDATE SKIP LOCKED`, можно запускать несколько экземпляров), публикует persistent-сообщения с publisher confirms и только после подтверждения брокера проставляет `published_at`. Если публикация упала, транзакция откатывается и событие уходит в следующей итерации. Это даёт доставку *at-least-once*.
- **Идемпотентность API.** Заголовок `Idempotency-Key` обязателен. Ключ резервируется в таблице `idempotency_keys` на `IDEMPOTENCY_KEY_TTL_HOURS` (24 ч) через `INSERT … ON CONFLICT DO NOTHING`, поэтому параллельные запросы с одним ключом создают один платёж и одно событие. Пока ключ жив, повтор с тем же ключом и телом возвращает существующий платёж (`202`), а тот же ключ с другим телом — `409 Conflict` (сохраняется SHA-256 тела запроса). Истёкший ключ освобождается в той же транзакции, и запрос с ним создаёт новый платёж. Так клиент не обязан держать ключи уникальными вечно, а таблица ключей не растёт бесконечно. В `payments.idempotency_key` ключ остаётся для истории.
- **Идемпотентность consumer.** Сообщение может прийти повторно (at-least-once, ретраи), поэтому каждый шаг защищён состоянием в БД. Шлюз вызывается, только пока платёж в статусе `pending`; статус меняется условным `UPDATE … WHERE status = 'pending'`. Webhook отправляется, пока не заполнено `webhook_delivered_at`. Повтор после сбоя webhook не списывает деньги второй раз.
- **Ключ идемпотентности для шлюза.** Между вызовом шлюза и условным `UPDATE` есть окно: consumer может упасть после списания, но до записи статуса, или одно событие может обрабатываться параллельно. Поэтому шлюз получает `payment.id` как ключ идемпотентности (аналог `Idempotency-Key` у Stripe). Повторный запрос с тем же ключом возвращает исходный результат без второго списания. Эмулятор (`app/consumer/gateway.py`) хранит результаты в памяти процесса, у реального шлюза они хранятся на его стороне.
  Альтернативу — промежуточный статус `processing` — отвергли. Он защищает только от параллельной доставки. При падении после перехода в `processing` платёж зависает, нужен «уборщик» по таймауту, а ему всё равно приходится узнавать у шлюза результат по ключу.
- **Retry с экспоненциальной задержкой — через брокер.** При ошибке (недоступен webhook, сбой БД и т.п.) consumer переиздаёт сообщение в `payments.retry` с заголовком `x-retry-count`. Оно попадает в очередь `payments.retry.N` с TTL `RETRY_BASE_DELAY * 2^(N-1)`, то есть 1, 2 и 4 с. Когда TTL истекает, RabbitMQ через dead-letter возвращает сообщение в `payments.new`. Consumer при этом не блокируется на ожидании.
- **Dead Letter Queue.** После `MAX_RETRIES` (3) неудачных повторов сообщение отклоняется (`reject`). `payments.new` объявлена с `x-dead-letter-exchange=payments.dlx`, поэтому RabbitMQ кладёт его в `payments.dlq`. Туда же сразу уходят сообщения, которые повторять бессмысленно: невалидное тело или несуществующий платёж.

Отказ шлюза (10% случаев) — это бизнес-результат, а не ошибка: платёж получает статус `failed`, клиенту уходит webhook.

### Безопасность

- **Секреты.** `API_KEY` и `WEBHOOK_SECRET` передаются через Docker secrets: файлы `secrets/api_key` и `secrets/webhook_secret` монтируются в `/run/secrets/`, их читает pydantic-settings (`secrets_dir`). В образ и в `.env` они не попадают. Значений по умолчанию нет: без секретов сервис не запустится. В коде это `SecretStr`, поэтому значения не появятся в `repr` и логах. Одноимённая переменная окружения имеет приоритет над файлом, это удобно для локального запуска.
- **Подпись webhook.** Каждый запрос подписан HMAC-SHA256 с секретом `WEBHOOK_SECRET`. Заголовки `X-Webhook-Timestamp` (Unix-время) и `X-Webhook-Signature: sha256=<hex>`, подпись считается от строки `"{timestamp}.{тело запроса}"`. Получатель пересчитывает подпись над сырым телом, сравнивает её в постоянное время и отклоняет запросы старше 5 минут, это защищает от повтора перехваченного запроса. Пример проверки — `tools/webhook_mock.py` (на неверную подпись отвечает `401`), функции — `app/consumer/signing.py`.
- **Защита от SSRF.** `webhook_url` задаёт клиент, поэтому без проверки сервис можно заставить отправлять запросы во внутреннюю сеть (БД, metadata-эндпоинт облака, админки). API сразу отвечает `422` на `localhost` и непубличные IP-литералы. Перед каждой отправкой consumer резолвит имя, и если хотя бы один адрес непубличный (private, loopback, link-local, CGNAT, multicast, reserved, в том числе IPv4-mapped IPv6), отправка блокируется. Запрос уходит на проверенный IP, а имя передаётся в `Host` и в SNI, так что TLS-сертификат проверяется как обычно. Поэтому DNS rebinding (смена DNS-ответа между проверкой и подключением) не срабатывает. Редиректы не выполняются. Встроенный IPv4 в NAT64 (`64:ff9b::/96`), SIIT и 6to4 тоже проверяется. Keep-alive отключён: соединения в пуле привязаны к IP, и TLS-соединение, проверенное для одного имени, не должно переиспользоваться для другого хоста на том же адресе. Заблокированный адрес — перманентная ошибка: сообщение сразу уходит в DLQ. Хосты из `WEBHOOK_ALLOWED_HOSTS` проверку пропускают; в compose там `webhook-mock`, демо-приёмник во внутренней сети.

### Очистка данных

`outbox-relay` раз в `CLEANUP_INTERVAL` (60 с) удаляет опубликованные события `outbox` старше `OUTBOX_RETENTION_HOURS` (24 ч) и истёкшие ключи идемпотентности. Удаление идёт пачками по `CLEANUP_BATCH_SIZE` с `FOR UPDATE SKIP LOCKED`: несколько экземпляров relay не мешают друг другу, длинных транзакций нет. Для выборки есть частичный индекс `outbox(published_at) WHERE published_at IS NOT NULL`. За один проход удаляется не больше 10 пачек, чтобы большой накопленный объём не тормозил публикацию: он разбирается за несколько проходов. При большом потоке событий лучше партиционировать `outbox` по `created_at` и удалять целые партиции (`DROP PARTITION` вместо `DELETE` не раздувает таблицу), но для этого сервиса пачечного `DELETE` достаточно.

## Запуск

```bash
mkdir -p secrets
openssl rand -hex 32 > secrets/api_key          # ключ для заголовка X-API-Key
openssl rand -hex 32 > secrets/webhook_secret   # секрет подписи webhook
chmod 644 secrets/*                             # файлы монтируются с правами хоста, сервисы работают не от root
cp .env.example .env                            # необязательно: переопределение настроек
docker compose up -d --build
```

API доступно на http://localhost:8000, Swagger UI — http://localhost:8000/docs.

Настройки (переменные окружения, см. `.env.example` и `app/config.py`): `MAX_RETRIES`, `RETRY_BASE_DELAY`, `GATEWAY_MIN_DELAY`/`GATEWAY_MAX_DELAY` (2–5 с), `GATEWAY_SUCCESS_RATE` (0.9), `WEBHOOK_TIMEOUT`, `WEBHOOK_ALLOWED_HOSTS`, `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_RETENTION_HOURS`, `CLEANUP_INTERVAL`, `CLEANUP_BATCH_SIZE`, `IDEMPOTENCY_KEY_TTL_HOURS`. Секреты `API_KEY` и `WEBHOOK_SECRET` — из `secrets/` (см. «Безопасность»).

## Примеры

Все запросы требуют заголовок `X-API-Key`.

**Создание платежа:**

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $(cat secrets/api_key)" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-mock:9000/webhook"
      }'
```

```
HTTP/1.1 202 Accepted

{"payment_id":"66a237ae-62be-4004-b0b4-fbfa299ffdb4","status":"pending","created_at":"2026-11-09T11:33:17.449833Z"}
```

Повтор с тем же `Idempotency-Key` вернёт тот же `payment_id`. Тот же ключ с другим телом вернёт `409`.

**Получение платежа** (через 2–5 с статус станет `succeeded` или `failed`):

```bash
curl http://localhost:8000/api/v1/payments/66a237ae-62be-4004-b0b4-fbfa299ffdb4 -H "X-API-Key: $(cat secrets/api_key)"
```

```json
{
  "payment_id": "66a237ae-62be-4004-b0b4-fbfa299ffdb4",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-mock:9000/webhook",
  "created_at": "2026-11-09T11:33:17.449833Z",
  "processed_at": "2026-11-09T11:33:20.763830Z"
}
```

**Webhook**, который получит клиент (`POST` на `webhook_url`):

```json
{"payment_id": "66a237ae-…", "status": "succeeded", "amount": "100.50", "currency": "RUB", "processed_at": "2026-11-09T11:33:20.763830Z"}
```

Полученные уведомления можно посмотреть так: `curl http://localhost:9000/webhook`.

**Retry и DLQ.** Укажите `"webhook_url": "http://webhook-mock:9000/webhook/fail"` — приёмник всегда отвечает 500. В логах `docker compose logs -f consumer` будут видны повторы через 1, 2 и 4 с, затем сообщение окажется в `payments.dlq`:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

## Тесты

Unit- и интеграционные тесты (pytest) на реальном PostgreSQL. Тестовая база `payments_test` пересоздаётся автоматически. Compose монтирует секреты во все сервисы, поэтому файлы `secrets/` должны существовать (см. «Запуск»). Тесты задают свои значения через переменные окружения.

```bash
docker compose up -d postgres
docker compose --profile test run --rm tests                                       # все тесты
docker compose --profile test run --rm tests pytest tests/test_api.py -k conflict  # выборочно
```

## Структура

```
app/
  api/            FastAPI: роуты, проверка X-API-Key
  services/       создание платежа: идемпотентность (ключи с TTL) + outbox
  relay/          outbox → RabbitMQ, очистка outbox и истёкших ключей
  consumer/       обработчик (retry/DLQ), эмуляция шлюза, подписанный webhook, защита от SSRF
  messaging/      топология RabbitMQ (обменники, очереди, retry, DLQ)
  models.py       SQLAlchemy-модели payments, outbox и idempotency_keys
  schemas.py      Pydantic-схемы API и сообщений
migrations/       Alembic
tests/
tools/            webhook-mock для демонстрации
```
