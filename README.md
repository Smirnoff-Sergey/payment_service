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
| `outbox-relay` | Публикует неотправленные события из `outbox` в RabbitMQ (`app/relay`) |
| `consumer` | Один обработчик (`app/consumer`): эмуляция шлюза → статус в БД → webhook |
| `migrations` | Одноразовый `alembic upgrade head` перед стартом остальных сервисов |
| `webhook-mock` | Демонстрационный приёмник webhook (`tools/webhook_mock.py`, не часть сервиса) |
| `postgres`, `rabbitmq` | Инфраструктура (management UI RabbitMQ: http://localhost:15672, guest/guest) |

### Гарантии доставки

- **Outbox pattern.** API не публикует сообщения в брокер напрямую: платёж и событие в `outbox` фиксируются одной транзакцией. `outbox-relay` забирает события пачками (`FOR UPDATE SKIP LOCKED`, можно запускать несколько экземпляров), публикует persistent-сообщения с publisher confirms и только после подтверждения брокера проставляет `published_at`. Если публикация упала, транзакция откатывается и событие уходит в следующей итерации. Это даёт доставку *at-least-once*.
- **Идемпотентность API.** Заголовок `Idempotency-Key` обязателен и уникален в БД. Вставка идёт через `INSERT … ON CONFLICT DO NOTHING`, поэтому параллельные запросы с одним ключом создают один платёж и одно событие. Повтор с тем же ключом и телом возвращает существующий платёж (`202`). Тот же ключ с другим телом возвращает `409 Conflict`: сохраняется SHA-256 тела запроса.
- **Идемпотентность consumer.** Сообщение может прийти повторно (at-least-once, ретраи), поэтому каждый шаг защищён состоянием в БД. Шлюз вызывается, только пока платёж в статусе `pending`; статус меняется условным `UPDATE … WHERE status = 'pending'`. Webhook отправляется, пока не заполнено `webhook_delivered_at`. Повтор после сбоя webhook не списывает деньги второй раз.
- **Retry с экспоненциальной задержкой — через брокер.** При ошибке (недоступен webhook, сбой БД и т.п.) consumer переиздаёт сообщение в `payments.retry` с заголовком `x-retry-count`. Оно попадает в очередь `payments.retry.N` с TTL `RETRY_BASE_DELAY * 2^(N-1)`, то есть 1, 2 и 4 с. Когда TTL истекает, RabbitMQ через dead-letter возвращает сообщение в `payments.new`. Consumer при этом не блокируется на ожидании.
- **Dead Letter Queue.** После `MAX_RETRIES` (3) неудачных повторов сообщение отклоняется (`reject`). `payments.new` объявлена с `x-dead-letter-exchange=payments.dlx`, поэтому RabbitMQ кладёт его в `payments.dlq`. Туда же сразу уходят сообщения, которые повторять бессмысленно: невалидное тело или несуществующий платёж.

Отказ шлюза (10% случаев) — это бизнес-результат, а не ошибка: платёж получает статус `failed`, клиенту уходит webhook.

## Запуск

```bash
cp .env.example .env        # задайте API_KEY
docker compose up -d --build
```

API доступно на http://localhost:8000, Swagger UI — http://localhost:8000/docs.

Настройки (переменные окружения, см. `.env.example` и `app/config.py`): `API_KEY`, `MAX_RETRIES`, `RETRY_BASE_DELAY`, `GATEWAY_MIN_DELAY`/`GATEWAY_MAX_DELAY` (2–5 с), `GATEWAY_SUCCESS_RATE` (0.9), `WEBHOOK_TIMEOUT`, `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`.

## Примеры

Все запросы требуют заголовок `X-API-Key`.

**Создание платежа:**

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
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
curl http://localhost:8000/api/v1/payments/66a237ae-62be-4004-b0b4-fbfa299ffdb4 -H "X-API-Key: change-me"
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

Unit- и интеграционные тесты (pytest) на реальном PostgreSQL. Тестовая база `payments_test` пересоздаётся автоматически.

```bash
docker compose up -d postgres
docker compose --profile test run --rm tests                                       # все тесты
docker compose --profile test run --rm tests pytest tests/test_api.py -k conflict  # выборочно
```

## Структура

```
app/
  api/            FastAPI: роуты, проверка X-API-Key
  services/       создание платежа: идемпотентность + outbox
  relay/          outbox → RabbitMQ
  consumer/       обработчик (retry/DLQ), эмуляция шлюза, отправка webhook
  messaging/      топология RabbitMQ (обменники, очереди, retry, DLQ)
  models.py       SQLAlchemy-модели payments и outbox
  schemas.py      Pydantic-схемы API и сообщений
migrations/       Alembic
tests/
tools/            webhook-mock для демонстрации
```
