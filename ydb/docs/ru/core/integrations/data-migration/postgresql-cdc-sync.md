# Инкрементальная синхронизация PostgreSQL и {{ ydb-short-name }} через CDC

## Обзор

{{ ydb-short-name }} не предоставляет единого «коробочного» коннектора для двунаправленной репликации с PostgreSQL. Для **near real-time** инкрементальной синхронизации используется связка стандартных инструментов CDC и встроенных возможностей {{ ydb-short-name }}:

| Направление | Цепочка | Роль {{ ydb-short-name }} |
| --- | --- | --- |
| **PostgreSQL → {{ ydb-short-name }}** | Debezium PostgreSQL Connector → [топик](../../concepts/datamodel/topic.md) (Kafka API) → [TRANSFER](../../concepts/transfer.md) | Приём изменений в таблицы |
| **{{ ydb-short-name }} → PostgreSQL** | [CDC changefeed](../../concepts/cdc.md) → топик (Kafka API) → Kafka Connect JDBC Sink | Источник изменений |

Для **начальной (полной) загрузки** из PostgreSQL используйте [YDB Importer](import-jdbc.md). Для ad-hoc чтения без репликации — [федеративные запросы](../../concepts/query_execution/federated_query/postgresql.md) (только `SELECT`, не для регулярного ETL).

Документ описывает архитектуру обоих направлений на примере доменных объектов **заказы пользователя** (`user_orders`) и **платежи пользователя** (`user_payments`) и может быть воспроизведён локально в Docker.

---

## Доменная модель (пример)

### PostgreSQL (schema `shop`)

```sql
CREATE SCHEMA shop;

CREATE TABLE shop.user_orders (
    order_id    BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    product_name TEXT NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'new',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE shop.user_payments (
    payment_id  BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES shop.user_orders(order_id),
    user_id     BIGINT NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE shop.user_orders REPLICA IDENTITY FULL;
ALTER TABLE shop.user_payments REPLICA IDENTITY FULL;
```

### {{ ydb-short-name }}

```yql
CREATE TABLE user_orders (
    order_id Int64 NOT NULL,
    user_id Int64,
    product_name Utf8,
    amount Decimal(12, 2),
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (order_id)
);

CREATE TABLE user_payments (
    payment_id Int64 NOT NULL,
    order_id Int64,
    user_id Int64,
    amount Decimal(12, 2),
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (payment_id)
);
```

Неключевые колонки в {{ ydb-short-name }} допускают `NULL` — это упрощает обработку partial update из Debezium.

---

## PostgreSQL → {{ ydb-short-name }}

### Архитектура

```mermaid
sequenceDiagram
  participant PG as PostgreSQL
  participant DBZ as Debezium Connect
  participant Topic as YDB Topic (Kafka API)
  participant TR as TRANSFER
  participant YDB as YDB Table

  PG->>DBZ: WAL (logical replication)
  DBZ->>Topic: Debezium envelope (JSON)
  Topic->>TR: read by consumer
  TR->>YDB: UPSERT (lambda)
```

**Компоненты:**

1. **PostgreSQL** — источник транзакций; logical decoding через `pgoutput`.
2. **[Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)** — читает WAL, публикует изменения в топики.
3. **[Kafka API](../../reference/kafka-api/index.md)** — {{ ydb-short-name }} принимает запись в [топики](../../concepts/datamodel/topic.md) по протоколу, совместимому с Apache Kafka.
4. **[TRANSFER](../../concepts/transfer.md)** — асинхронно читает топик, преобразует сообщения lambda-функцией и выполняет UPSERT в целевую таблицу.

Альтернатива TRANSFER — [ydb-kafka-sink-connector](https://github.com/ydb-platform/ydb-kafka-sink-connector); TRANSFER предпочтительнее, если преобразование выполняется в YQL на стороне {{ ydb-short-name }}.

### Требования к PostgreSQL

| Параметр | Значение |
| --- | --- |
| `wal_level` | `logical` |
| `max_replication_slots` | ≥ 4 |
| `max_wal_senders` | ≥ 4 |
| `REPLICA IDENTITY` | `FULL` на реплицируемых таблицах |
| Publication | На `shop.user_orders`, `shop.user_payments` |
| Пользователь Debezium | `REPLICATION` + `SELECT` на таблицы |

Пример publication:

```sql
CREATE PUBLICATION dbz_publication FOR TABLE shop.user_orders, shop.user_payments;
```

### Настройка Debezium

Ключевые параметры коннектора:

```json
{
  "name": "postgresql-orders-payments",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.dbname": "shopdb",
    "topic.prefix": "dbz",
    "schema.include.list": "shop",
    "table.include.list": "shop.user_orders,shop.user_payments",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",
    "snapshot.mode": "initial"
  }
}
```

Bootstrap-сервер для записи в {{ ydb-short-name }}: `ydb:9092` (имя сервиса в Docker-сети) или `localhost:9092` при локальном запуске. Топики нужно **создать заранее** в {{ ydb-short-name }} с именами, совпадающими с Debezium (например, `dbz.shop.user_orders`).

### TRANSFER и формат Debezium

Debezium пишет envelope с полями `payload.op` (`c`/`u`/`r`/`d`), `payload.before`, `payload.after`. {{ ydb-short-name }} natively поддерживает совместимый формат `DEBEZIUM_JSON` в [changefeed](../../concepts/cdc.md#debezium-json-record-structure); для TRANSFER из внешнего Debezium lambda разбирает тот же envelope.

Пример lambda для `user_orders` (soft-delete при `op = "d"`):

```yql
$transformation_lambda = ($msg) -> {
    $cdc_data = CAST($msg._data AS Json);
    $operation = Json::ConvertToString($cdc_data.payload.op);
    $is_deleted = $operation == "d";
    $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

    return IF(
        $data IS NOT NULL,
        [
            <|
                order_id: CAST(Json::ConvertToString($data.order_id) AS Int64),
                user_id: CAST(Json::ConvertToString($data.user_id) AS Int64),
                product_name: CAST(Json::ConvertToString($data.product_name) AS Utf8),
                amount: CAST(Json::ConvertToString($data.amount) AS Decimal(12, 2)),
                status: IF($is_deleted, Just("deleted"), CAST(Json::ConvertToString($data.status) AS Utf8)),
                updated_at: CurrentUtcTimestamp()
            |>
        ],
        []
    );
};

CREATE TRANSFER orders_transfer
    FROM `dbz.shop.user_orders` TO user_orders
    USING $transformation_lambda;
```

Аналогичный TRANSFER создаётся для `user_payments`.

{% note warning %}

[TRANSFER](../../concepts/transfer.md) выполняет только UPSERT. Физическое удаление строк в {{ ydb-short-name }} через TRANSFER не поддерживается — используйте soft-delete (флаг `is_deleted` / статус `deleted`) или периодическую очистку через `DELETE`. Подробнее — в [рецепте SCD1](../../analyst/practical-guides/scd/scd1-transfer.md).

{% endnote %}

### Начальная загрузка

Два варианта:

1. **`snapshot.mode=initial`** в Debezium — snapshot существующих строк PG попадает в топик до streaming.
2. Разовый [YDB Importer](import-jdbc.md), затем Debezium с `snapshot.mode=never` для только инкрементальных изменений.

---

## {{ ydb-short-name }} → PostgreSQL

### Архитектура

```mermaid
sequenceDiagram
  participant YDB as YDB Table
  participant CF as CDC Changefeed
  participant Topic as YDB Topic (Kafka API)
  participant KC as Kafka Connect JDBC Sink
  participant PG as PostgreSQL

  YDB->>CF: INSERT/UPDATE/DELETE
  CF->>Topic: DEBEZIUM_JSON
  Topic->>KC: consume
  KC->>PG: UPSERT
```

**Компоненты:**

1. **Строковая таблица {{ ydb-short-name }}** — источник OLTP-изменений.
2. **[CDC changefeed](../../concepts/cdc.md)** — формирует поток изменений в топик; гарантии exactly-once в пределах ключа.
3. **Kafka Connect + JDBC Sink** — читает топик через Kafka API, записывает в PostgreSQL ([пример конфигурации](../../reference/kafka-api/connect/connect-examples.md)).

### Changefeed на источнике

```yql
ALTER TABLE user_orders ADD CHANGEFEED orders_cf WITH (
    FORMAT = 'DEBEZIUM_JSON',
    MODE = 'NEW_AND_OLD_IMAGES',
    INITIAL_SCAN = TRUE
);

ALTER TABLE user_payments ADD CHANGEFEED payments_cf WITH (
    FORMAT = 'DEBEZIUM_JSON',
    MODE = 'NEW_AND_OLD_IMAGES',
    INITIAL_SCAN = TRUE
);
```

`INITIAL_SCAN = TRUE` выгружает существующие строки в топик при создании changefeed — аналог начального snapshot для PG.

Имя топика changefeed получите через:

```bash
ydb scheme describe user_orders
```

### Kafka Connect worker

Минимальные настройки для чтения из {{ ydb-short-name }}:

```ini
bootstrap.servers=ydb:9092
consumer.check.crcs=false
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

Перед запуском sink создайте [читателя топика](../../reference/ydb-cli/topic-consumer-add.md) с именем, совпадающим с `group.id` коннектора (см. [пошаговую инструкцию](../../reference/kafka-api/connect/connect-step-by-step.md)).

### JDBC Sink в PostgreSQL

Базовый конфиг (один коннектор на таблицу):

```ini
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
connection.url=jdbc:postgresql://postgres:5432/shopdb
connection.user=shop
connection.password=shop

topics=<changefeed-topic-for-orders>
insert.mode=upsert
pk.mode=record_key
auto.create=false
auto.evolve=false

transforms=unwrap
transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
transforms.unwrap.drop.tombstones=false
transforms.unwrap.delete.handling.mode=rewrite
```

Целевые таблицы `shop.user_orders` и `shop.user_payments` создаются в PostgreSQL заранее. Foreign key между payments и orders для CDC-пайплайна лучше не включать — события могут приходить out-of-order.

---

## Локальная проверка в Docker

Минимальный стек для POC на одной машине:

| Сервис | Образ | Порты |
| --- | --- | --- |
| {{ ydb-short-name }} | `ydbplatform/local-ydb:latest` | 2136 (gRPC), 8765 (UI), 9092 (Kafka API) |
| PostgreSQL | `postgres:16` | 5432 |
| Debezium Connect | `debezium/connect:2.7` | 8083 (REST) — для PG→YDB |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.6.0` + JDBC plugin | 8083 — для YDB→PG |

Запуск {{ ydb-short-name }} в Docker — см. [инструкцию](../../reference/docker/start.md). Kafka API доступен на порту **9092** без SASL при анонимной аутентификации.

{% note info %}

Для одновременной проверки **обоих** направлений используйте **два изолированных compose-стека** (разные порты PG и {{ ydb-short-name }}), чтобы не смешивать Debezium-топики и changefeed-топики.

{% endnote %}

### Сценарий проверки

1. Поднять стек, применить DDL в PG и {{ ydb-short-name }}.
2. Убедиться, что seed-данные реплицировались (initial snapshot / INITIAL_SCAN).
3. Выполнить `INSERT` и `UPDATE` в источнике.
4. Через ≤ 30 секунд проверить строки в приёмнике по PK.
5. Проверить `DELETE` (soft-delete в PG→YDB через TRANSFER; tombstone/rewrite в YDB→PG через JDBC Sink).

---

## Сравнение с другими подходами

| Задача | Инструмент | Тип синхронизации |
| --- | --- | --- |
| Разовая миграция PG → {{ ydb-short-name }} | [YDB Importer](import-jdbc.md) | Bulk, не инкремент |
| Ad-hoc чтение из PG | [Federated Query](../../concepts/query_execution/federated_query/postgresql.md) | Запрос по требованию, не CDC |
| Репликация {{ ydb-short-name }} → {{ ydb-short-name }} | [Async Replication](../../concepts/async-replication.md) | Встроенная, не PostgreSQL |
| Polling PG → {{ ydb-short-name }} | JDBC Source (`mode=timestamp`) → топик → TRANSFER | Минуты задержки, без WAL |

---

## Ограничения и риски

| Риск | Рекомендация |
| --- | --- |
| TRANSFER не удаляет строки | Soft-delete или периодический `DELETE` |
| Debezium не пишет напрямую в YDB Kafka API | Промежуточный брокер (Redpanda/Kafka) или проверка совместимости |
| DELETE в {{ ydb-short-name }} не отражается в PG | Настроить `ExtractNewRecordState` + `delete.handling.mode` |
| Out-of-order доставка payments/orders | Без FK в PG-приёмнике или deferrable constraints |
| Двунаправленная запись в одни таблицы | Раздельные master-системы, разрешение конфликтов на уровне приложения |

---

## Связь с документацией и кодом

| Тема | Документ |
| --- | --- |
| CDC, DEBEZIUM_JSON | [concepts/cdc.md](../../concepts/cdc.md) |
| TRANSFER | [concepts/transfer.md](../../concepts/transfer.md) |
| Kafka API + Connect | [reference/kafka-api/connect/index.md](../../reference/kafka-api/connect/index.md) |
| Примеры PG ↔ топик | [reference/kafka-api/connect/connect-examples.md](../../reference/kafka-api/connect/connect-examples.md) |
| Lambda для Debezium | [analyst/practical-guides/scd/scd1-transfer.md](../../analyst/practical-guides/scd/scd1-transfer.md) |
| Docker | [reference/docker/start.md](../../reference/docker/start.md) |

---

## Краткие ответы

**Как организовать инкремент PG → {{ ydb-short-name }}?**  
Debezium (logical replication) → топик {{ ydb-short-name }} (Kafka API) → `CREATE TRANSFER` с lambda под Debezium envelope.

**Как организовать инкремент {{ ydb-short-name }} → PG?**  
`ADD CHANGEFEED` (`DEBEZIUM_JSON`, `INITIAL_SCAN`) → Kafka Connect JDBC Sink → PostgreSQL.

**Есть ли готовый двунаправленный sync?**  
Нет. Собирается из двух однонаправленных пайплайнов с явной моделью master/replica.
