# Архитектура поиска данных в Tiering

## Обзор

Когда пользователь выполняет запрос к колоночной таблице с настроенным tiering, данные могут находиться на разных уровнях хранения:
- **Hot tier** (DefaultStorageId) — данные в YDB на локальных дисках
- **Cold tier** (S3) — данные, эвиктированные в S3 через механизм TTL

YDB обеспечивает **прозрачный и параллельный доступ** к данным из всех тиров без участия приложения.

## Компоненты системы

### 1. KQP (Kikimr Query Processor)
**Роль:** Точка входа для всех SQL-запросов

- Парсит и оптимизирует SQL запрос
- Создает физический план выполнения
- Определяет, какие шарды (ColumnShard) нужно задействовать
- Запускает **Scan Compute Actor** для каждого шарда

**Код:** `ydb/core/kqp/`

**Важно:** KQP не знает о том, где физически находятся данные (YDB или S3). Для него это прозрачно.

---

### 2. Scan Compute Actor (TKqpScanComputeActor)
**Роль:** Координатор выполнения scan-операций на стороне compute

**Функции:**
- Управляет потоком данных между ColumnShard и compute engine
- Контролирует backpressure (через ack-механизм)
- Агрегирует результаты от разных fetcher'ов
- Применяет дополнительные фильтры и проекции

**Файл:** `ydb/core/kqp/compute_actor/kqp_scan_compute_actor.h`

**Взаимодействие:**
```
User Query → KQP → TKqpScanComputeActor → TColumnShardScan
                                       ↓
                                  Fetcher Actors
```

---

### 3. ColumnShard Scan Actor (TColumnShardScan)
**Роль:** Главный актор, координирующий чтение данных из ColumnShard

**Функции:**
- Получает запрос от KQP Scan Compute Actor
- Создает **ScanIterator** для обхода данных
- Управляет memory limits и flow control
- Координирует параллельное чтение данных
- Отправляет батчи данных обратно в compute

**Файл:** `ydb/core/tx/columnshard/engines/reader/actor/actor.h`

**Ключевая структура:**
```cpp
class TColumnShardScan {
    TReadMetadataBase::TConstPtr ReadMetadataRange;  // Метаданные о том, что читать
    std::unique_ptr<TScanIteratorBase> ScanIterator;  // Итератор по данным
    IStoragesManager* StoragesManager;                // Менеджер хранилищ (YDB, S3, ...)
}
```

---

### 4. Scan Iterator (TScanIteratorBase)
**Роль:** Итератор, который обходит все источники данных (portions) в правильном порядке

**Реализации:**
- `TColumnShardScanIterator` (trivial reader) - для простых запросов
- `TPlainReadIterator` - для сложных сканов
- `TSimpleReadIterator` - для специализированных случаев

**Функции:**
- Определяет, какие portions (порции данных) попадают в scan
- Создает **Data Sources** для каждой portion
- Управляет порядком чтения (сортировка, merge)
- Координирует параллельное выполнение fetching'а

**Файл:** `ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/iterator.h`

---

### 5. Data Source (IDataSource / TPortionDataSource)
**Роль:** Абстракция над одной **portion** (порцией данных)

**Ключевые моменты:**
- Каждая portion может находиться на **любом tier** (hot/cold)
- Data Source **не знает**, где физически лежит portion — это решает Storage Operator
- Для каждой portion создается отдельный Data Source
- Data Sources обрабатываются **параллельно**

**Файл:** `ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h`

```cpp
class IDataSource {
    TPortionInfo Portion;          // Метаданные о portion (где лежит, размер, колонки)
    TFetchingScript FetchingPlan;  // План загрузки данных
}
```

---

### 6. Storages Manager (IStoragesManager)
**Роль:** Центральный менеджер всех типов хранилищ

**Функции:**
- Управляет всеми типами storage operators:
  - **DefaultStorageId** — локальное blob storage YDB
  - **Custom tier names** — S3-совместимые хранилища
  - **MemoryStorageId** — in-memory кеш
- Создает и кеширует операторы для каждого storage
- Координирует tiering операции

**Файл:** `ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h`

```cpp
class IStoragesManager {
    THashMap<TString, std::shared_ptr<IBlobsStorageOperator>> Constructed;
    
    std::shared_ptr<IBlobsStorageOperator> GetOperator(const TString& storageId);
}
```

**Как определяется storage для portion:**
```cpp
TString tierName = portion.GetTierNameDef(IStoragesManager::DefaultStorageId);
auto storageOp = storagesManager->GetOperator(tierName);
```

---

### 7. Storage Operator (IBlobsStorageOperator)
**Роль:** Абстракция над физическим хранилищем данных

**Реализации:**
- **TBlobStorageOperator** — для локального YDB blob storage
- **TS3Operator** (tier operator) — для S3-совместимых хранилищ
- **TMemoryOperator** — для in-memory кеша

**Функции:**
- Создает **Reading Actions** для чтения блобов
- Управляет GC (garbage collection)
- Контролирует лимиты (memory, IOPS)
- Предоставляет абстракцию над физическим хранилищем

**Файл:** `ydb/core/tx/columnshard/blobs_action/abstract/storage.h`

```cpp
class IBlobsStorageOperator {
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() = 0;
    virtual void DoOnTieringModified(...) = 0;
}
```

---

### 8. Reading Action (IBlobsReadingAction)
**Роль:** Актуальная операция чтения блобов из хранилища

**Для локального storage:**
- Использует BlobStorage API YDB
- Читает через tablet pipe
- Кеширует часто используемые блобы

**Для S3 storage:**
- Использует AWS SDK S3 Client
- Применяет настройки из `aws_client_config`:
  - `MaxConnectionsCount` — параллельные соединения
  - `ExecutorThreadsCount` — потоки для I/O
  - `RequestTimeoutMs` — таймауты
- Применяет ограничения из `resource_broker_config`:
  - `queue_restore` — CPU limit для restore операций

**Файлы:**
- `ydb/core/tx/columnshard/blobs_action/blob_storage/` — для YDB
- `ydb/core/tx/columnshard/blobs_action/tier/` — для S3

---

### 9. Tiering Manager (ITiersManager)
**Роль:** Управление политиками tiering для таблиц

**Функции:**
- Хранит конфигурацию tiering rules (какие данные, когда и куда эвиктировать)
- Определяет, на каком tier должна быть portion
- Уведомляет Storage Manager об изменениях в политиках
- Интегрируется с Background Operations для автоматической эвикции

**Связь с поиском:**
```
Portion metadata содержит TierName → Tiering Manager знает S3 bucket
                                   ↓
                            StoragesManager получает правильный operator
                                   ↓
                            S3 Operator читает данные
```

---

### 10. Granule (TGranuleMeta)
**Роль:** Логическое разбиение данных внутри таблицы (аналог range partition)

**Функции:**
- Содержит набор portions (порций данных)
- Управляет метаданными о диапазонах ключей
- Координирует compaction в рамках granule
- **Каждая granule обрабатывается независимо и параллельно**

**Portions внутри granule:**
- Могут находиться на разных tiers
- Могут читаться параллельно
- Сортируются по primary key для merge

**Файл:** `ydb/core/tx/columnshard/engines/column_engine_logs.h`

---

## Поток выполнения запроса с Tiering

### Шаг 1: Инициация запроса

```
User SQL Query
     ↓
KQP (Query Processor)
     ↓
TKqpScanComputeActor
     ↓
TColumnShardScan (Scan Actor на ColumnShard)
```

### Шаг 2: Построение плана чтения

```cpp
TColumnShardScan::Bootstrap() {
    // 1. Получить метаданные о таблице и запросе
    ReadMetadataRange = ...;
    
    // 2. Создать итератор
    ScanIterator = CreateIterator(ReadMetadataRange);
    
    // 3. Итератор определяет, какие granules и portions нужно читать
    // ВАЖНО: Portions могут быть на разных tiers!
}
```

### Шаг 3: Определение расположения данных

```cpp
// Для каждой portion:
for (auto& portion : portions) {
    TString tierName = portion.GetTierNameDef(DefaultStorageId);
    // tierName может быть:
    // - "" или "default" → данные в YDB
    // - "s3-cold-logs" → данные в S3 (пример имени tier)
    
    auto storageOp = storagesManager->GetOperator(tierName);
    // Получаем правильный оператор (YDB или S3)
}
```

### Шаг 4: Параллельное чтение из разных tiers

```cpp
// Scan Iterator создает Data Sources для каждой portion
for (auto& portion : selectedPortions) {
    auto dataSource = CreateDataSource(portion);
    
    // Data Source инициирует чтение через Storage Operator
    dataSource->StartFetching() {
        TString tierName = portion.GetTierName();
        auto storage = storagesManager->GetOperator(tierName);
        
        auto readAction = storage->StartReadingAction();
        // Если tier = YDB:   readAction = BlobStorage read
        // Если tier = S3:    readAction = S3 SDK GetObject
        
        readAction->Start(blobIds);
    }
}

// ВСЕ Data Sources работают параллельно!
```

### Шаг 5: Параллелизм на уровне компонентов

**Источники параллелизма:**

1. **Granule-уровень:**
   - Разные granules могут обрабатываться разными scan actors
   
2. **Portion-уровень:**
   - Внутри одного scan'а portions читаются параллельно
   - Каждая portion = отдельный Data Source
   - Data Sources обрабатываются в **thread pool**
   
3. **Blob-уровень:**
   - Одна portion состоит из нескольких блобов
   - Блобы читаются параллельно через Storage Operator
   
4. **Tier-уровень:**
   - **YDB portions и S3 portions читаются одновременно**
   - S3 читает через свой пул соединений (`MaxConnectionsCount`)
   - YDB читает через свой BlobStorage channel

**Конфигурация параллелизма:**

```yaml
# Для S3 (низкий уровень)
aws_client_config:
  max_connections_count: 32          # Параллельные HTTP соединения к S3
  executor_threads_count: 32         # I/O потоки для S3 операций

# Для background операций (высокий уровень)
resource_broker_config:
  queues:
    - name: queue_restore
      limit:
        cpu: 2                        # Параллельные restore/read задачи
```

### Шаг 6: Merge и возврат результатов

```cpp
TColumnShardScan::ProduceResults() {
    // 1. Получить батчи от всех Data Sources
    std::vector<arrow::RecordBatch> batches;
    
    for (auto& source : sources) {
        if (source->IsReady()) {
            batches.push_back(source->GetBatch());
        }
    }
    
    // 2. Merge данных из разных sources (включая разные tiers)
    //    Сортировка по PK, применение фильтров
    auto merged = MergeBatches(batches);
    
    // 3. Отправить результат в compute
    SendToCompute(merged);
}
```

---

## Diagram: Архитектура поиска с Tiering

```
┌─────────────────────────────────────────────────────────────┐
│                        User Query                           │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                   KQP (Query Processor)                     │
│  • Parse SQL                                                │
│  • Optimize plan                                            │
│  • Determine shards                                         │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│             TKqpScanComputeActor (Compute)                  │
│  • Flow control (backpressure)                              │
│  • Aggregates results                                       │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│          TColumnShardScan (Scan Actor)                      │
│  • Creates ScanIterator                                     │
│  • Manages memory limits                                    │
│  • Coordinates parallel reading                             │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│              TScanIteratorBase (Iterator)                   │
│  • Determines which granules/portions to read               │
│  • Creates Data Sources                                     │
│  • Manages read ordering                                    │
└───────────────┬───────────────────────┬─────────────────────┘
                │                       │
       ┌────────▼────────┐     ┌────────▼────────┐
       │   Granule 1     │     │   Granule 2     │
       │  (Range A-M)    │     │  (Range N-Z)    │
       └────────┬────────┘     └────────┬────────┘
                │                       │
    ┌───────────┴────────┐  ┌───────────┴────────┐
    │                    │  │                    │
┌───▼──────┐  ┌─────▼────┐ ┌▼──────┐  ┌────▼────┐
│ Portion1 │  │ Portion2 │ │Portion3│ │Portion4 │
│ (Hot)    │  │ (Cold-S3)│ │(Hot)   │ │(Cold-S3)│
└───┬──────┘  └─────┬────┘ └┬───────┘ └────┬────┘
    │               │       │              │
    │      IDataSource      │              │
    │       (per portion)   │              │
    └───────┬───────────────┴──────────────┘
            │
┌───────────▼──────────────────────────────────────────┐
│              IStoragesManager                        │
│  • GetOperator("default") → BlobStorage              │
│  • GetOperator("s3-cold") → S3 Operator              │
└───────────┬──────────────────────────┬───────────────┘
            │                          │
    ┌───────▼────────┐        ┌────────▼──────────┐
    │ BlobStorage    │        │   S3 Operator     │
    │ Operator       │        │                   │
    │ (YDB Local)    │        │ • AWS SDK Client  │
    │                │        │ • max_conn=32     │
    │ • Tablet Pipe  │        │ • threads=32      │
    │ • Cache        │        │ • queue_restore   │
    └───────┬────────┘        └────────┬──────────┘
            │                          │
    ┌───────▼────────┐        ┌────────▼──────────┐
    │   YDB Blobs    │        │    S3 Bucket      │
    │  (Hot Tier)    │        │   (Cold Tier)     │
    └────────────────┘        └───────────────────┘

              PARALLEL READING FROM ALL TIERS
                           ↓
                  Merge + Sort by PK
                           ↓
                   Return to User
```

---

## Как обеспечивается параллельный поиск из разных тиров?

### Механизм параллелизма

1. **Одинаковая абстракция для всех tiers:**
   ```cpp
   // Код одинаковый для YDB и S3!
   auto readAction = storageOperator->StartReadingAction();
   readAction->ReadBlobs(blobIds);
   ```

2. **Асинхронная обработка:**
   - Каждый Data Source работает независимо
   - Callback-driven architecture (actor model)
   - Нет блокировок между sources

3. **Resource Management:**
   ```cpp
   // YDB blob storage:
   - Использует tablet pipe channels (параллелизм на уровне BS)
   - Локальные операции (быстрые)
   
   // S3 storage:
   - Использует AWS SDK executor thread pool (executor_threads_count)
   - Параллельные HTTP connections (max_connections_count)
   - Resource Broker ограничивает общую нагрузку (cpu limit)
   ```

4. **Intelligent Scheduling:**
   - Iterator определяет порядок обработки portions
   - Prefetch данных (читать заранее)
   - Flow control (не читать больше, чем можем обработать)

---

## Конфигурация для оптимизации параллельного поиска

### Для S3 Tier (Cold Storage)

```yaml
aws_client_config:
  # Сетевой уровень
  max_connections_count: 64          # ↑ для большей пропускной способности
  executor_threads_count: 32         # ↑ для более параллельного I/O
  
  # Таймауты
  request_timeout_ms: 30000
  connection_timeout_ms: 5000
  
resource_broker_config:
  queues:
    - name: queue_restore             # Для чтения из S3
      weight: 100
      limit:
        cpu: 4                        # ↑ для большего параллелизма
```

### Для YDB Tier (Hot Storage)

```yaml
# Управляется через BlobStorage конфигурацию
# Параллелизм определяется:
# - Количеством дисков в storage pool
# - BlobStorage channels
# - VDisk workers
```

---

## Performance Considerations

### Факторы, влияющие на скорость

1. **Latency tiers:**
   - YDB (hot): 1-10ms
   - S3 (cold): 50-200ms (первый байт), 100MB/s+ throughput
   
2. **Predicate Pushdown:**
   - Фильтры применяются **до** чтения блобов
   - Минимизация данных, читаемых из S3
   - Использование min/max indexes, bloom filters
   
3. **Chunk-level parallelism:**
   - Одна portion = множество column chunks
   - Chunks читаются параллельно
   - Колонки, не нужные для запроса, не читаются

4. **Caching:**
   - Часто используемые S3 blocks могут кешироваться
   - Metadata всегда в памяти (быстрый доступ)

### Best Practices

1. **Правильная настройка tiering:**
   ```sql
   -- Эвиктировать старые данные
   ALTER TABLE logs SET (
     TTL = Interval('P90D') TO EXTERNAL DATA SOURCE s3_cold
   );
   ```

2. **Партиционирование:**
   - Разбивать таблицу по времени
   - Старые партиции целиком в S3 (быстрее)
   
3. **Predicate pushdown:**
   ```sql
   -- Хороший запрос (использует min/max indexes)
   SELECT * FROM logs 
   WHERE timestamp > '2024-01-01' AND user_id = 123;
   
   -- Плохой запрос (требует full scan)
   SELECT * FROM logs 
   WHERE SUBSTRING(message, 1, 5) = 'ERROR';
   ```

4. **Мониторинг:**
   ```
   # Смотреть метрики через Embedded UI
   - CompactedPortionsBytes (hot)
   - InsertedPortionsBytes (hot)
   - CommittedPortionsBytes (hot + cold)
   ```

---

## Summary: Кто обеспечивает параллельный поиск?

| Компонент | Роль в параллелизме |
|-----------|---------------------|
| **TScanIteratorBase** | Создает параллельные Data Sources для portions |
| **IDataSource** | Независимая обработка каждой portion |
| **IStoragesManager** | Маршрутизация к правильному Storage Operator |
| **IBlobsStorageOperator** | Параллельные операции чтения (YDB или S3) |
| **AWS SDK (для S3)** | Параллельные HTTP connections + I/O threads |
| **Resource Broker** | Контроль общей нагрузки (CPU limits) |
| **Actor System** | Асинхронная, неблокирующая обработка |

**Итог:** Параллелизм встроен на всех уровнях архитектуры. Данные из YDB и S3 читаются **одновременно**, merge происходит **по мере готовности** батчей из разных sources.
