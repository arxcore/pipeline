# Scale Analysis - Macro Data Pipeline

**Scaling Analysis from 2 to 200 Countries**  
**Based on:** codebase_tracing.md, documentasi_pipeline.md  
**Date:** 2026-08-14

---

## TABLE OF CONTENTS

1. [CURRENT BASELINE](#1-current-baseline)
2. [SCALING MODEL](#2-scaling-model)
3. [BOTTLENECK ANALYSIS](#3-bottleneck-analysis)
4. [FAILURE ISOLATION](#4-failure-isolation)
5. [IDEMPOTENCY AND DEDUPLICATION](#5-idempotency-and-deduplication)
6. [DATABASE SCALE](#6-database-scale)
7. [ORCHESTRATION SCALE](#7-orchestration-scale)
8. [CONFIGURATION SCALE](#8-configuration-scale)
9. [RECOMMENDED TARGET ARCHITECTURE](#9-recommended-target-architecture)
10. [PRIORITIZED OPTIMIZATION ROADMAP](#10-prioritized-optimization-roadmap)

---

## 1. CURRENT BASELINE

### Current State (Verified from Codebase)

| Dimension | Current Value | Source |
|-----------|---------------|--------|
| Countries | 2 | catalog.yaml (usa, uk) |
| Categories | 11 | 6 (usa) + 5 (uk) |
| Indicators | ~50-60 | Estimated from YAML files |
| Providers | 4 | BLS, FRED, BEA, ONS |
| Provider Types | 2 | API (3), File (1) |
| API Indicators | ~35-45 | USA indicators (BLS, FRED, BEA) |
| File Indicators | ~15-20 | UK indicators (ONS) |

### Current Resource Usage (Estimated)

| Resource | Current Usage | Notes |
|----------|---------------|-------|
| API Requests | ~50-60 per full run | 1 per indicator |
| File Downloads | ~15-20 per full run | 1 per ONS indicator |
| DB Rows (raw) | ~50-60 per run | Only new data inserted |
| DB Rows (staging) | ~50-60 per run | UPSERT on existing |
| DB Connections | 7 max | Connection pool size |
| HTTP Connections | 4 sessions | 1 per provider |
| Memory | Low | All metadata in memory |

### Current Performance (Estimated)

| Operation | Time Estimate | Notes |
|-----------|---------------|-------|
| Startup | ~1-2s | Session opening, table creation |
| API Fetch (BLS/FRED/BEA) | ~5-10s | Concurrent with semaphore(5) |
| File Download (ONS) | ~30-60s | Sequential with delays |
| Parsing | ~1-5s | Depends on data size |
| DB Write | ~1-2s | Batch inserts |
| **Total** | **~1-2 minutes** | Full pipeline, all indicators |

---

## 2. SCALING MODEL

### Scaling Dimensions

The pipeline scales across multiple dimensions:

```
Countries (C)
    │
    ├── Categories per Country (Cat)
    │       │
    │       ├── Indicators per Category (I)
    │       │       │
    │       │       ├── Provider Type (API/File)
    │       │       │
    │       │       ├── Frequency (monthly, weekly, quarterly)
    │       │       │
    │       │       └── Historical Periods (Years)
    │       │
    │       └── Total Indicators per Country = Σ(I)
    │
    └── Total Indicators = C × (Average Indicators per Country)
```

### Scaling Scenarios

| Scenario | Countries | Indicators | API Requests | File Downloads |
|----------|-----------|------------|--------------|----------------|
| Current | 2 | ~50-60 | ~50-60 | ~15-20 |
| 10x | 20 | ~500-600 | ~500-600 | ~150-200 |
| 50x | 100 | ~2500-3000 | ~2500-3000 | ~750-1000 |
| 100x | 200 | ~5000-6000 | ~5000-6000 | ~1500-2000 |

### Linear vs Superlinear Scaling

| Component | Scaling Type | Reason |
|-----------|--------------|--------|
| API Requests | Linear | 1 request per API indicator |
| File Downloads | Linear | 1 download per file indicator |
| DB Writes | Linear | 1 write per indicator |
| Network Bandwidth | Linear | Proportional to data size |
| HTTP Sessions | Constant | 1 session per provider (not per indicator) |
| DB Connections | Constant | Fixed pool size (7) |
| Memory (Metadata) | Linear | ALL_INDICATORS loaded in memory |
| Startup Time | Linear | Sequential session opening |
| Parsing CPU | Linear to Quadratic | Depends on data size per indicator |
| Storage (DB) | Linear | 1 row per indicator |
| Storage (Files) | Linear | 1 file per ONS indicator |

**Superlinear Risks:**
- **DB Connection Pool:** Fixed at 7, may become bottleneck (linear growth in DB operations)
- **Provider Rate Limits:** BLS has daily quota of 500 - at ~100 countries, quota may be exhausted
- **Parsing Complexity:** Excel parsing with Calamine may not scale linearly with file size/complexity
- **Concurrency Limits:** Semaphore sizes fixed, may limit parallelism

---

## 3. BOTTLENECK ANALYSIS

### Network Bottlenecks

#### HTTP Requests

| Provider | Current Concurrency | Scaling Issue |
|---------|---------------------|---------------|
| BLS | Semaphore(5) | May hit daily quota at scale |
| FRED | Semaphore(5) | No daily quota coded, but provider may have limits |
| BEA | Semaphore(5) + delay | May hit provider limits |
| ONS | Semaphore(1) | Sequential downloads, slow at scale |

**Analysis:**
- **BLS:** Daily quota of 500 requests. At ~100 countries with ~25 indicators each = 2500 indicators. With BLS being ~60% of USA indicators, ~1500 BLS requests needed. **This exceeds the 500 daily quota by 3x.**
- **FRED/BEA:** No coded limits, but providers likely have quotas
- **ONS:** Sequential with 10-15s delays. 2000 file indicators × 12s = 24000s = **6.7 hours** just for downloads

**Recommendation:**
- Implement rate limit tracking for all providers (not just BLS)
- Add backpressure mechanism when approaching quotas
- Increase ONS concurrency (currently 1)
- Implement batching for BLS (supports up to 30 series per request)

#### Connection Pooling

**Current:** max_size=7 connections

**Scaling Issue:**
- Each DB operation (insert, query) needs a connection
- With 5000 indicators:
  - Fetch stage: 5000 inserts to raw_respons_api/file_registry
  - Parse stage: 5000 inserts to staging_indicators
  - At 7 connections, batch inserts needed

**Current Implementation:**
- Uses `executemany()` for batch inserts - good
- But each load method creates its own connection/transaction
- No connection reuse across operations

**Recommendation:**
- Increase pool size (P0)
- Implement connection reuse across operations
- Consider async batch processing with backpressure

### Concurrency Bottlenecks

#### Provider Session Opening

**Current:** Sequential
```python
for p in self.providerd:
    await self.providerd[p].__aenter__()
```

**Scaling Issue:**
- With 4 providers currently: ~4 × connection_time
- If adding more provider types: scales linearly
- TODO comment acknowledges this is a bottleneck

**Recommendation:**
- Open all provider sessions concurrently with `asyncio.gather()` (F-002)

#### Global Concurrency Limits

**Current:**
- Per-provider semaphores: 5 (API), 1 (ONS)
- No global concurrency limit
- No per-country or per-indicator limits

**Scaling Issue:**
- At 200 countries, 5000 indicators, all fetched concurrently
- Could overwhelm network, memory, or external services
- No way to limit total concurrent operations

**Recommendation:**
- Add global semaphore for max concurrent indicators
- Add per-country concurrency limits
- Implement priority queue for important indicators

### Provider Layer Bottlenecks

#### API-Specific Constraints

| Provider | Constraint | Impact at Scale |
|---------|------------|-----------------|
| BLS | Daily quota 500 | Will be exhausted at ~100 indicators |
| FRED | Unknown | Risk of hitting unknown limits |
| BEA | Unknown + random delay | May have quotas, delays help |
| ONS | Sequential + delays | Very slow at scale |

**Recommendation:**
- Research and implement all provider quotas
- Add quota tracking for all providers
- Implement distributed quota management (if running multiple instances)

#### File-Based Constraints

**ONS Issues:**
- Full file downloads (not granular queries)
- Excel parsing is expensive (Polars + Calamine)
- Boundary detection adds overhead
- Sequential processing

**Recommendation:**
- Increase ONS concurrency (currently 1)
- Cache parsed results
- Consider incremental file downloads (if ONS supports range queries)

### Parser Bottlenecks

#### CPU Usage

**Current:**
- Polars for CSV parsing (fast)
- Polars + Calamine for Excel parsing (slower)
- All parsing happens in process (not async)

**Scaling Issue:**
- At 5000 indicators, parsing becomes CPU-bound
- Excel files particularly expensive
- No parallel CPU processing (asyncio is I/O-bound)

**Recommendation:**
- Profile parsing performance
- Consider multiprocessing for CPU-bound parsing
- Cache parsed results
- Optimize Calamine usage

#### Memory Usage

**Current:**
- Each file loaded into Polars DataFrame
- Multiple files parsed concurrently (for API, concurrent parsing)

**Scaling Issue:**
- Large Excel files could consume significant memory
- No memory limits or monitoring

**Recommendation:**
- Add memory monitoring
- Implement streaming/chunked parsing for large files
- Add memory limits and backpressure

### Database Bottlenecks

#### Connection Pool Size

**Current:** max_size=7

**Scaling Issue:**
- Each insert/query needs a connection
- With 5000 indicators:
  - ~5000 raw_respons_api inserts
  - ~5000 file_registry inserts (ONS)
  - ~5000 staging_indicators inserts
  - Total: ~10000-15000 DB operations
  - At 7 connections: ~2100-3300 operations per connection
  - With transaction overhead: could be slow

**Recommendation:**
- Increase pool size to 20-50 (P0)
- Use larger batch sizes
- Consider async batch processing

#### Index Performance

**Current:**
- Indexes on checksum, source, country, code_name for raw_respons_api
- Index on lookup columns for staging_indicators

**Scaling Issue:**
- JSONB indexing may slow down with many rows
- Unique constraints require index lookups

**Recommendation:**
- Monitor index performance
- Consider partitioning by date or country
- Optimize JSONB queries

#### Write Throughput

**Current:**
- Batch inserts using `executemany()`
- No batch size optimization
- Each load method has its own transaction

**Scaling Issue:**
- 5000 rows per table per run
- Transaction size limited by DB configuration
- Network round trips for each batch

**Recommendation:**
- Optimize batch sizes (current code doesn't specify)
- Use COPY command for bulk loads (if using PostgreSQL directly)
- Consider larger transactions

### Configuration Bottlenecks

#### Metadata Loading

**Current:**
- All metadata loaded at module import
- Stored in `ALL_INDICATORS` global variable
- In-memory dictionary structure

**Scaling Issue:**
- At 200 countries, ~5000 indicators:
  - Memory: ~5000 × ~500 bytes per indicator = ~2.5 MB (manageable)
  - Loading time: Linear with number of YAML files
  - Filtering: Linear search through nested dicts

**Recommendation:**
- Keep in memory (acceptable)
- Optimize filtering (currently O(n) where n = all indicators)
- Consider lazy loading for very large scale

---

## 4. FAILURE ISOLATION

### Current Behavior

#### Indicator-Level Isolation

**Status:** GOOD

- Individual indicator failures are captured by `asyncio.gather(return_exceptions=True)`
- Logged and skipped
- Pipeline continues with successful indicators
- Error counts tracked and reported

**Example:**
```python
results = await asyncio.gather(*tasks, return_exceptions=True)
for result in results:
    if isinstance(result, BaseException):
        logger.exception("Error task, skiping %s indicator..", name)
        error_count += 1
        continue
```

#### Provider-Level Isolation

**Status:** PARTIAL

- Each provider has its own session
- Provider failures affect only that provider's indicators
- BUT: No circuit breaker - repeated failures continue to retry

#### Country-Level Isolation

**Status:** NONE

- No country-level isolation
- All countries processed together
- No way to resume specific country

#### Pipeline-Level Isolation

**Status:** POOR

- Database errors cause immediate `SystemExit(1)` (F-004)
- No partial success at pipeline level
- Entire pipeline stops on any DB error

### Scaling Impact

At 200 countries:
- **GOOD:** Individual indicator failures handled gracefully
- **POOR:** DB error stops entire pipeline (could lose 199 countries due to 1 failure)
- **NONE:** No country-level isolation or resume capability

### Recommendations

| Isolation Level | Current | Recommended | Priority |
|----------------|---------|-------------|----------|
| Indicator | Good | Maintain | P0 |
| Provider | Partial | Add circuit breaker | P0 |
| Country | None | Add country-level tracking | P1 |
| Pipeline | Poor | Graceful degradation | P0 |
| Resume | None | Track progress, resume capability | P1 |

---

## 5. IDEMPOTENCY AND DEDUPLICATION

### Current Implementation

#### API Providers (BLS, FRED, BEA)

**Deduplication:** Checksum-based
```python
checksum = hashlib.sha256(
    json.dumps(raw_data.raw_respons, sort_keys=True).encode()
).hexdigest()

INSERT INTO raw_respons_api (payload) VALUES (%s)
ON CONFLICT ((payload -> 'meta' ->> 'checksum')) DO NOTHING
```

**Idempotency:** YES - Same response produces same checksum, insert is skipped

**Revision Handling:** YES - If data changes, new checksum, new row inserted

#### File Providers (ONS)

**Deduplication:** ETag + file_path based
```python
INSERT INTO file_registry (...) VALUES (...)
ON CONFLICT (file_path, country, category, indicator) DO NOTHING
```

**Idempotency:** YES - Same file path, same indicator: skipped

**ETag Handling:** YES - 304 Not Modified returns existing file

#### Staging Table

**UPSERT:**
```python
INSERT INTO staging_indicators (...) VALUES (...)
ON CONFLICT (date, source, code, country, frequency)
DO UPDATE SET value = EXCLUDED.value, ...
```

**Idempotency:** YES - Same date/source/code/country/frequency: updates value

### Scaling Analysis

| Aspect | Current | At Scale | Notes |
|--------|---------|----------|-------|
| API Deduplication | Checksum | Scales well | SHA-256 is fast |
| File Deduplication | ETag + path | Scales well | Path uniqueness may be issue at scale |
| Staging Deduplication | Composite key | Scales well | Unique constraint efficient |
| Storage Growth | Linear | Manageable | ~1 row per indicator per fetch |
| Historical Tracking | Full | Storage concern | All revisions kept |

**Concerns at Scale:**
1. **Storage Growth:** With 5000 indicators and daily runs, raw_respons_api could grow quickly
2. **Checksum Collisions:** SHA-256 collisions theoretically possible but astronomically unlikely
3. **Path Uniqueness:** File path generation uses hash + timestamp + uuid - should be unique

### Recommendations

| Issue | Recommendation | Priority |
|-------|---------------|----------|
| Storage Growth | Add retention policy | P2 |
| Historical Tracking | Consider archival for old data | P2 |
| Path Uniqueness | Current approach is good | N/A |

---

## 6. DATABASE SCALE

### Row Growth

| Table | Current Rows | At 200 Countries | Growth Rate |
|-------|--------------|-------------------|-------------|
| raw_respons_api | ~50-60 | ~5000-6000 | Linear (new data only) |
| file_registry | ~15-20 | ~1500-2000 | Linear (new data only) |
| staging_indicators | ~50-60 | ~5000-6000 | Linear (UPSERT) |
| **Total** | ~115-140 | ~11500-14000 | Linear |

**Assumptions:**
- Each indicator has ~1 data point per fetch (simplified)
- Actual data points depend on frequency and historical range
- API indicators produce JSON with multiple data points
- File indicators produce files with multiple rows

### JSONB Storage

**Current:**
- Full JSON response stored in payload JSONB column
- Average response size: ~1-10 KB (estimated)
- At 5000 indicators: ~50-500 MB for raw_respons_api

**Scaling:**
- 200 countries, 5000 indicators: ~50-500 MB
- With historical data (multiple runs): Could grow to GBs
- JSONB indexing: GIN indexes on JSON fields have overhead

**Recommendations:**
- Monitor JSONB storage size
- Consider compressing large responses
- Archive old raw data
- Partition by date or country

### Index Performance

**Current Indexes:**
```sql
-- raw_respons_api
CREATE UNIQUE INDEX idx_unique_checksum ON ((payload -> 'meta' ->> 'checksum'))
CREATE INDEX idx_meta_source ON ((payload -> 'meta' ->> 'source'))
CREATE INDEX idx_meta_country ON ((payload -> 'meta' ->> 'country'))
CREATE INDEX idx_meta_codename ON ((payload -> 'meta' ->> 'code_name'))

-- staging_indicators
CREATE UNIQUE INDEX ... ON (date, source, code, country, frequency)
CREATE INDEX idx_stg_lookup ON (code, country, date)
```

**Scaling Issues:**
- JSONB functional indexes (using `->` and `->>`) have performance overhead
- At millions of rows, query performance may degrade
- Insert performance with many indexes

**Recommendations:**
- Monitor index performance at scale
- Consider materialized columns instead of JSONB functional indexes
- Consider partitioning
- Optimize queries

### Write Throughput

**Current:**
- Batch inserts using `executemany()`
- No explicit batch size (uses default)
- Each load operation has its own transaction

**Estimated Write Volume:**
- 5000 indicators × ~100 data points each = 500,000 rows in staging_indicators
- 5000 rows in raw_respons_api
- 1500 rows in file_registry
- Total: ~506,500 rows per full run

**At 7 connections:**
- ~72,000 rows per connection per run
- With transaction overhead: acceptable for PostgreSQL

**Recommendations:**
- Increase pool size to 20-50 (P0)
- Optimize batch sizes (explicitly set to 1000-5000)
- Use COPY for bulk loads
- Consider async batch processing

### Connection Count

**Current:** max_size=7

**At Scale:**
- Need to handle 500,000+ rows per run
- With batch inserts: 7 connections × 1000 rows/batch = 7000 rows/round trip
- For 500,000 rows: ~72 round trips
- This is acceptable but could be faster with more connections

**Recommendations:**
- Increase to max_size=20-50 (P0)
- Monitor connection usage
- Consider connection pool tuning

### Partitioning

**Current:** No partitioning

**Scaling Benefit:**
- Partition by date: Queries for recent data faster
- Partition by country: Queries for specific country faster
- Partition by source: Queries for specific provider faster

**Recommendations:**
- Add partitioning by date for raw_respons_api (P1)
- Add partitioning by country for file_registry (P1)
- Consider partitioning for staging_indicators (P2)

### Retention

**Current:** No retention policy - all data kept indefinitely

**Scaling Issue:**
- Storage grows indefinitely
- Query performance may degrade with old data
- Backup size increases

**Recommendations:**
- Add retention policy (e.g., keep raw data for 90 days) (P2)
- Implement archival to cold storage (P2)
- Consider data lifecycle management

---

## 7. ORCHESTRATION SCALE

### Current Orchestration

**Structure:**
- Single process, single machine
- Asyncio for concurrency
- CLI-driven execution
- No scheduling or workflow management

### Scaling Capabilities

| Capability | Current | Needed for 200 Countries |
|-----------|---------|--------------------------|
| Scheduled Runs | No | Yes (cron, Airflow, etc.) |
| Retries | Per-request only | Pipeline-level retry |
| Partial Retries | No | Yes (retry failed countries) |
| Failed-Country Reruns | No | Yes |
| Provider-Level Reruns | No | Yes |
| Backfills | No | Yes |
| Historical Replay | Yes (--stage replay) | Enhance |
| Concurrency Controls | Basic | Advanced |
| Observability | Basic logging | Full metrics/monitoring |
| Long-Running | Yes (async) | Yes |

### Recommendations

| Capability | Recommendation | Priority |
|-----------|---------------|----------|
| Scheduling | Add Airflow/Prefect/Dagster | P1 |
| Pipeline Retry | Add pipeline-level retry logic | P0 |
| Partial Retry | Track failed indicators, retry specific | P1 |
| Resume | Track progress, resume from checkpoint | P1 |
| Concurrency | Add advanced concurrency controls | P0 |
| Monitoring | Add metrics collection | P1 |

---

## 8. CONFIGURATION SCALE

### Current Configuration

**Metadata Structure:**
```
config/metadata/
├── catalog.yaml          # Country → Categories
├── usa/
│   ├── price.yaml        # ~5-10 indicators
│   ├── labour.yaml       # ~5-10 indicators
│   └── ...
└── uk/
    └── ...
```

**At 200 Countries:**
```
config/metadata/
├── catalog.yaml          # 200 countries, ~5-10 categories each = 1000-2000 entries
├── country1/
│   ├── category1.yaml    # Indicators
│   └── ...
├── country2/
│   └── ...
└── country200/
    └── ...
```

### Scaling Issues

1. **File Count:**
   - Current: ~12 YAML files
   - At scale: ~1000-2000 YAML files (200 countries × 5-10 categories)
   - File system limits: Usually fine (ext4 supports millions of files)

2. **Loading Time:**
   - Current: ~10-100ms to load all YAML
   - At scale: Linear with file count
   - 2000 files × 50ms = 100 seconds (too slow)

3. **Memory:**
   - Current: ~2.5 MB for 50 indicators
   - At scale: ~250 MB for 5000 indicators (acceptable on modern machines)

4. **Discoverability:**
   - Current: Manual editing of YAML files
   - At scale: Need search, validation, management tools

5. **Duplication:**
   - Current: Manual entry, risk of duplication
   - At scale: Need deduplication, validation

### Recommendations

| Issue | Recommendation | Priority |
|-------|---------------|----------|
| Loading Time | Lazy loading, caching | P1 |
| File Count | Consider database-backed metadata | P2 |
| Discoverability | Add metadata search/index | P2 |
| Validation | Add schema validation for YAML | P1 |
| Duplication | Add deduplication checks | P1 |

### Recommended Metadata Architecture

**Option 1: Database-Backed Metadata (Recommended)**
```
metadata_indicators table:
    id, country, category, indicator_name, code_name, source, freq,
    start_year, start_month, calc, unit, description, sheet_name,
    dataset, table, line_number, url, is_active, created_at, updated_at

metadata_countries table:
    id, code, name, is_active

metadata_categories table:
    id, code, name, country_id
```

**Benefits:**
- Fast querying/filtering
- Easy to manage via UI or API
- Scalable to thousands of indicators
- Built-in validation
- Auditable

**Option 2: Optimized YAML Structure**
```yaml
# catalog.yaml
countries:
  usa:
    name: United States
    categories:
      price:
        name: Price Indicators
        indicators:
          CPI_YoY:
            code_name: CUUR0000SA0
            source: bls
            ...
```

**Benefits:**
- Single file or fewer files
- Faster loading
- Easier to manage

**Drawbacks:**
- Large files may be hard to edit
- Merge conflicts
- Still need validation

---

## 9. RECOMMENDED TARGET ARCHITECTURE

### Current AS-IS vs Recommended TO-BE

#### Current AS-IS

```
┌─────────────────────────────────────────────────────────┐
│                    Single Process                            │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                  Python Asyncio                        │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │   Fetch      │  │   Parse     │  │   Load      │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │  │
│  │                                                         │  │
│  │  ┌─────────────────────────────────────────────┐    │  │
│  │  │                    Providers                        │    │  │
│  │  └─────────────────────────────────────────────┘    │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                    PostgreSQL                          │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │ raw_data    │  │ file_reg    │  │ staging     │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

**Characteristics:**
- Single process, single machine
- CLI-driven
- All-or-nothing execution
- Limited error handling
- Fixed concurrency limits
- Manual scheduling

#### Recommended TO-BE (200 Countries)

```
┌─────────────────────────────────────────────────────────┐
│              Orchestration Layer (Airflow/Prefect)          │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              Pipeline Scheduler                        │  │
│  │  - DAG definitions                                    │  │
│  │  - Trigger: time-based, manual, API                    │  │
│  │  - Retry policies                                    │  │
│  │  - Monitoring & alerting                              │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────┐
│              Pipeline Worker (Containerized)               │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                    Pipeline Process                     │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │
│  │  │   Metadata  │  │   Concurrency │  │   Progress   │    │  │
│  │  │   Service   │  │   Manager    │  │   Tracking   │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │  │
│  │                                                         │  │
│  │  ┌─────────────────────────────────────────────┐    │  │
│  │  │                  Process Layer                       │    │  │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐│  │  │
│  │  │  │   Fetch      │  │   Parse     │  │   Load      ││  │  │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘│  │  │
│  │  └─────────────────────────────────────────────┘    │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
         │                                  │                                  │
         ▼                                  ▼                                  ▼
┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│   Provider       │              │   Provider       │              │   Provider       │
│   Service 1      │              │   Service 2      │              │   Service N      │
│   (BLS/FRED)     │              │   (BEA)          │              │   (New)          │
└─────────────────┘              └─────────────────┘              └─────────────────┘
         │                                  │                                  │
         └──────────────────────────────────┬──────────────────────────────┘
                                          ▼
                               ┌─────────────────────┐
                               │   External Services  │
                               │   (APIs, File Servers)│
                               └─────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────┐
│                    Data Storage                              │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              PostgreSQL Cluster                        │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │ raw_respons_api │  │ file_registry │  │ staging_indic│  │  │
│  │  │ (partitioned) │  │ (partitioned) │  │   ators      │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │  │
│  │                                                         │  │
│  │  ┌─────────────────────────────────────────────┐    │  │
│  │  │              Metadata Tables                       │    │  │
│  │  │  (replaces YAML files)                              │    │  │
│  │  └─────────────────────────────────────────────┘    │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              Cold Storage/Archive                      │  │
│  │  - Old raw data                                        │  │
│  │  - Backup data                                         │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│              Transformation Layer                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                    dbt Cloud                            │  │
│  │  - Scheduled runs                                     │  │
│  │  - Version control                                    │  │
│  │  - Documentation                                      │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│              Observability Layer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  Logging     │  │  Metrics     │  │  Monitoring  │        │
│  │  (ELK)       │  │  (Prom)     │  │  (Grafana)   │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────┘
```

**Characteristics:**
- Distributed/multi-process architecture
- Scheduled and event-driven execution
- Full observability stack
- Database-backed metadata
- Partitioned tables
- Connection pooling and concurrency controls
- Progress tracking and resume capability
- Graceful error handling
- Scalable storage

### Component Boundaries

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| Orchestration | Schedule, trigger, retry, monitor | Airflow/Prefect/Dagster |
| Pipeline | Execute pipeline stages | Python/Asyncio |
| Metadata | Store and manage indicator metadata | PostgreSQL |
| Providers | Fetch data from external sources | Python/aiohttp |
| Process | Parse and transform data | Python/Polars |
| Storage | Store raw and processed data | PostgreSQL |
| Archive | Long-term storage | S3/Blob Storage |
| Transform | Business logic calculations | dbt |
| Observability | Logging, metrics, monitoring | ELK/Prometheus/Grafana |

---

## 10. PRIORITIZED OPTIMIZATION ROADMAP

### Priority Definitions

| Priority | Definition | When to Implement |
|----------|------------|-------------------|
| P0 | Required before 200 countries | Before scaling |
| P1 | Strongly recommended | Before or during early scaling |
| P2 | Optimization after scale | After reaching scale |

---

### P0 - Required Before 200 Countries

#### P0-01: Fix Non-PipelineCrash Exception Handling

**Problem:** Non-PipelineCrash exceptions (ValueError, TypeError, KeyError) not caught in main(), propagate to asyncio.run() with unhelpful messages

**Evidence:** main.py only catches `exc.PipelineCrash`

**Why it matters:** At scale, unexpected errors will cause confusing failures

**Recommended change:** Add broad exception handler in main()

```python
# In main.py
try:
    await orch.runner(cfg)
except exc.PipelineCrash as e:
    logger.exception("Error during execution pipeline: %s", e)
    print(f"\nFull traceback: {traceback.format_exc()}")
    raise SystemExit(1)
except Exception as e:
    logger.exception("Unexpected error during execution: %s", e)
    print(f"\nFull traceback: {traceback.format_exc()}")
    raise SystemExit(1)
```

**Expected impact:** All errors properly logged and reported

**Complexity:** Low

**Risk:** Low

**When:** Immediately

---

#### P0-02: Increase Database Connection Pool Size

**Problem:** Connection pool fixed at 7, may become bottleneck with 5000+ indicators

**Evidence:** max_size=7 hardcoded in main.py

**Why it matters:** DB operations will queue up, slowing pipeline

**Recommended change:** Increase to 20-50, make configurable

```python
# In main.py or config
pool = AsyncConnectionPool(
    conninfo=CONN_STR,
    min_size=5,
    max_size=20,  # Increased from 7
    max_waiting=50,  # Increased from 30
    timeout=10,
)
```

**Expected impact:** 3-5x improvement in DB write throughput

**Complexity:** Low

**Risk:** Low (PostgreSQL can handle hundreds of connections)

**When:** Before scaling to 200 countries

---

#### P0-03: Add Graceful Database Error Handling

**Problem:** DB errors cause immediate SystemExit(1), stopping entire pipeline

**Evidence:** All DB load methods raise SystemExit(1) on error

**Why it matters:** At 200 countries, losing 199 due to 1 DB error is unacceptable

**Recommended change:** Replace SystemExit with proper error propagation

```python
# In upload/postgres/*.py
# Instead of:
except psycopg_pool.PoolTimeout:
    logger.error("Connection pool timeout...")
    raise SystemExit(1)

# Use:
except psycopg_pool.PoolTimeout as e:
    logger.error("Connection pool timeout: %s", e)
    raise exc.UploadFailed("Connection pool timeout") from e
```

Then in orchestration layer, handle UploadFailed with retry or partial success.

**Expected impact:** Pipeline can continue after DB errors

**Complexity:** Medium

**Risk:** Medium (need to ensure data consistency)

**When:** Before scaling

---

#### P0-04: Parallel Provider Session Opening

**Problem:** Sequential session opening is a bottleneck (acknowledged in TODO)

**Evidence:** RawProcessors.__aenter__() opens sessions sequentially

**Why it matters:** Startup time scales linearly with providers

**Recommended change:** Use asyncio.gather() to open sessions concurrently

```python
# In RawProcessors.__aenter__()
async def __aenter__(self):
    open_session: list[str] = []
    
    # Create tasks for all providers
    tasks = [
        self._open_provider_session(p) 
        for p in self.providerd
    ]
    
    # Execute concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check for errors
    for i, p in enumerate(self.providerd):
        if isinstance(results[i], Exception):
            # Close all opened
            for o in reversed(open_session):
                await self.providerd[o].__aexit__(None, None, None)
            raise results[i]
        open_session.append(p)
    
    return self

async def _open_provider_session(self, provider_name: str):
    await self.providerd[provider_name].__aenter__()
    return provider_name
```

**Expected impact:** 4x faster startup (from ~500ms to ~125ms for 4 providers)

**Complexity:** Low

**Risk:** Low

**When:** Before scaling

---

#### P0-05: Implement Provider Quota Tracking for All Providers

**Problem:** Only BLS has quota tracking; FRED and BEA may have unknown limits

**Evidence:** Only BLS checks for "daily threshold" in response

**Why it matters:** At scale, hitting unknown quotas will cause failures

**Recommended change:** Add quota tracking for all API providers

```python
# Add to Resources class
class Resources(BaseSettings):
    # ... existing ...
    bls_daily_quota: int = 500
    fred_daily_quota: int | None = None  # Research actual limit
    bea_daily_quota: int | None = None   # Research actual limit

# Add to each provider
class BaseAPIProvider:
    def __init__(self, api_key, daily_quota=None):
        self.daily_quota = daily_quota
        self.daily_request_count = 0
        self.limit_event = asyncio.Event() if daily_quota else None
    
    def check_quota(self):
        if self.limit_event and self.limit_event.is_set():
            return False
        if self.daily_quota and self.daily_request_count >= self.daily_quota:
            if self.limit_event:
                self.limit_event.set()
            return False
        return True
```

**Expected impact:** Prevents quota exhaustion failures

**Complexity:** Medium

**Risk:** Low

**When:** Before scaling

---

### P1 - Strongly Recommended

#### P1-01: Add Circuit Breaker for Provider Failures

**Problem:** No circuit breaker - repeated failures to same provider continue indefinitely

**Evidence:** Only retry with max 5 attempts, no circuit breaker pattern

**Why it matters:** At scale, repeated failures waste resources and delay pipeline

**Recommended change:** Add circuit breaker using tenacity or custom implementation

```python
from tenacity import CircuitBreakerError, circuit_breaker

# Add to provider fetch_data
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(...),
    retry=retry_if_exception(Retryable()),
    reraise=True,
)
@circuit_breaker(failure_threshold=5, recovery_timeout=300)
async def fetch_data(...):
    ...
```

**Expected impact:** Prevents cascading failures

**Complexity:** Low (tenacity supports circuit breaker)

**Risk:** Low

**When:** Before or during early scaling

---

#### P1-02: Add Country-Level Progress Tracking and Resume

**Problem:** No way to resume from failure - must restart entire pipeline

**Evidence:** No progress tracking, no checkpointing

**Why it matters:** At 200 countries, 8-hour pipeline failing at 99% means restarting from 0%

**Recommended change:** Track progress, implement resume capability

```python
# Add progress tracking table
CREATE TABLE pipeline_progress (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT,  # running, completed, failed
    total_indicators INTEGER,
    completed INTEGER,
    failed INTEGER
);

CREATE TABLE indicator_progress (
    run_id UUID REFERENCES pipeline_progress(run_id),
    country TEXT,
    category TEXT,
    indicator TEXT,
    status TEXT,  # pending, running, completed, failed
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE (run_id, country, category, indicator)
);

# In fetch_config_indicators()
for country, categories in filtered_indicators.items():
    for category, indicators in categories.items():
        for indicator_name, meta in indicators.items():
            # Check if already completed in this run
            if await self.fetch_db.is_indicator_completed(run_id, country, category, indicator_name):
                continue
            
            # Mark as running
            await self.fetch_db.mark_indicator_running(run_id, country, category, indicator_name)
            
            try:
                # Process indicator
                tasks.append(...)
            except Exception as e:
                # Mark as failed
                await self.fetch_db.mark_indicator_failed(run_id, country, category, indicator_name, str(e))
```

**Expected impact:** Can resume from last checkpoint

**Complexity:** Medium-High

**Risk:** Medium

**When:** Before or during early scaling

---

#### P1-03: Increase ONS Concurrency

**Problem:** ONS downloads are sequential (Semaphore(1)) with 10-15s delays

**Evidence:** self.semaphore = asyncio.Semaphore(limit_requests=1) in ONSProvider

**Why it matters:** At scale, file downloads will be the slowest part

**Recommended change:** Increase concurrency, reduce delays

```python
# In ONSProvider.__init__
self.semaphore = asyncio.Semaphore(limit_requests=5)  # Increased from 1

# In fetch_data
# Reduce delay
await asyncio.sleep(random.uniform(1, 3))  # Reduced from 10-15
```

**Considerations:**
- ONS server may have rate limits
- Monitor for 429 errors
- May need to adjust based on actual ONS limits

**Expected impact:** 5-15x faster file downloads

**Complexity:** Low

**Risk:** Medium (risk of hitting rate limits)

**When:** Before scaling

---

#### P1-04: Add Scheduling (Airflow/Prefect)

**Problem:** No scheduling - pipeline must be run manually

**Evidence:** CLI-driven only, no scheduler integration

**Why it matters:** At scale, need automated, scheduled runs

**Recommended change:** Add Airflow/Prefect/Dagster integration

```python
# Example Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_pipeline():
    import subprocess
    result = subprocess.run(
        ["python", "extract/src/main.py", "--stage", "all"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Pipeline failed: {result.stderr}")

with DAG(
    dag_id="macro_data_pipeline",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    run_pipeline = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
```

**Expected impact:** Automated, scheduled pipeline execution

**Complexity:** Medium (new infrastructure)

**Risk:** Low

**When:** Before scaling

---

#### P1-05: Add Metrics and Monitoring

**Problem:** Only basic logging to stdout, no metrics collection

**Evidence:** Logging only, no Prometheus/Grafana integration

**Why it matters:** At scale, need visibility into performance, errors, bottlenecks

**Recommended change:** Add Prometheus metrics

```python
# Add to main.py
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# Metrics
INDICATOR_FETCHED = Counter(
    'pipeline_indicators_fetched_total',
    'Total indicators fetched',
    ['country', 'provider', 'status']  # success, error, skip
)
INDICATOR_PARSED = Counter(
    'pipeline_indicators_parsed_total',
    'Total indicators parsed',
    ['country', 'provider', 'status']
)
PIPELINE_DURATION = Histogram(
    'pipeline_duration_seconds',
    'Pipeline execution duration',
    buckets=[30, 60, 120, 300, 600, 1200]
)

# Start metrics server
start_http_server(8000)

# In fetch_config_indicators
for result in results:
    if isinstance(result, BaseException):
        INDICATOR_FETCHED.labels(country=..., provider=..., status='error').inc()
    elif isinstance(result, ApiResult):
        INDICATOR_FETCHED.labels(country=..., provider=..., status='success').inc()
    # etc.
```

**Expected impact:** Full visibility into pipeline performance

**Complexity:** Low (prometheus_client is simple)

**Risk:** Low

**When:** Before scaling

---

#### P1-06: Optimize Batch Insert Sizes

**Problem:** No explicit batch size optimization for DB inserts

**Evidence:** Uses executemany() with default batch behavior

**Why it matters:** At scale, batch size affects performance significantly

**Recommended change:** Explicit batch sizes, tune for performance

```python
# In load_raw_respons
BATCH_SIZE = 1000  # Tune based on testing

for i in range(0, len(data), BATCH_SIZE):
    batch = data[i:i + BATCH_SIZE]
    await acur.executemany(
        "INSERT INTO raw_respons_api (payload) VALUES (%s) ON CONFLICT ...",
        [(Json(payload),) for payload in batch],
    )
```

**Expected impact:** 10-50% improvement in DB write performance

**Complexity:** Low

**Risk:** Low

**When:** Before scaling

---

### P2 - Optimization After Scale

#### P2-01: Database-Backed Metadata

**Problem:** YAML-based metadata doesn't scale to 200 countries

**Evidence:** Manual YAML editing, file count growth

**Why it matters:** Managing thousands of YAML files is impractical

**Recommended change:** Migrate metadata to database tables

**Expected impact:** Easier metadata management

**Complexity:** Medium

**Risk:** Medium (migration required)

**When:** After reaching 50+ countries

---

#### P2-02: Add Data Retention Policy

**Problem:** No retention policy - raw data grows indefinitely

**Evidence:** No deletion or archival logic

**Why it matters:** Storage costs, query performance

**Recommended change:** Add TTL to raw_respons_api, archive old data

```python
# Add to LoadRaw.load_raw_respons
# Delete old data
DELETE FROM raw_respons_api 
WHERE load_at < NOW() - INTERVAL '90 days';

# Or partition by date and drop old partitions
```

**Expected impact:** Controlled storage growth

**Complexity:** Low

**Risk:** Low (if retention period is reasonable)

**When:** After reaching scale

---

#### P2-03: Add Table Partitioning

**Problem:** No partitioning - queries may slow down with millions of rows

**Evidence:** Single tables for all data

**Why it matters:** Query performance, maintenance

**Recommended change:** Partition tables by date

```sql
-- Example for raw_respons_api
CREATE TABLE raw_respons_api (
    id BIGSERIAL,
    payload JSONB NOT NULL,
    load_at TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (load_at);

-- Create monthly partitions
CREATE TABLE raw_respons_api_y2026m01 PARTITION OF raw_respons_api
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

**Expected impact:** Faster queries, easier maintenance

**Complexity:** Medium (requires migration)

**Risk:** Medium

**When:** After reaching scale

---

#### P2-04: Add Parallel CPU Processing for Parsing

**Problem:** Parsing is CPU-bound, no parallel processing

**Evidence:** All parsing happens in asyncio event loop (I/O-bound only)

**Why it matters:** At scale, parsing may become CPU bottleneck

**Recommended change:** Use multiprocessing for CPU-bound parsing

```python
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

async def parse_all(data_list):
    # Split data into batches
    batch_size = 100
    batches = [data_list[i:i+batch_size] for i in range(0, len(data_list), batch_size)]
    
    # Use process pool for CPU-bound work
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, parse_batch, batch) for batch in batches]
        results = await asyncio.gather(*tasks)
    
    return [item for batch in results for item in batch]

def parse_batch(batch):
    # CPU-bound parsing in process pool
    return [parse_item(item) for item in batch]
```

**Expected impact:** Better CPU utilization, faster parsing

**Complexity:** Medium (need to handle process communication)

**Risk:** Medium (serialization overhead, complexity)

**When:** After reaching scale and CPU becomes bottleneck

---

#### P2-05: Add API Batching for BLS

**Problem:** BLS supports batch requests (up to 30 series per request), but not utilized

**Evidence:** Each BLS indicator fetched separately

**Why it matters:** Reduces number of requests, better utilizes BLS quota

**Recommended change:** Batch BLS requests

```python
# In BLSProvider.fetch_data
async def fetch_data(self, meta, category, country, indicator_name):
    # Group by BLS provider
    # Batch up to 30 series per request
    
    # Or better: modify fetch_config_indicators to group by provider
    # and batch requests
```

**Expected impact:** 10-30x fewer BLS requests

**Complexity:** Medium (requires orchestration changes)

**Risk:** Low

**When:** After reaching scale

---

## SUMMARY

### Immediate Actions (Before 200 Countries)

1. **P0-01:** Fix exception handling in main()
2. **P0-02:** Increase DB connection pool size
3. **P0-03:** Add graceful DB error handling
4. **P0-04:** Parallel provider session opening
5. **P0-05:** Add quota tracking for all providers
6. **P1-03:** Increase ONS concurrency
7. **P1-04:** Add scheduling (Airflow/Prefect)
8. **P1-05:** Add metrics and monitoring
9. **P1-06:** Optimize batch insert sizes

### Early Scaling (During Initial Scale-Up)

1. **P1-01:** Add circuit breaker for provider failures
2. **P1-02:** Add country-level progress tracking and resume

### Post-Scale Optimization (After Reaching Scale)

1. **P2-01:** Database-backed metadata
2. **P2-02:** Add data retention policy
3. **P2-03:** Add table partitioning
4. **P2-04:** Add parallel CPU processing
5. **P2-05:** Add API batching for BLS

### Estimated Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| P0 | 2-4 weeks | Error handling, DB pool, gracefulness, concurrency |
| P1 | 4-8 weeks | Circuit breaker, resume, scheduling, monitoring |
| P2 | Ongoing | Partitioning, retention, metadata migration |

---

*Document provides analysis and recommendations based on actual codebase implementation.*
