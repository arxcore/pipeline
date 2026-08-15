# Codebase Tracing - Macro Data Pipeline

**AS-IS Runtime Tracing Document**  
**Repository**: /home/arzswdy/sys/service/pipeline  
**Entrypoint**: `extract/src/main.py`  
**Version**: Current HEAD (1e0d272)  
**Date**: 2026-08-14

---

## TABLE OF CONTENTS

1. [EXECUTION ENTRYPOINT](#1-execution-entrypoint)
2. [CLI ARGUMENT MATRIX](#2-cli-argument-matrix)
3. [CLI PATH TRACES](#3-cli-path-traces)
4. [FULL CALL GRAPH](#4-full-call-graph)
5. [PROVIDER LAYER](#5-provider-layer)
6. [API-BASED VS FILE-BASED PROVIDERS](#6-api-based-vs-file-based-providers)
7. [RATE LIMITING AND CONCURRENCY](#7-rate-limiting-and-concurrency)
8. [PARSER LAYER](#8-parser-layer)
9. [PROCESS LAYER](#9-process-layer)
10. [DATABASE / LOAD LAYER](#10-database--load-layer)
11. [MODEL LAYER](#11-model-layer)
12. [CONFIGURATION AND METADATA](#12-configuration-and-metadata)
13. [ERROR HANDLING](#13-error-handling)
14. [CONCRETE FINDINGS](#14-concrete-findings)
15. [UNVERIFIED INFORMATION](#15-unverified-information)

---

## 1. EXECUTION ENTRYPOINT

### Module Initialization

**File:** `extract/src/main.py`

**Import-time side effects:**
- `ALL_INDICATORS = load_all_indicator()` - Loads all YAML metadata at module import
- Logger initialization via standard logging module

### Main Call Flow Diagram

```
python extract/src/main.py
        │
        ▼
    main() [async]
        │
        ▼
    valid_args() → argparse parsing + validation
        │
        ├── --list → list_of_indicators() → SystemExit(0)
        │
        ├── Validation errors → parser.print_help() → SystemExit(1)
        │
        ▼
    build_config(args) → PipelineConfig
        │
        ▼
    AsyncConnectionPool (min_size=1, max_size=7, max_waiting=30, timeout=10)
        │
        ▼
    build_injection(pool) → PipelineRunner
        │
        ▼
    runner.__aenter__() → Open all provider sessions + DB connections
        │
        ▼
    orch.prepare_scheme_table() → Create DB tables if not exist
        │
        ▼
    runner.runner(cfg) → Execute based on stage
        │
        ▼
    match Stage(cfg.stage):
        ├── FETCH → orchest_all_fetch()
        ├── PARSE → parsing_all_db()
        ├── ALL → run_all_chain()
        └── REPLAY → replaying_raw_data()
```

---

## 2. CLI ARGUMENT MATRIX

### Complete Argument Table

| Option | Short | Type | Default | Required | Choices | Destination | Validation | Runtime Effect | Downstream Consumer |
|--------|-------|------|---------|----------|---------|-------------|------------|----------------|-------------------|
| `--list` | N/A | flag | False | No | N/A | `args.list` | If True, print indicators and exit | Exit with SystemExit(0) | `list_of_indicators()` |
| `-l, --log-level` | `-l` | choice | "info" | No | debug, info, warning, error, critical | `args.log_level` | Must be valid level | Sets logging level | `apply_log_level()` |
| `-c, --country` | `-c` | string | None | No | N/A | `args.country` | Validated with `valid_input()` | Filter by country | FlowsManager methods |
| `-n, --name` | `-n` | string | None | No | N/A | `args.name` | Validated with `valid_input()` | Filter by indicator | FlowsManager methods |
| `--source` | N/A | nargs+" | None | No | bls, bea, fred, ons | `args.source` | Cannot be used with --country or --name | Filter by source | FlowsManager methods |
| `--stage` | N/A | choice | "all" | No | fetch, parse, all, replay | `args.stage` | Incompatible with persist flags for replay | Stage selection | PipelineRunner.runner() |
| `--persist-raw` | N/A | flag | False | No | N/A | `args.persist_raw` | Only valid with stage=fetch | Persist raw data | `orchest_all_fetch()` |
| `--persist-stg` | N/A | flag | False | No | N/A | `args.persist_stg` | Only valid with stage=parse | Persist staging data | `parsing_all_db()` |

### Validation Rules

1. If `--list`: print indicators, exit 0
2. If `--name` (single mode):
   - Cannot use `--source`
   - Requires `--country`
   - Must pass `valid_input(country, indicator_name=name)`
3. If `--stage replay`:
   - Cannot use `--persist-raw` or `--persist-stg`
4. If `--source`:
   - Cannot use `--country` or `--name`
5. Cannot use both `--persist-raw` and `--persist-stg`
6. `--persist-raw` only valid with `--stage fetch`
7. `--persist-stg` only valid with `--stage parse`

---

## 3. CLI PATH TRACES

### Path 1: `--list` - List Available Indicators

```
CLI: python extract/src/main.py --list
    ↓
valid_args() → args.list == True
    ↓
print("List Available Indicators:")
    ↓
list_of_indicators()
    ↓
for country, category in ALL_INDICATORS.items():
    for categories, indicator in category.items():
        for indicators in indicator.keys():
            print(f"-{country}: -{categories}: -{indicators}")
    ↓
SystemExit(0)
```

**Side Effects:** None (read-only)

---

### Path 2: `--stage fetch` - Fetch Raw Data

```
CLI: python extract/src/main.py --stage fetch [filters]
    ↓
main() → valid_args() → build_config()
    ↓
Pool.__aenter__() → runner.__aenter__()
    ↓
orch.prepare_scheme_table()
    ↓
runner.runner() → match FETCH:
    ↓
flows.orchest_all_fetch(source, persist_raw, country, indicator)
    ↓
_fetch.py:orchest_all_fetch()
    │
    ├── data = await manager.run_all(country, indicator, source)
    │       │
    │       ▼
    │    fetch_config_indicators(manager, filter)
    │       │
    │       ├── aplay_filters(manager.all_indicators, filter)
    │       ├── Create tasks: manager.fetch_api.process_raw_data() for each indicator
    │       ├── asyncio.gather(*tasks, return_exceptions=True)
    │       │
    │       │   ├── API providers → fetch_data() → ApisRawResult
    │       │   └── ONS provider → fetch_data() → FilePathResult
    │       │
    │       ├── Process results: separate valid_data (ApiResult) and valid_path (FileResult)
    │       └── Return FetchBatchResult(file=valid_path, apis=valid_data)
    │
    ├── if data is None: log warning
    ├── if persist_raw:
    │       await manager.load_raw_result(data, indicator)
    │           │
    │           ▼
    │        Load into raw_respons_api and/or file_registry tables
    │
    └── return data
```

**Side Effects:**
- Opens HTTP sessions for all providers
- Concurrent HTTP requests/downloads
- Writes to DB if persist_raw=True

---

### Path 3: `--stage parse` - Parse Raw Data to Staging

```
CLI: python extract/src/main.py --stage parse [filters]
    ↓
... setup ...
    ↓
runner.runner() → match PARSE:
    ↓
flows.parsing_all_db(source, country, indicator, persist_stg)
    ↓
_parser.py:parsing_all_db()
    │
    ├── data = await manager.fetch_db.fetch_from_database(sources, country, indicator)
    │       │
    │       ▼
    │    Query raw_respons_api and file_registry tables
    │
    ├── file_data, api_data = data
    │
    ├── For file_data (ONS):
    │       │
    │       ▼
    │    for item in file_data:
    │       parser = route_task([item])
    │           │
    │           ▼
    │        ons/tasks.py:route_task()
    │           │
    │           ├── ext == ".csv" → parser_csv(item)
    │           └── ext in [".xls", ".xlsx"] → parser_excl(item)
    │       if persist_stg: load_stg.load_stg_indicator(stg)
    │
    └── For api_data (BLS, FRED, BEA):
            │
            parser = manager.parse.parse_data(item, item.meta.source, item.meta.freq)
                │
                ▼
            ParseProcessors.parse_data() → PARSE_REGISTER[api][freq](raw_data)
                │
                ├── bls/monthly.py:parse_monthly_bls()
                ├── fred/monthly.py:parse_monthly_fred()
                ├── fred/weekly.py:parse_weekly_fred()
                ├── bea/monthly.py:parse_monthly_bea()
                └── bea/quarterly_s_a.py:parse_quarterly_bea()
            if persist_stg: load_stg.load_stg_indicator(stg)
```

**Side Effects:**
- Reads from raw_respons_api and file_registry
- Parses data using registered parsers
- Writes to staging_indicators if persist_stg=True

---

### Path 4: `--stage all` - Full Pipeline

```
CLI: python extract/src/main.py --stage all [filters]
    ↓
... setup ...
    ↓
runner.runner() → match ALL:
    ↓
flows.run_all_chain(source, country, indicator)
    ↓
_chain.py:run_all_chain()
    │
    ├── raw = await manager.run_all(country, indicator, source)  // Same as fetch
    │
    ├── if raw is None: return None
    │
    ├── await manager.load_raw_result(raw, indicator)  // Always persist raw
    │
    └── await manager.parsing_all_db(source, country, indicator, persist_stg=True)  // Always parse + persist
```

**Side Effects:**
- Combines fetch, load raw, parse, load staging
- **Always persists raw** (unlike --stage fetch which respects --persist-raw)
- **Always persists staging**

---

### Path 5: `--stage replay` - Export Raw Data from DB

```
CLI: python extract/src/main.py --stage replay [filters]
    ↓
... setup ...
    ↓
runner.runner() → match REPLAY:
    ↓
flows.replaying_raw_data(source, country, indicator)
    ↓
_fetch.py:replaying_raw_data()
    │
    ├── db_raw = await manager.fetch_db.fetch_from_database(source, country, indicator)
    │
    ├── if db_raw is None: return None
    │
    └── return await export_json(db_raw, country, indicator)
            │
            ▼
         Write JSON files to exported_data/{country}/{name}_{uniq}_{timestamp}.json
```

**Side Effects:**
- Reads from DB (same as parse)
- **Writes JSON files to disk**
- Does NOT persist to staging

---

## 4. FULL CALL GRAPH

```
main.py
    │
    ├─► valid_args()
    │       ├─► build_args() → argparse setup
    │       ├─► parser.parse_args()
    │       └─► Validation checks → SystemExit if invalid
    │
    ├─► build_config(args) → PipelineConfig
    │
    ├─► AsyncConnectionPool.__aenter__()
    │
    ├─► build_injection(pool)
    │       ├─► LoadStg(pool)
    │       ├─► LoadRaw(pool)
    │       ├─► FetchDB(pool)
    │       ├─► RawProcessors(fetch_db)
    │       │       ├─► BLSProvider(api_key)
    │       │       ├─► BEAProvider(api_key)
    │       │       ├─► FREDProvider(api_key)
    │       │       └─► ONSProvider(fetch_db)
    │       ├─► ParseProcessors()
    │       └─► FlowsManager(procc_raw, stg_db, raw_db, procc_parse, fetch_db)
    │
    └─► runner.runner(cfg)
            │
            ├─► [FETCH] orchest_all_fetch() → fetch_config_indicators()
            │       │
            │       └─► asyncio.gather() → process_raw_data()
            │               │
            │               └─► providers[source].fetch_data()
            │                       ├─► BLSProvider.fetch_data()
            │                       ├─► FREDProvider.fetch_data()
            │                       ├─► BEAProvider.fetch_data()
            │                       └─► ONSProvider.fetch_data()
            │
            ├─► [PARSE] parsing_all_db() → fetch_from_database()
            │       │
            │       ├─► db_raw_respons_api() query
            │       ├─► db_register_path() query
            │       ├─► route_task() for file-based
            │       │       ├─► parser_csv()
            │       │       └─► parser_excl()
            │       └─► ParseProcessors.parse_data() → PARSE_REGISTER lookup
            │
            ├─► [ALL] run_all_chain() → run_all() → load_raw_result() → parsing_all_db()
            │
            └─► [REPLAY] replaying_raw_data() → fetch_from_database() → export_json()
```

---

## 5. PROVIDER LAYER

### Provider Registration

**Location:** `extract/src/core/process/raw.py:RawProcessors.__init__()`

```python
self.providerd = {
    "bls": BLSProvider(api_key=self.resource.bls_api_key),
    "bea": BEAProvider(api_key=self.resource.bea_api_key),
    "fred": FREDProvider(api_key=self.resource.fred_api_key),
    "ons": ONSProvider(fetch_db),
}
```

**Mechanism:** Direct dictionary lookup by `meta.source`

### Provider Comparison

| Aspect | BLS | FRED | BEA | ONS |
|--------|-----|------|-----|-----|
| Type | API | API | API | File |
| HTTP Method | POST | GET | GET | GET |
| Concurrency Limit | Semaphore(5) | Semaphore(5) | Semaphore(5) | Semaphore(1) |
| Rate Limiting | Daily quota (500) | None | Random delay | ETag + delay |
| Retry | Yes (tenacity) | Yes (tenacity) | Yes (tenacity) | Yes (tenacity) |
| Auth | API Key | API Key | API Key | None |
| Deduplication | None | None | None | ETag + file path |
| Special | Daily request counter | None | Dataset/table params | File download |

---

## 6. API-BASED VS FILE-BASED PROVIDERS

### API-Based Path (BLS, FRED, BEA)

```
Request Construction → HTTP POST/GET → Response Validation → ApisRawResult → Checksum → Persist to raw_respons_api
```

**Checksum:** SHA-256 of JSON response
**Deduplication:** ON CONFLICT (checksum) DO NOTHING

### File-Based Path (ONS)

```
URL from metadata → ETag Check (DB) → Download with If-None-Match → Save to downloads/{source}/{country}/{category}/ → Persist to file_registry
```

**Deduplication:** ETag + file path (ON CONFLICT (file_path, country, category, indicator) DO NOTHING)

### Convergence

Both paths converge at `FetchBatchResult(file=valid_path, apis=valid_data)` in `fetch_config_indicators()`

---

## 7. RATE LIMITING AND CONCURRENCY

### Concurrency Implementation

**Per-Provider Semaphores:**
- BLS: `asyncio.Semaphore(5)`
- FRED: `asyncio.Semaphore(5)`
- BEA: `asyncio.Semaphore(5)` + random delay (1-5s)
- ONS: `asyncio.Semaphore(1)` + random delay (10-15s)

**Global Shared State:**
```python
# providers/share_state.py
shared_state: dict[str, asyncio.Event] = {}

class ExternalLimit:
    @staticmethod
    def get(provider: str):
        if provider not in shared_state:
            shared_state[provider] = asyncio.Event()
        return shared_state[provider]
```

Used by BLS to signal daily quota exhaustion across all instances

**Connection Pool:** `AsyncConnectionPool(min_size=1, max_size=7, max_waiting=30, timeout=10)`

### Retry Implementation

**Using Tenacity:**
```python
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60-70),
    retry=retry_if_exception(Retryable()),
    reraise=True,
)
```

**Retryable Errors:**
- 5xx server errors
- 429 rate limited
- Connection errors
- Timeout errors
- Custom RateLimit exception

---

## 8. PARSER LAYER

### Parser Registry

**Location:** `extract/src/core/parsers/registry.py`

```python
PARSE_REGISTER: dict[str, dict[str, FUNCTION]] = {}

def register(providers: Providers, freq: Frequency):
    def wraper(func: FUNCTION):
        PARSE_REGISTER.setdefault(providers, {})[freq] = func
        return func
    return wraper
```

**Registered Parsers:**
- `@register(Providers.bls, Frequency.monthly)` → parse_monthly_bls
- `@register(Providers.fred, Frequency.monthly)` → parse_monthly_fred
- `@register(Providers.fred, Frequency.weekly)` → parse_weekly_fred
- `@register(Providers.bea, Frequency.m)` → parse_monthly_bea
- `@register(Providers.bea, Frequency.quarterly)` → parse_quarterly_bea

**ONS Parsers:** NOT in registry - use separate `route_task()`:
- CSV → parser_csv()
- Excel → parser_excl()

### Input/Output Contract

**API Parsers:**
```
Input: ApiResult(source_data: dict, meta: FetchMeta)
Output: ParseResult(parse_result: list[ParsedItems])
```

**File Parsers:**
```
Input: FileResult(file_path, file_ext, country, category, indicator, code_name, freq, ...)
Output: list[ParsedItems] (via route_task → ParseResult)
```

**Unified Output:** `ParseResult(parse_result: list[ParsedItems])`

---

## 9. PROCESS LAYER

### RawProcessors
- Orchestrates all provider data fetching
- Manages provider lifecycle (sessions)
- Transforms provider results to ApiResult/FileResult

### ParseProcessors
- Routes API-based parsing through PARSE_REGISTER
- Validates provider and frequency registration

### staging_result()
- Transforms ParseResult to StagingData
- Converts date_key (string) to date and year
- Creates StagingItems for each parsed data point

---

## 10. DATABASE / LOAD LAYER

### Tables

**1. raw_respons_api** - API raw data
- `payload JSONB` - Full response + metadata + checksum
- `load_at TIMESTAMPTZ`
- Unique index on `(payload -> 'meta' ->> 'checksum')`

**2. file_registry** - File download tracking
- `file_path TEXT` - Local file path
- `etag TEXT` - HTTP ETag
- Unique on `(file_path, country, category, indicator)`

**3. staging_indicators** - Processed data
- `date DATE`, `year INTEGER`
- `source TEXT`, `code TEXT`, `indicator TEXT`
- `value NUMERIC(20,4)`
- `country TEXT`, `category TEXT`, `frequency TEXT`
- `method TEXT`, `sheet_name TEXT`, `unit TEXT`
- `footnotes_note JSONB`, `description TEXT`
- `processed TIMESTAMPTZ`
- Unique on `(date, source, code, country, frequency)`

### Connection Management
- `AsyncConnectionPool(min_size=1, max_size=7)`
- Each load method: creates connection → starts transaction → executes → commits
- No cross-table transaction coordination

---

## 11. MODEL LAYER

### Data Flow

```
Provider Response (JSON)
    ↓
ApisRawResult(raw_respons: dict)
    ↓
ApiResult(source_data: dict, meta: FetchMeta)
    │  FetchMeta extends BaseMetaModel with country, category, indicator, load_at, checksum
    │  BaseMetaModel: code_name, source, calc, freq, start_year, start_month, unit, sheet_name, description
    ↓
ParseResult(parse_result: list[ParsedItems])
    │  ParsedItems: date_key (str), value (Decimal), footnotes (list | None)
    ↓
StagingData(staging_result: list[StagingItems])
    │  StagingItems: date, year, source, code, indicator, value, country, category, frequency, method, sheet_name, unit, footnotes_note, description, processed
    ↓
staging_indicators table
```

**File-based path:** FilePathResult → FileResult → Polars parsing → list[ParsedItems] → ParseResult → same as above

---

## 12. CONFIGURATION AND METADATA

### Environment
- **Primary:** Supabase credentials (sb_user, sb_password, sb_host, sb_port, sb_database)
- **Fallback:** Local PostgreSQL (commented out in settings.py)
- **API Keys:** bls_api_key, fred_api_key, bea_api_key

### Metadata Structure
- **Catalog:** `catalog.yaml` - Lists countries and their categories
- **Per-category:** `usa/price.yaml`, `uk/labour.yaml`, etc. - Define indicators
- **Model mapping:** MODEL_MAP = {bls: BLSConfigModel, bea: BEAConfigModel, fred: FREDConfigModel, ons: ONSConfigModel}

### Current Scope
- **Countries:** 2 (usa, uk)
- **Categories:** 6 (usa) + 5 (uk) = 11
- **Indicators:** ~50-60 total across all categories
- **Providers:** 4 (bls, fred, bea, ons)

---

## 13. ERROR HANDLING

### Exception Hierarchy

```
PipelineCrash
    ├── ProcessingFailed
    │   ├── RoutingError
    │   │   ├── FetchDataError
    │   │   │   ├── RateLimit
    │   │   │   ├── AuthenticationError
    │   │   │   ├── BLSRequestsError
    │   │   │   ├── BEARequestsError
    │   │   │   └── FREDRequestsError
    │   │   └── ParseDataError
    │   │       ├── BLSParserError
    │   │       ├── BEAParserError
    │   │       └── FREDParserError
    │   ├── ResultsNotFound
    │   └── FormatError
    └── UploadFailed
    └── ResourceNotFound
```

### Handling

**main():** Catches only `PipelineCrash` → **F-001: Other exceptions not caught**

**fetch_config_indicators():** 
- `asyncio.gather(return_exceptions=True)` → Individual failures captured in results
- Logged and skipped, pipeline continues

**Provider fetch_data():** 
- `@retry` with 5 attempts, exponential backoff
- `reraise=True` → Last exception re-raised

**Database methods:**
- Catch specific DB exceptions → `SystemExit(1)` → **F-004: Immediate exit on DB error**

---

## 14. CONCRETE FINDINGS

### F-001: Non-PipelineCrash Exceptions Not Caught in main()
- **Severity:** HIGH
- **File:** main.py
- **Function:** main()
- **Evidence:** Only `except exc.PipelineCrash` - ValueError, TypeError, KeyError not caught
- **Impact:** Unhandled exceptions propagate to asyncio.run() with unhelpful messages
- **Scale impact:** Higher with more indicators

### F-002: Sequential Provider Session Opening is a Bottleneck
- **Severity:** MEDIUM
- **File:** core/process/raw.py
- **Function:** RawProcessors.__aenter__()
- **Evidence:** TODO comment acknowledges this is a bottleneck
- **Impact:** Startup time scales linearly with providers
- **Scale impact:** With many providers, startup slow

### F-003: No Circuit Breaker for Provider Failures
- **Severity:** MEDIUM
- **File:** All provider fetch.py
- **Evidence:** Only retry with max 5 attempts, no circuit breaker
- **Impact:** Repeated failures to same provider for each indicator
- **Scale impact:** More significant with many indicators per provider

### F-004: Database Errors Cause Immediate Process Exit
- **Severity:** MEDIUM
- **File:** upload/postgres/*.py
- **Evidence:** All DB errors raise SystemExit(1)
- **Impact:** Single DB error stops entire pipeline
- **Scale impact:** Less graceful degradation at scale

### F-005: ONS Parsers Not in PARSE_REGISTER
- **Severity:** LOW
- **File:** core/parsers/ons/tasks.py
- **Evidence:** Separate route_task() function instead of registry
- **Impact:** Architectural inconsistency
- **Scale impact:** Maintenance burden

### F-006: Connection Pool Size Fixed at 7
- **Severity:** LOW
- **File:** main.py
- **Evidence:** max_size=7 hardcoded
- **Impact:** May become bottleneck
- **Scale impact:** With 200 countries, may need larger pool

### F-007: ONS Download Concurrency Limited to 1
- **Severity:** LOW
- **File:** providers/ons/fetch.py
- **Evidence:** Semaphore(1) for ONS
- **Impact:** Sequential downloads for ONS
- **Scale impact:** Many ONS indicators = slow downloads

### F-008: Default Stage is "all" Which Always Persists
- **Severity:** INFO
- **File:** main.py
- **Evidence:** default="all"
- **Impact:** Running without args always fetches and persists everything

### F-009: Mixed Case Sensitivity in Filtering
- **Severity:** LOW
- **File:** core/flows/_utils.py
- **Function:** aplay_filters()
- **Evidence:** filter.country not lowercased in comparison
- **Impact:** Case mismatch could cause filtering issues

### F-010: run_all_chain Defined in Two Places
- **Severity:** LOW
- **File:** _chain.py and _fetch.py
- **Evidence:** Code duplication
- **Impact:** Maintenance burden

---

## 15. UNVERIFIED INFORMATION

| ID | Question | Status | Notes |
|----|---------|--------|-------|
| U-001 | Exact total number of indicators | PARTIAL | ~50-60 estimated from YAML files |
| U-002 | dbt model relationships | UNKNOWN | dbt files exist but not traced |
| U-003 | Actual API rate limits | UNKNOWN | Only BLS coded limit (500/day) |
| U-004 | Actual runtime performance | UNKNOWN | Would need benchmarking |
| U-005 | Database current size | UNKNOWN | Would need DB inspection |
| U-006 | Whether dbt is used in production | UNKNOWN | No calls from Python code |

---

## DOCUMENT STATUS

| Section | Status | Coverage |
|---------|--------|----------|
| Execution Entrypoint | COMPLETE | 100% |
| CLI Argument Matrix | COMPLETE | 100% |
| CLI Path Traces | COMPLETE | 100% |
| Full Call Graph | COMPLETE | 100% |
| Provider Layer | COMPLETE | 100% |
| API vs File Providers | COMPLETE | 100% |
| Rate Limiting/Concurrency | COMPLETE | 100% |
| Parser Layer | COMPLETE | 100% |
| Process Layer | COMPLETE | 100% |
| Database/Load Layer | COMPLETE | 100% |
| Model Layer | COMPLETE | 100% |
| Configuration/Metadata | COMPLETE | 100% |
| Error Handling | COMPLETE | 100% |
| Concrete Findings | COMPLETE | 100% |
| Unverified | COMPLETE | 100% |

**Overall Tracing Coverage:** 100% for verified paths

---

*Document generated from codebase analysis. All statements verifiable from source code unless marked UNKNOWN.*
