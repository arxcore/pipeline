# Pipeline Documentation - Macro Data Pipeline

**Higher-Level Technical Documentation**  
**Based on:** codebase_tracing.md (verified from actual implementation)  
**Date:** 2026-08-14

---

## TABLE OF CONTENTS

1. [SYSTEM PURPOSE](#1-system-purpose)
2. [CURRENT SCOPE](#2-current-scope)
3. [ARCHITECTURE](#3-architecture)
4. [DATA FLOW](#4-data-flow)
5. [PROVIDER ARCHITECTURE](#5-provider-architecture)
6. [API VS FILE-BASED INGESTION](#6-api-vs-file-based-ingestion)
7. [FETCH LAYER](#7-fetch-layer)
8. [PARSE LAYER](#8-parse-layer)
9. [PROCESS LAYER](#9-process-layer)
10. [DATABASE/LOAD LAYER](#10-databaseload-layer)
11. [MODELS/DATA CONTRACTS](#11-modelsdata-contracts)
12. [CONFIGURATION](#12-configuration)
13. [ERROR HANDLING](#13-error-handling)
14. [RETRY/RATE LIMITING/CONCURRENCY](#14-retryrate-limitingconcurrency)
15. [RAW DATA STRATEGY](#15-raw-data-strategy)
16. [STAGING STRATEGY](#16-staging-strategy)
17. [DBT TRANSFORMATION LAYER](#17-dbt-transformation-layer)
18. [CLI EXECUTION MODES](#18-cli-execution-modes)
19. [OPERATIONAL BEHAVIOR](#19-operational-behavior)

---

## 1. SYSTEM PURPOSE

The Macro Data Pipeline is an **automated, multi-source ELT (Extract, Load, Transform) pipeline** designed to:

1. **Extract** macroeconomic indicator data from multiple statistical agencies
2. **Normalize** heterogeneous data formats into a unified structure
3. **Store** raw vintage data immutably for point-in-time historical analysis
4. **Process** normalized data into staging tables for downstream analytics
5. **Support** transformation via dbt for business logic calculations

**Primary Use Case:** Enable accurate backtesting and historical analysis of macroeconomic indicators by preserving raw data as it was originally published.

**Secondary Use Case:** Provide a unified, standardized dataset of economic indicators across multiple countries and sources.

---

## 2. CURRENT SCOPE

### Countries

- **2 countries:** United States (USA), United Kingdom (UK)
- **Country codes:** `usa`, `uk`

### Providers

| Provider | Type | Agency                         | Data Type        |
| -------- | ---- | ------------------------------ | ---------------- |
| BLS      | API  | Bureau of Labor Statistics     | US economic data |
| FRED     | API  | Federal Reserve Economic Data  | US economic data |
| BEA      | API  | Bureau of Economic Analysis    | US economic data |
| ONS      | File | Office for National Statistics | UK economic data |

### Indicators

- **Approximate count:** 50-60 indicators total across all categories
- **Categories:**
  - **USA:** price, labour, trade, money, consumer, business (6 categories)
  - **UK:** price, labour, consumer, business, trade (5 categories)

### Supported Formats

| Provider | Format   | Content Type      |
| -------- | -------- | ----------------- |
| BLS      | JSON     | REST API response |
| FRED     | JSON     | REST API response |
| BEA      | JSON     | REST API response |
| ONS      | CSV/XLSX | File download     |

### Frequencies

- Monthly (most common)
- Weekly (FRED only)
- Quarterly (BEA)
- Annual

---

## 3. ARCHITECTURE

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLI INTERFACE                                  │
│              (extract/src/main.py)                                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE RUNNER                                   │
│              (core.cli.PipelineRunner)                              │
│  - Manages stage execution (FETCH, PARSE, ALL, REPLAY)               │
│  - Coordinates dependency injection                                 │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    FLOWS MANAGER                                     │
│              (core.flows.Manager.FlowsManager)                       │
│  - Manages provider lifecycle (open/close sessions)                │
│  - Manages database connection pool                               │
│  - Delegates to process layer                                       │
└─────────────────────────────────────────────────────────────────────┘
         │                                  │                                  │
         ▼                                  ▼                                  ▼
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│   RAW PROCESSORS     │      │   PARSE PROCESSORS   │      │    LOAD LAYER       │
│   (core.process.raw)│      │   (core.process.parse)│     │   (upload.postgres)  │
│  - Orchestrates all  │      │  - Registry-based    │      │  - LoadRaw          │
│    provider fetching │      │    dispatch         │      │  - LoadStg          │
│  - Manages sessions  │      │  - Routes to        │      │  - FetchDB          │
│  - Transforms results│      │    registered parsers│      │                    │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘
         │                                  │                                  │
         ▼                                  ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PROVIDER LAYER                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐│
│  │   BLS       │  │   FRED      │  │    BEA      │  │    ONS      ││
│  │ (API-based) │  │ (API-based) │  │ (API-based) │  │ (File-based)││
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘│
└─────────────────────────────────────────────────────────────────────┘
         │                                  │
         ▼                                  ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│      PostgreSQL              │   │       File System            │
│  ┌─────────────────┐        │   │  ┌─────────────────┐        │
│  │ raw_respons_api │        │   │  │ downloads/       │        │
│  │ (JSONB storage) │        │   │  │   ├── bls/        │        │
│  └─────────────────┘        │   │  │   ├── fred/       │        │
│  ┌─────────────────┐        │   │  │   ├── bea/        │        │
│  │ file_registry    │        │   │  │   └── ons/        │        │
│  │ (file tracking)  │        │   │  │       └── ...    │        │
│  └─────────────────┘        │   │  └─────────────────┘        │
│  ┌─────────────────┐        │   │                              │
│  │ staging_indicators│        │   │                              │
│  │ (processed data) │        │   │                              │
│  └─────────────────┘        │   │                              │
└─────────────────────────────┘   └─────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        DBT LAYER                                      │
│              (transforms/)                                            │
│  - Separate from Python pipeline                                   │
│  - Final transformations and calculations                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Architectural Decisions

1. **Dual Path Architecture:** Separate handling for API-based (BLS, FRED, BEA) and file-based (ONS) providers, converging at the raw data storage layer.

2. **Immutable Raw Storage:** Raw responses stored with checksums to enable point-in-time analysis and prevent data loss from revisions.

3. **Provider Registry:** Hardcoded dictionary of providers (not plugin-based), enabling simple selection and management.

4. **Parser Registry:** Decorator-based registration of parsers by provider and frequency, enabling extensibility.

5. **Async-First Design:** Entire pipeline uses asyncio for high-concurrency fetching and I/O operations.

6. **Connection Pooling:** Database connections pooled for efficient reuse across operations.

---

## 4. DATA FLOW

### End-to-End Data Flow

```
CLI Command
    │
    ▼
Argument Parsing & Validation
    │
    ▼
Configuration Loading (metadata from YAML)
    │
    ▼
Dependency Initialization (providers, DB pool, processors)
    │
    ▼
─────────────────────────────────────────────────────────────
                         STAGE: FETCH
─────────────────────────────────────────────────────────────
    │
    ▼
Filter Indicators (by country, name, source)
    │
    ▼
For Each Indicator:
    │
    ├── API Providers (BLS, FRED, BEA):
    │       │
    │       ▼
    │   Construct request (URL, payload, headers)
    │       │
    │       ▼
    │   HTTP request via aiohttp (with semaphore, retry)
    │       │
    │       ▼
    │   Validate response
    │       │
    │       ▼
    │   Generate checksum (SHA-256 of JSON)
    │       │
    │       ▼
    │   Return ApisRawResult
    │
    └── ONS Provider:
            │
            ▼
        Check ETag in file_registry
            │
            ▼
        Download file (with semaphore, retry)
            │
            ▼
        Save to downloads/{source}/{country}/{category}/
            │
            ▼
        Return FilePathResult
    │
    ▼
Converge: FetchBatchResult(file=[FileResult...], apis=[ApiResult...])
    │
    ▼
[If persist_raw] Persist to Database:
    │
    ├── ApiResult → raw_respons_api table (ON CONFLICT checksum DO NOTHING)
    └── FileResult → file_registry table (ON CONFLICT file_path DO NOTHING)
    │
─────────────────────────────────────────────────────────────
                         STAGE: PARSE
─────────────────────────────────────────────────────────────
    │
    ▼
Query Database for Raw Data:
    │
    ├── Query raw_respons_api (filtered by source, country, indicator)
    └── Query file_registry (filtered by source, country, indicator)
    │
    ▼
For Each Raw Record:
    │
    ├── File-based (ONS):
    │       │
    │       ▼
    │   Route by file extension:
    │       ├── .csv → parser_csv() using Polars
    │       └── .xlsx/.xls → parser_excl() using Polars + Calamine
    │           │
    │           ▼
    │       Parse date and value columns
    │       Filter valid rows
    │       Convert to list[ParsedItems]
    │
    └── API-based (BLS, FRED, BEA):
            │
            ▼
        Lookup parser in PARSE_REGISTER[provider][frequency]
            │
            ▼
        Validate raw response using Pydantic model
            │
            ▼
        Extract data points:
            ├── Parse date strings to YYYY-MM-DD format
            ├── Convert values to Decimal
            ├── Extract footnotes/metadata
            │
            ▼
        Return ParseResult(parse_result=[ParsedItems...])
    │
    ▼
Transform to Staging Format:
    │
    ▼
staging_result() → StagingData(staging_result=[StagingItems...])
    │
    ▼
[If persist_stg] Persist to staging_indicators:
    │
    └── INSERT ... ON CONFLICT (date, source, code, country, frequency) DO UPDATE
─────────────────────────────────────────────────────────────
                         STAGE: ALL (FETCH + PARSE)
─────────────────────────────────────────────────────────────
    │
    ▼
Execute FETCH stage (always persists raw)
    │
    ▼
Execute PARSE stage (always persists staging)

─────────────────────────────────────────────────────────────
                         STAGE: REPLAY
─────────────────────────────────────────────────────────────
    │
    ▼
Query Database (same as PARSE)
    │
    ▼
Export to JSON files:
    │
    └── exported_data/{country}/{name}_{uniq}_{timestamp}.json
```

---

## 5. PROVIDER ARCHITECTURE

### Provider Design Pattern

All providers implement the same interface:

```python
class BaseProvider:
    async def __aenter__(self):  # Open session
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, ...):  # Close session
        await self.session.close()

    @retry(...)
    async def fetch_data(self, meta, category, country, indicator_name):
        # Provider-specific implementation
        return ApisRawResult or FilePathResult
```

### API-Based Providers (BLS, FRED, BEA)

**Common Characteristics:**

- Use aiohttp for HTTP requests
- Per-provider semaphore for concurrency control
- Tenacity for retry with exponential backoff
- Return `ApisRawResult(raw_respons=dict)`
- Validate API keys at request time

**Provider-Specific Details:**

| Provider | Endpoint                                              | Auth                       | Rate Limiting                    | Special Features                      |
| -------- | ----------------------------------------------------- | -------------------------- | -------------------------------- | ------------------------------------- |
| BLS      | `https://api.bls.gov/publicAPI/v2/timeseries/data/`   | API Key in payload         | Daily quota (500) + shared state | POST with JSON payload, batch support |
| FRED     | `https://api.stlouisfed.org/fred/series/observations` | API Key in params          | None                             | GET with query params                 |
| BEA      | `https://apps.bea.gov/api/data`                       | API Key (UserID) in params | Random delay (1-5s)              | Complex params for dataset/table      |

### File-Based Provider (ONS)

**Characteristics:**

- Downloads files from URLs specified in metadata
- Uses aiohttp for HTTP downloads
- Semaphore(1) for sequential downloads
- ETag-based deduplication
- Random delay (10-15s) between downloads
- Saves to local filesystem
- Returns `FilePathResult(path, ETag)`

**File Types:**

- CSV (parsed with Polars)
- XLSX/XLS (parsed with Polars + Calamine engine)

---

## 6. API VS FILE-BASED INGESTION

### API-Based Ingestion

**Flow:**

```
Metadata → Request Construction → HTTP Request → Response Validation → Raw Storage
```

**Advantages:**

- Granular queries (by series ID, date range)
- Structured JSON responses
- No local file storage needed
- Checksum-based deduplication

**Disadvantages:**

- Rate limits and quotas
- API key management
- Different response formats per provider

### File-Based Ingestion

**Flow:**

```
Metadata (URL) → ETag Check → Download → Local Storage → Registry Entry
```

**Advantages:**

- No API keys needed
- Full file available for complex parsing
- ETag-based deduplication (no re-download if unchanged)

**Disadvantages:**

- Must download entire file (not granular)
- Local storage management
- File format variability (CSV vs Excel)
- More complex parsing (boundary detection, sheet selection)
- Slower (sequential with delays)

### Convergence Points

1. **Raw Data Storage:** Both API and file results stored in database tables
2. **Parse Input:** Both provide data that can be parsed to ParsedItems
3. **Staging Output:** Both produce StagingData for final storage

### Divergence Points

1. **Storage Tables:** API → raw_respons_api, File → file_registry
2. **Parser Routing:** API → PARSE_REGISTER, File → route_task()
3. **Deduplication:** API → checksum, File → ETag + file_path

---

## 7. FETCH LAYER

### Orchestration

**Primary Function:** `fetch_config_indicators()` in `core/flows/_fetch.py`

**Execution:**

1. Apply filters to ALL_INDICATORS
2. Create asyncio task for each matching indicator
3. Execute all tasks concurrently with `asyncio.gather(return_exceptions=True)`
4. Collect results, separating API and file results
5. Return FetchBatchResult

**Concurrency:**

- Tasks run concurrently
- Each provider has its own semaphore limiting concurrent requests
- Global connection pool manages DB connections

### Error Handling

- Individual task failures captured by `return_exceptions=True`
- Logged as errors, counted, and skipped
- Pipeline continues with successful results
- Aggregated counts: success, skip, error

### Performance Characteristics

- **Parallelism:** High - all indicators fetched concurrently
- **Bottlenecks:**
  - Provider semaphores (5 for API, 1 for ONS)
  - Database connection pool (max 7)
  - Network latency
  - Rate limits (BLS daily quota)

---

## 8. PARSE LAYER

### Dual Parser Architecture

**API-Based Parsers:** Registry-based dispatch

```python
@register(Providers.bls, Frequency.monthly)
def parse_monthly_bls(data: ApiResult) -> ParseResult:
    # Validate with Pydantic model
    # Extract data points
    # Transform to ParsedItems
    return ParseResult(...)
```

**File-Based Parsers:** Extension-based routing

```python
def route_task(metafile: list[FileResult]):
    for x in metafile:
        if x.file_ext == ".csv":
            return parser_csv(x)
        elif x.file_ext in [".xls", ".xlsx"]:
            return parser_excl(x)
```

### Parser Implementation Details

**Common Responsibilities:**

- Validate input data structure
- Extract date and value pairs
- Convert values to Decimal for precision
- Handle missing/NA values
- Extract footnotes where available

**Provider-Specific Parsing:**

| Parser        | Input Format                        | Key Transformations                                     |
| ------------- | ----------------------------------- | ------------------------------------------------------- |
| BLS monthly   | JSON with series/year/period        | Convert period to YYYY-MM-DD, handle footnotes          |
| FRED monthly  | JSON with observations/date/value   | Direct date/value mapping                               |
| FRED weekly   | JSON with observations/date/value   | Same as monthly                                         |
| BEA monthly   | JSON with Data/TimePeriod/DataValue | Split TimePeriod, filter SeriesCode                     |
| BEA quarterly | JSON with Data/TimePeriod/DataValue | Split TimePeriod, convert to quarterly date             |
| ONS CSV       | CSV with period/value columns       | Pattern-based period parsing, regex filtering           |
| ONS Excel     | Excel with complex structure        | Boundary detection, code name search, Polars + Calamine |

### Parser Input/Output

**Input (API):**

```python
ApiResult(
    source_data=dict,  # Raw JSON from provider
    meta=FetchMeta(   # Metadata including country, category, etc.
        country=str,
        category=str,
        indicator=str,
        source=str,
        code_name=str,
        freq=str,
        ...
    )
)
```

**Input (File):**

```python
FileResult(
    file_path=Path,
    file_ext=str,
    country=str,
    category=str,
    indicator=str,
    code_name=str,
    freq=str,
    calc=str,
    unit=str,
    sheet_name=str | None,
    description=str,
    etag=str | None
)
```

**Output (Unified):**

```python
ParseResult(
    parse_result=[
        ParsedItems(
            date_key="YYYY-MM-DD",
            value=Decimal("123.45"),
            footnotes=[...] | None
        ),
        ...
    ]
)
```

---

## 9. PROCESS LAYER

### RawProcessors

**Responsibilities:**

- Initialize all provider instances
- Manage provider session lifecycle
- Orchestrate concurrent fetching
- Transform provider results to unified format

**Key Function:**

```python
async def process_raw_data(self, name, meta, category, country):
    provider = self.providerd[meta.source]
    raw_data = await provider.fetch_data(meta, category, country, name)

    if isinstance(raw_data, FilePathResult):
        return FileResult(...)  # Build from file result

    if isinstance(raw_data, ApisRawResult):
        return ApiResult(...)  # Build with checksum

    return None  # No data
```

### ParseProcessors

**Responsibilities:**

- Route API-based parsing through registry
- Validate parser registration

**Key Function:**

```python
def parse_data(self, raw_data: ApiResult, api: str, freq: str | None):
    if api not in PARSE_REGISTER:
        raise RoutingError(f"{api} not found")
    if freq not in PARSE_REGISTER[api]:
        raise RoutingError(f"frequency {freq} not found")
    return PARSE_REGISTER[api][freq](raw_data)
```

### staging_result()

**Responsibilities:**

- Transform ParseResult to StagingData
- Convert string dates to date objects
- Extract year from date
- Prepare for database insertion

---

## 10. DATABASE/LOAD LAYER

### Database Design

**Connection:**

- `AsyncConnectionPool[AsyncConnection[TupleRow]]`
- Configuration: min_size=1, max_size=7, max_waiting=30, timeout=10
- Using psycopg (async PostgreSQL adapter)

**Tables:**

#### 1. raw_respons_api

**Purpose:** Immutable storage of API responses

**Schema:**

```sql
id BIGSERIAL PRIMARY KEY
payload JSONB NOT NULL        -- Full response + metadata + checksum
load_at TIMESTAMPTZ DEFAULT NOW()

UNIQUE INDEX idx_unique_checksum ON ((payload -> 'meta' ->> 'checksum'))
INDEX idx_meta_source ON ((payload -> 'meta' ->> 'source'))
INDEX idx_meta_country ON ((payload -> 'meta' ->> 'country'))
INDEX idx_meta_codename ON ((payload -> 'meta' ->> 'code_name'))
```

**Deduplication:** ON CONFLICT ((payload -> 'meta' ->> 'checksum')) DO NOTHING

**Payload Structure:**

```json
{
  "source_data": { "raw": "json from provider" },
  "meta": {
    "country": "usa",
    "category": "price",
    "indicator": "CPI_YoY",
    "source": "bls",
    "code_name": "CUUR0000SA0",
    "freq": "monthly",
    "calc": "yoy",
    "unit": "%",
    "description": "Consumer Price Index...",
    "load_at": "2026-08-14T10:00:00+00:00",
    "checksum": "sha256_hash..."
  }
}
```

#### 2. file_registry

**Purpose:** Tracking of downloaded files

**Schema:**

```sql
id BIGSERIAL PRIMARY KEY
file_path TEXT NOT NULL
file_ext TEXT NOT NULL
country TEXT NOT NULL
category TEXT NOT NULL
indicator TEXT NOT NULL
frequency TEXT NOT NULL
source TEXT NOT NULL
code_name TEXT NOT NULL
calc TEXT NOT NULL
unit TEXT
sheet_name TEXT
description TEXT NOT NULL
etag TEXT
load_at TIMESTAMPTZ DEFAULT NOW()

UNIQUE (file_path, country, category, indicator)
```

**Deduplication:** ON CONFLICT (file_path, country, category, indicator) DO NOTHING

#### 3. staging_indicators

**Purpose:** Processed, standardized time series data

**Schema:**

```sql
id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
date DATE NOT NULL
year INTEGER
source TEXT NOT NULL
code TEXT NOT NULL
indicator TEXT NOT NULL
value NUMERIC(20, 4)
country TEXT NOT NULL
category TEXT NOT NULL
frequency TEXT NOT NULL
method TEXT NOT NULL
sheet_name TEXT
unit TEXT
footnotes_note JSONB
description TEXT NOT NULL
processed TIMESTAMPTZ

UNIQUE (date, source, code, country, frequency)
INDEX idx_stg_lookup ON (code, country, date)
```

**Upsert:** ON CONFLICT (date, source, code, country, frequency) DO UPDATE SET value = EXCLUDED.value, footnotes_note = EXCLUDED.footnotes_note, processed = EXCLUDED.processed

### Load Classes

| Class   | Responsibility                   | Table                          |
| ------- | -------------------------------- | ------------------------------ |
| LoadRaw | Load API raw data, file registry | raw_respons_api, file_registry |
| LoadStg | Load staging data                | staging_indicators             |
| FetchDB | Read raw data from DB            | raw_respons_api, file_registry |

### Transaction Management

- Each load method creates its own connection
- Each connection starts a transaction implicitly
- Transactions commit on successful execution
- No cross-table transaction coordination
- Errors cause SystemExit(1) (no rollback attempt)

---

## 11. MODELS/DATA CONTRACTS

### Model Hierarchy

```
Provider Response (JSON/dict)
    ↓
ApisRawResult (raw_respons: dict)
    ↓
ApiResult (source_data: dict, meta: FetchMeta)
    │
    └── FetchMeta extends BaseMetaModel with:
        country, category, indicator, load_at, checksum
    │
    └── BaseMetaModel:
        code_name, source, calc, freq, start_year, start_month, unit, sheet_name, description
    ↓
ParseResult (parse_result: list[ParsedItems])
    │
    └── ParsedItems:
        date_key: str (YYYY-MM-DD format)
        value: Decimal
        footnotes: list | None
    ↓
StagingData (staging_result: list[StagingItems])
    │
    └── StagingItems:
        date: date, year: int, source: str, code: str | None
        indicator: str, country: str, category: str, value: Decimal
        frequency: str, method: str, sheet_name: str | None, unit: str | None
        footnotes_note: list | None, description: str, processed: datetime
    ↓
staging_indicators table
```

### File-Based Path Models

```
FilePathResult (path: Path, ETag: str | None)
    ↓
FileResult (file_path, file_ext, country, category, indicator,
            code_name, freq, calc, unit, sheet_name, description, etag)
    ↓
Polars DataFrame (from CSV/Excel)
    ↓
list[ParsedItems]
    ↓
ParseResult (same as API path)
    ↓
StagingData (same as API path)
```

### Data Contract Enforcement

- **Pydantic models** validate all data structures
- **Type hints** throughout codebase
- **Runtime validation** at model instantiation
- **No schema validation** at database level (JSONB is flexible)

---

## 12. CONFIGURATION

### Environment Configuration

**Primary Configuration:** Supabase database credentials

```python
# In config/settings.py
class Resources(BaseSettings):
    sb_user: str
    sb_password: str
    sb_host: str
    sb_port: int
    sb_database: str
    bls_api_key: str | None = None
    fred_api_key: str | None = None
    bea_api_key: str | None = None

CONN_STR = f"postgresql://{source.sb_user}:{source.sb_password}@{source.sb_host}:{source.sb_port}/{source.sb_database}"
```

**Configuration Source:**

- Environment variables (primary)
- `.env` file at repository root
- All optional except Supabase credentials

### Metadata Configuration

**Structure:**

```
config/metadata/
├── catalog.yaml          # Country → Categories mapping
├── load_yaml.py         # Metadata loader
├── usa/
│   ├── price.yaml        # Indicators for USA/price
│   ├── labour.yaml       # Indicators for USA/labour
│   └── ...
└── uk/
    ├── price.yaml        # Indicators for UK/price
    └── ...
```

**Metadata Model:**

```python
# MODEL_MAP: provider string → Pydantic model class
MODEL_MAP = {
    "bls": BLSConfigModel,
    "bea": BEAConfigModel,
    "fred": FREDConfigModel,
    "ons": ONSConfigModel,
}
```

**Provider-Specific Fields:**

| Provider | Required Fields                                                        | Optional Fields                            |
| -------- | ---------------------------------------------------------------------- | ------------------------------------------ |
| BLS      | code_name, source, freq, start_year, start_month, description          | unit, calc, sheet_name                     |
| FRED     | code_name, source, freq, start_year, start_month, description          | unit, calc, sheet_name                     |
| BEA      | code_name, source, dataset, freq, start_year, start_month, description | table, line_number, unit, calc, sheet_name |
| ONS      | url, code_name, source, freq, start_year, start_month, description     | unit, calc, sheet_name                     |

### Loading Process

1. At module import: `ALL_INDICATORS = load_all_indicator()`
2. Load catalog.yaml to get country/category mapping
3. For each country/category, load corresponding YAML file
4. Validate each indicator against provider-specific model
5. Store in nested dict: `{country: {category: {indicator_name: model_instance}}}`

---

## 13. ERROR HANDLING

### Exception Hierarchy

```
PipelineCrash (Exception)
    │
    ├── ProcessingFailed
    │   │
    │   ├── RoutingError
    │   │   │
    │   │   ├── FetchDataError
    │   │   │   │
    │   │   │   ├── RateLimit (429, server errors)
    │   │   │   ├── AuthenticationError (401)
    │   │   │   ├── BLSRequestsError
    │   │   │   ├── BEARequestsError
    │   │   │   └── FREDRequestsError
    │   │   │
    │   │   └── ParseDataError
    │   │       │
    │   │       ├── BLSParserError
    │   │       ├── BEAParserError
    │   │       └── FREDParserError
    │   │
    │   ├── ResultsNotFound
    │   └── FormatError
    │
    └── UploadFailed
    └── ResourceNotFound (missing API keys, config)
```

### Error Handling Patterns

**1. Main Entry Point:**

```python
try:
    # Run pipeline
    await orch.runner(cfg)
except exc.PipelineCrash as e:
    logger.exception("Error during execution pipeline: %s", e)
    print(f"\nFull traceback: {traceback.format_exc()}")
    raise SystemExit(1)
```

- **F-001:** Only catches PipelineCrash - other exceptions propagate to asyncio.run()

**2. Concurrent Task Execution:**

```python
results = await asyncio.gather(*tasks, return_exceptions=True)
for i, result in enumerate(results):
    if isinstance(result, BaseException):
        logger.exception("Error task, skiping %s indicator..", name)
        error_count += 1
        continue
```

- Individual task failures are captured, logged, and skipped
- Pipeline continues with successful results
- Graceful degradation at indicator level

**3. Provider Fetch:**

```python
@retry(stop=stop_after_attempt(5), wait=wait_exponential(...), reraise=True)
async def fetch_data(...):
    # Make request
```

- Retry with exponential backoff (5 attempts)
- Re-raises last exception after all attempts
- **F-003:** No circuit breaker for repeated failures

**4. Database Operations:**

```python
try:
    # DB operation
except (PoolTimeout, PoolClosed, OperationalError) as e:
    logger.error("... %s", e)
    raise SystemExit(1)
```

- **F-004:** All DB errors cause immediate process exit
- No retry or recovery mechanism

### Error Aggregation

- Success count: indicators successfully processed
- Skip count: indicators with None/no data returned
- Error count: indicators with exceptions
- Logs available for all errors with full traceback

---

## 14. RETRY/RATE LIMITING/CONCURRENCY

### Concurrency Model

**Global:**

- Asyncio-based throughout
- Non-blocking I/O for HTTP and database
- Concurrent execution at indicator level

**Per-Provider Concurrency Control:**

| Provider | Semaphore Size | Additional Limits                       |
| -------- | -------------- | --------------------------------------- |
| BLS      | 5              | Daily quota (500 requests)              |
| FRED     | 5              | None                                    |
| BEA      | 5              | Random delay (1-5s between requests)    |
| ONS      | 1              | Random delay (10-15s between downloads) |

**Implementation:**

```python
# In provider __init__
self.semaphore = asyncio.Semaphore(limit_requests)

# In fetch_data
async with self.semaphore:
    # Make request
```

**Shared State for Rate Limits:**

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

- Used by BLS to signal daily quota exhaustion
- Event set when "daily threshold" message received
- All subsequent requests check event and skip if set

**Database Connection Pool:**

- min_size=1, max_size=7 connections
- max_waiting=30 (max tasks waiting for connection)
- timeout=10 seconds
- **F-006:** Fixed size may become bottleneck at scale

### Retry Mechanism

**Library:** Tenacity

**Configuration:**

```python
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception(Retryable()),
    reraise=True,
)
```

**Retryable Conditions:**

```python
class Retryable:
    def __call__(self, error: BaseException) -> bool:
        if isinstance(error, aiohttp.ClientResponseError):
            if error.status >= 500:  # Server errors
                return True
            if error.status == 429:  # Rate limited
                return True
        if isinstance(error, (aiohttp.ClientConnectionError,
                              aiohttp.ServerTimeoutError)):
            return True
        if isinstance(error, exc.RateLimit):
            return True
        return False
```

### Rate Limiting Strategies

| Provider | Strategy                                                                       |
| -------- | ------------------------------------------------------------------------------ |
| BLS      | Semaphore(5) + Daily quota tracking + Shared event + Exponential backoff retry |
| FRED     | Semaphore(5) + Exponential backoff retry                                       |
| BEA      | Semaphore(5) + Random delay (1-5s) + Exponential backoff retry                 |
| ONS      | Semaphore(1) + Random delay (10-15s) + Exponential backoff retry + ETag        |

---

## 15. RAW DATA STRATEGY

### Purpose

Preserve raw vintage data to enable:

1. **Point-in-time analysis:** Reconstruct data as it was at any historical date
2. **Revision tracking:** Identify when and how data was revised
3. **Auditability:** Full traceability from raw response to final output
4. **Reproducibility:** Re-run pipeline with exact same inputs

### Implementation

**API Providers:**

- Store full JSON response in `raw_respons_api.payload`
- Generate SHA-256 checksum of response
- Deduplicate using checksum (ON CONFLICT DO NOTHING)
- Include metadata in payload (country, category, indicator, source, code_name, etc.)

**File Providers:**

- Download files to local filesystem
- Store file path in `file_registry.file_path`
- Track ETag for HTTP cache validation
- Deduplicate using (file_path, country, category, indicator)

### Storage Characteristics

| Aspect        | API Providers  | File Providers           |
| ------------- | -------------- | ------------------------ |
| Storage       | JSONB column   | Filesystem + TEXT column |
| Deduplication | Checksum       | ETag + file_path         |
| Size          | Response size  | File size                |
| Queryability  | JSON functions | Join with file_registry  |
| Immutability  | Yes            | Yes                      |

### Data Growth

- **API:** ~1 row per provider per indicator per fetch (if checksum changed)
- **File:** ~1 row per ONS indicator per fetch (if file changed)
- **Current:** ~50-60 indicators total
- **With 200 countries:** Potentially thousands of indicators

---

## 16. STAGING STRATEGY

### Purpose

Transform heterogeneous raw data into unified, queryable format for:

1. **Standardization:** Consistent schema across all providers
2. **Query performance:** Optimized for time series queries
3. **Data quality:** Validated and cleaned data
4. **Downstream processing:** Input for dbt transformations

### Implementation

**Transformation Process:**

1. Parse raw data to ParsedItems (date_key, value, footnotes)
2. Convert to StagingItems with full context (country, category, source, etc.)
3. Insert into staging_indicators with UPSERT

**Schema Design:**

- Date as primary dimension (DATE type)
- Year extracted for fast filtering (INTEGER)
- Source, code, indicator for identification
- Value as NUMERIC(20,4) for precision
- Country, category, frequency for grouping
- Method (calc), unit, description for context
- Footnotes as JSONB for flexibility

**UPSERT Strategy:**

```sql
ON CONFLICT (date, source, code, country, frequency)
DO UPDATE SET
    value = EXCLUDED.value,
    footnotes_note = EXCLUDED.footnotes_note,
    processed = EXCLUDED.processed
```

- Updates value and metadata if same date/source/code/country/frequency
- Preserves load_at from original insert
- Enables re-processing with updated parsers

---

## 17. DBT TRANSFORMATION LAYER

### Status

**Location:** `transforms/` directory

**Contents:**

- `dbt_project.yml` - dbt configuration
- `profiles.yml` - Database connection profiles
- `models/marts/final_data.sql` - Single model
- `models/marts/schema.yml` - Schema definition

**Observations:**

- No calls from Python code to dbt commands
- dbt appears to be separate workflow
- Final transformations outside Python pipeline

---

## 18. CLI EXECUTION MODES

### Mode 1: `--list` - List Indicators

**Command:** `python extract/src/main.py --list`

**Behavior:**

- Print all available indicators organized by country/category
- No data fetching or processing
- Read-only operation

**Use Case:** Discover available indicators

---

### Mode 2: `--stage fetch` - Fetch Only

**Command:** `python extract/src/main.py --stage fetch [--source SOURCE] [--country COUNTRY] [--name INDICATOR] [--persist-raw]`

**Behavior:**

- Fetch raw data from all configured providers
- Concurrent execution for all indicators
- Optionally persist to database with `--persist-raw`
- Without `--persist-raw`: Data returned but not saved

**Use Case:** Test fetching, download new data without processing

---

### Mode 3: `--stage parse` - Parse Only

**Command:** `python extract/src/main.py --stage parse [--source SOURCE] [--country COUNTRY] [--name INDICATOR] [--persist-stg]`

**Behavior:**

- Read raw data from database (raw_respons_api and file_registry)
- Parse using registered parsers
- Optionally persist to staging with `--persist-stg`
- Without `--persist-stg`: Parsing happens but results not saved

**Use Case:** Re-process existing raw data, test parsers

---

### Mode 4: `--stage all` - Full Pipeline

**Command:** `python extract/src/main.py --stage all [--source SOURCE] [--country COUNTRY] [--name INDICATOR]`

**Behavior:**

- Fetch raw data (always persists to DB, regardless of flags)
- Load raw data to raw_respons_api/file_registry
- Parse raw data
- Load staging data to staging_indicators (always persists)

**Important:** Unlike `--stage fetch`, this ALWAYS persists both raw and staging data

**Use Case:** Complete data pipeline execution

---

### Mode 5: `--stage replay` - Export Raw Data

**Command:** `python extract/src/main.py --stage replay [--source SOURCE] [--country COUNTRY] [--name INDICATOR]`

**Behavior:**

- Read raw data from database (same as parse)
- Export to JSON files on disk
- Files saved to `exported_data/{country}/{name}_{uniq}_{timestamp}.json`
- Does NOT persist to staging

**Use Case:** Debugging, data export, backup

---

### Filter Options

| Filter             | Description              | Valid with Stages         |
| ------------------ | ------------------------ | ------------------------- |
| `--source bls      | fred                     | bea                       | ons`                      | Filter by provider | fetch, parse, all, replay |
| `--country usa     | uk`                      | Filter by country         | fetch, parse, all, replay |
| `--name INDICATOR` | Filter by indicator name | fetch, parse, all, replay |

**Mutual Exclusivity:**

- `--source` cannot be used with `--country` or `--name`
- `--name` requires `--country`

---

## 19. OPERATIONAL BEHAVIOR

### Startup

1. Load environment variables from .env file
2. Load all metadata from YAML files (ALL_INDICATORS global)
3. Initialize logging (default INFO level)
4. Parse CLI arguments and validate
5. Create database connection pool
6. Initialize all providers (open HTTP sessions)
7. Create database tables if not exist

### Execution

1. Apply filters to indicator list
2. Execute appropriate stage(s)
3. Log progress and errors
4. Return results or exit code

### Shutdown

1. Close all provider HTTP sessions
2. Close database connection pool
3. Exit with code 0 (success) or 1 (error)

### Partial Success

- **Indicator level:** Individual indicator failures are logged and skipped
- **Pipeline level:** Pipeline continues with successful indicators
- **Database level:** No partial transaction support - DB errors cause full exit

### Idempotency

- **Fetch stage:** Idempotent - checksum/ETag prevents duplicate data
- **Parse stage:** Idempotent - UPSERT updates only if data changed
- **Full pipeline:** Idempotent - running multiple times produces same results

### Retry Behavior

- **Provider level:** 5 retry attempts with exponential backoff
- **Pipeline level:** No automatic retry - must be re-run manually
- **Database level:** No retry - errors cause immediate exit

---
