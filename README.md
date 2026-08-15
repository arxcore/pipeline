# Macro Data Pipeline

**Automated Multi-Source ELT Pipeline for Macroeconomic Indicators**

An automated pipeline designed to extract, normalize, and store macroeconomic indicator data from major statistical agencies. The system prioritizes **data fidelity** by preserving raw vintage data, enabling accurate point-in-time historical analysis.

---

## Current Scope

### Countries

- **2 countries:** United States (`usa`), United Kingdom (`uk`)

### Data Sources / Providers

| Provider | Type | Agency | Coverage |
| --------- | ------ | -------- | ---------- |
| **BLS** | REST API | Bureau of Labor Statistics | US economic data |
| **FRED** | REST API | Federal Reserve Economic Data | US economic data |
| **BEA** | REST API | Bureau of Economic Analysis | US economic data |
| **ONS** | File Download | Office for National Statistics | UK economic data |

### Indicators

- **Approximate count:** 50-60 indicators total across all categories
- **Categories:**
  - **USA:** price, labour, trade, money, consumer, business (6 categories)
  - **UK:** price, labour, consumer, business, trade (5 categories)

### Frequencies Supported

- Monthly (most common)
- Weekly (FRED only)
- Quarterly (BEA)
- Annual

---

## Architecture

The pipeline employs a **dual-path architecture** to handle API-based and file-based data sources differently, converging at the raw data storage layer.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLI INTERFACE                                  │
│              (extract/src/main.py)                                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE RUNNER                                   │
│  - Stage execution (FETCH, PARSE, ALL, REPLAY)                       │
│  - Dependency injection (providers, DB, processors)                 │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    FLOWS MANAGER                                     │
│  - Provider lifecycle management (open/close HTTP sessions)         │
│  - Database connection pooling                                       │
│  - Orchestration delegation                                          │
└─────────────────────────────────────────────────────────────────────┘
         │                                  │                                  │
         ▼                                  ▼                                  ▼
┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│   RAW PROCESSORS │              │  PARSE PROCESSORS│              │   LOAD LAYER     │
│  (core.process)  │              │  (core.process)  │              │ (upload.postgres)│
│                 │              │                  │              │                 │
│  - Concurrent   │              │  - Registry-    │              │  - LoadRaw      │
│    provider     │              │    based dispatch│              │  - LoadStg      │
│    fetching     │              │  - API parsers   │              │  - FetchDB      │
│  - Result       │              │  - File parsers  │              │                 │
│    transform    │              │    (ONS)         │              │                 │
└─────────────────┘              └─────────────────┘              └─────────────────┘
         │                                  │                                  │
         ▼                                  ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PROVIDER LAYER                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐│
│  │    BLS      │  │    FRED     │  │     BEA     │  │     ONS     ││
│  │  (API)      │  │  (API)      │  │   (API)     │  │  (File)     ││
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘│
└─────────────────────────────────────────────────────────────────────┘
         │                                  │
         ▼                                  ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│      PostgreSQL (Supabase)   │   │        File System            │
│  ┌─────────────────┐        │   │  ┌─────────────────┐        │
│  │ raw_respons_api │        │   │  │   downloads/     │        │
│  │ (JSONB storage) │        │   │  │   ├── bls/        │        │
│  └─────────────────┘        │   │  │   ├── fred/       │        │
│  ┌─────────────────┐        │   │  │   ├── bea/        │        │
│  │ file_registry    │        │   │  │   └── ons/        │        │
│  │ (file tracking)  │        │   │  │       └── ...    │        │
│  └─────────────────┘        │   │  └─────────────────┘        │
│  ┌─────────────────┐        │   │                              │
│  │ staging_indicators│        │   │                              │
│  │ (time series)    │        │   │                              │
│  └─────────────────┘        │   │                              │
└─────────────────────────────┘   └─────────────────────────────┘
```

### Key Design Decisions

1. **Dual-Path Architecture:** Separate handling for API-based (BLS, FRED, BEA) and file-based (ONS) providers
2. **Immutable Raw Storage:** Raw responses stored with checksums to enable point-in-time analysis
3. **Async-First:** Entire pipeline uses asyncio for high-concurrency fetching and I/O
4. **Connection Pooling:** Database connections pooled for efficient reuse

---

## Data Flow

### Stage 1: FETCH

```
Metadata -> Provider Selection -> HTTP Request/Download -> Validation -> Raw Storage
```

- **API Providers:** Construct request, HTTP POST/GET, validate response, generate SHA-256 checksum
- **File Providers (ONS):** ETag check, download with If-None-Match, save to filesystem, track in registry
- **Convergence:** Both paths produce unified `FetchBatchResult`
- **Deduplication:** API (checksum-based), File (ETag + file_path based)

### Stage 2: PARSE

```
Query DB -> Parser Dispatch -> Date/Value Extraction -> Standardization -> Staging Data
```

- **API Parsers:** Registry-based dispatch using `@register(provider, frequency)` decorator
- **File Parsers:** Extension-based routing (CSV -> Polars, Excel -> Polars + Calamine)
- **Output:** Unified `ParseResult` with standardized `ParsedItems(date_key, value, footnotes)`

### Stage 3: ALL (FETCH + PARSE)

Combines fetch and parse stages, **always persists** both raw and staging data.

### Stage 4: REPLAY

```
Query DB -> Export to JSON -> Save to exported_data/{country}/{name}_{timestamp}.json
```

---

## Project Structure

```
pipeline/
├── .env                    # Environment variables (Supabase credentials)
├── .env.example            # Example local PostgreSQL configuration
├── pyproject.toml          # Poetry dependencies
│
├── extract/
│   ├── config/
│   │   ├── settings.py         # Configuration (Resources, CONN_STR)
│   │   └── metadata/
│   │       ├── catalog.yaml     # Country -> Categories mapping
│   │       ├── load_yaml.py     # Metadata loader
│   │       ├── usa/            # USA metadata YAML files
│   │       │   ├── price.yaml, labour.yaml, trade.yaml, etc.
│   │       └── uk/             # UK metadata YAML files
│   │           ├── price.yaml, labour.yaml, etc.
│   │
│   └── src/
│       ├── main.py              # CLI entrypoint
│       ├── core/
│       │   ├── cli.py            # PipelineRunner, PipelineConfig, Stage
│       │   ├── models/           # Data schemas (Pydantic)
│       │   ├── parsers/          # Parser implementations
│       │   │   ├── registry.py   # Parser registry
│       │   │   ├── bls/, fred/, bea/  # API provider parsers
│       │   │   └── ons/          # File parser routing
│       │   ├── process/          # Process layer
│       │   │   ├── raw.py        # RawProcessors
│       │   │   ├── parse.py      # ParseProcessors
│       │   │   └── staging.py    # staging_result
│       │   └── flows/            # Orchestration
│       │       ├── manager.py    # FlowsManager
│       │       ├── _fetch.py     # Fetch orchestration
│       │       ├── _parser.py    # Parse orchestration
│       │       ├── _chain.py     # Full chain orchestration
│       │       └── _utils.py     # Utilities
│       │
│       ├── providers/           # Data providers
│       │   ├── metamodel.py      # BaseMetaModel
│       │   ├── bls/, fred/, bea/, ons/  # Provider implementations
│       │   └── retry_http.py, share_state.py
│       │
│       └── upload/
│           └── postgres/         # Database loaders
│               ├── fetch_db.py, raw_data_respons.py, stg_indicator.py
│
│       └── monitoring/         # Logging and exceptions
│           ├── logs/            # Logging setup
│           └── exc_models/       # Exception hierarchy
│
└── transforms/                    # dbt project (separate workflow)
    ├── dbt_project.yml
    ├── models/marts/final_data.sql
    └── profiles.yml
```

---

## Tech Stack

| Category | Technology |
| ---------- | ------------ |
| **Language** | Python 3.11+ (async/await) |
| **Dependency Mgmt** | Poetry |
| **Data Processing** | Polars, Calamine |
| **Data Validation** | Pydantic |
| **HTTP Client** | aiohttp |
| **Concurrency** | asyncio, tenacity |
| **Database** | PostgreSQL 15+ (via Supabase) |
| **DB Driver** | psycopg (async) + psycopg_pool |
| **Transformation** | dbt-core, dbt-postgres |

---

## Installation

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Poetry (recommended)

### Setup

```bash
# Clone repository
git clone https://github.com/arxcore/pipeline.git
cd pipeline

# Install dependencies
poetry install

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

**Environment Variables:**

```bash
# Supabase (primary - used in production)
SB_USER=your_supabase_user
SB_PASSWORD=your_supabase_password
SB_HOST=your_supabase_host
SB_PORT=5432
SB_DATABASE=your_supabase_database

# API keys (required for respective providers)
BLS_API_KEY=your_bls_api_key
FRED_API_KEY=your_fred_api_key
BEA_API_KEY=your_bea_api_key

# Local PostgreSQL (fallback - commented out in settings.py)
# DB_USER=user
# DB_PASSWORD=your_password
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=your_db_name
```

---

## Configuration

### Metadata Structure

Metadata is organized by country and category in YAML files:

```
config/metadata/
├── catalog.yaml          # Defines countries and their categories
└── {country}/
    └── {category}.yaml   # Defines indicators for that country/category
```

**Example indicator (usa/price.yaml):**

```yaml
CPI_YoY:
  code_name: "CUUR0000SA0"
  source: bls
  freq: monthly
  start_year: 2022
  start_month: 3
  calc: yoy
  unit: "%"
  description: "Consumer Price Index, Year-over-Year Change"

Core_PCE_MoM:
  source: bea
  code_name: "DPCERGM"
  dataset: "NIPA"
  table: "T20807"
  line_number: "25"
  freq: M
  start_year: 2023
  calc: raw
  unit: "%"
  description: "Core Personal Consumption Expenditures Price Index, MoM"
```

---

## CLI Usage

### Entry Point

```bash
python extract/src/main.py [OPTIONS]
```

### Main Options

| Option | Description |
| -------- | ------------- |
| `--list` | List all available indicators and exit |
| `-l, --log-level {debug,info,warning,error,critical}` | Set logging level (default: info) |
| `-c, --country COUNTRY` | Filter by country (usa, uk) |
| `-n, --name INDICATOR` | Filter by specific indicator name |
| `--source {bls,bea,fred,ons}+` | Filter by provider(s) |
| `--stage {fetch,parse,all,replay}` | Pipeline stage to execute |
| `--persist-raw` | Persist raw data to DB (only with `--stage fetch`) |
| `--persist-stg` | Persist staging data to DB (only with `--stage parse`) |

### Examples

#### List available indicators

```bash
python extract/src/main.py --list
```

#### Fetch raw data only (no persistence)

```bash
python extract/src/main.py --stage fetch
```

#### Fetch and persist raw data

```bash
python extract/src/main.py --stage fetch --persist-raw
```

#### Parse existing raw data to staging

```bash
python extract/src/main.py --stage parse --persist-stg
```

#### Full pipeline: Fetch + Persist Raw + Parse + Persist Staging

```bash
python extract/src/main.py --stage all
```

**Note:** `--stage all` ALWAYS persists both raw and staging data, regardless of flags.

#### Run for specific country

```bash
python extract/src/main.py --country usa --stage all
```

#### Run for specific indicator

```bash
python extract/src/main.py --country usa --name CPI_YoY --stage all
```

**Note:** When using `--name`, you must also specify `--country`.

#### Run for specific source(s)

```bash
python extract/src/main.py --source bls fred --stage fetch
```

**Note:** Cannot combine `--source` with `--country` or `--name`.

#### Export raw data from database to JSON

```bash
python extract/src/main.py --stage replay --country usa
```

Exports to: `exported_data/{country}/{name}_{uniq}_{timestamp}.json`

---

## Data Architecture

### Raw Data Storage (Immutable)

**API Providers:** Full JSON response stored in `raw_respons_api.payload` (JSONB) with SHA-256 checksum deduplication.

**File Providers:** Files stored on disk at `downloads/{source}/{country}/{category}/` with tracking in `file_registry` table.

### Staging Data Storage

Standardized time series data in `staging_indicators` table with composite unique key.

**Schema:**

```sql
staging_indicators (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER,
    source TEXT NOT NULL,
    code TEXT NOT NULL,
    indicator TEXT NOT NULL,
    value NUMERIC(20, 4),
    country TEXT NOT NULL,
    category TEXT NOT NULL,
    frequency TEXT NOT NULL,
    method TEXT NOT NULL,
    sheet_name TEXT,
    unit TEXT,
    footnotes_note JSONB,
    description TEXT NOT NULL,
    processed TIMESTAMPTZ,
    UNIQUE (date, source, code, country, frequency)
)
```

**UPSERT:** ON CONFLICT DO UPDATE SET value, footnotes_note, processed

---

## Concurrency & Retry

### Concurrency Limits

| Provider | Semaphore Size | Additional |
| --------- | --------------- | ----------- |
| BLS | 5 | Daily quota (500) |
| FRED | 5 | None |
| BEA | 5 | Random delay (1-5s) |
| ONS | 1 | Random delay (10-15s) |

**Database:** Connection pool (min=1, max=7)

### Retry Mechanism

- **Library:** Tenacity
- **Attempts:** 5 max
- **Backoff:** Exponential (multiplier=2, min=2-4s, max=60-70s)
- **Retryable:** 5xx errors, 429 (rate limit), connection errors, timeouts

---

## Error Handling

### Exception Hierarchy

```
PipelineCrash
    ├── ProcessingFailed (RoutingError, ResultsNotFound, FormatError)
    │   └── FetchDataError (RateLimit, AuthError, BLS/FRED/BEARequestsError)
    │   └── ParseDataError (BLS/FRED/BEAParserError)
    └── UploadFailed
    └── ResourceNotFound
```

### Behavior

| Error | Handling |
| ------- | ---------- |
| Individual indicator failure | Logged, counted, skipped - pipeline continues |
| Provider session failure | Logged, raises exception |
| Database error | **SystemExit(1)** - pipeline stops |
| Non-PipelineCrash exception | **NOT CAUGHT** - propagates |

---

## Idempotency

The pipeline is **idempotent** - safe to re-run multiple times:

| Stage | Mechanism | Result on Re-run |
| ------- | ---------- | ----------------- |
| Fetch (API) | Checksum dedup | Same: skipped; Changed: new row |
| Fetch (File) | ETag + path dedup | Same: skipped; Changed: re-download |
| Parse | UPSERT | Same key: update value; Different: new row |

---

## Development Status

- Async fetch layer with aiohttp
- Multi-provider support (BLS, FRED, BEA, ONS)
- Raw JSONB persistence with checksum dedup
- File download with ETag dedup
- Parser registry for API providers
- File parser routing (CSV, Excel with Calamine)
- Polars-based parsing
- Pydantic-based data validation
- Tenacity-based retry with exponential backoff
- Connection pooling
- CLI with multiple stages and filters
- Custom exception hierarchy

---

## Known Limitations

### Critical

1. **BLS Quota:** Daily quota of 500 requests - will be exceeded at ~100 indicators
2. **Exception Handling:** Non-PipelineCrash exceptions not caught in main()
3. **DB Errors:** Database errors cause immediate pipeline exit
4. **Connection Pool:** Fixed at 7 connections - bottleneck at scale

### Architectural

1. Dual parser architecture (API vs file-based routing inconsistency)
2. No plugin system (providers hardcoded)
3. Sequential provider session opening
4. No circuit breakers

### Scalability

1. ONS downloads sequential with delays
2. Metadata loaded entirely at startup
3. No retention policy for raw data

---

## Scaling Considerations

The current implementation covers 2 countries and ~50-60 indicators. The architecture was designed with expansion toward 200+ countries (~5,000-6,000 indicators) in mind, but the current implementation has only been tested against the present provider and country scope.

The pipeline supports this through:

- Async-first provider ingestion
- Database connection pooling
- Idempotent fetch and staging operations
- A modular provider/parser architecture

The main areas identified for a larger expansion are provider concurrency and quota management, database write throughput, progress tracking and resumability, failure isolation, and metadata management.

---

## Documentation

| Document | Description |
|----------|-------------|
| [codebase_tracing.md](docs/codebase_tracing.md) | Complete AS-IS runtime tracing and execution analysis |
| [documentasi_pipeline.md](docs/documentasi_pipeline.md) | Higher-level technical documentation |

---

## License

MIT License

---

*Documentation based on actual codebase implementation. Last updated: 2026-08-14*
