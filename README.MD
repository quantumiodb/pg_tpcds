## pg_tpcds - TPC-DS for Apache Cloudberry / PostgreSQL

A plug-and-play extension for TPC-DS benchmarking. Inspired by [duckdb](https://github.com/duckdb/duckdb.git) and [hyrise](https://github.com/hyrise/hyrise.git), it eliminates the cumbersome TPC-DS setup — install the extension and start benchmarking immediately.

Supports **Apache Cloudberry (MPP)** distributed architecture: data is properly distributed across segments via hash distribution.

### Requirements

- Apache Cloudberry (PostgreSQL 14 kernel) or PostgreSQL
- CMake 3.12+
- C++17 compiler (GCC 7+ or Clang 5+)

### Build & Install

Make sure `pg_config` is in your PATH.

```bash
cd contrib/pg_tpcds
cmake -Bbuild
cmake --build build --target install
```

### Quick Start

```sql
-- Install the extension (automatically creates schema and all 24 TPC-DS tables)
CREATE EXTENSION pg_tpcds;

-- Generate data for scale factor 1
SELECT * FROM dsdgen(1);
          tab           | row_count
------------------------+-----------
 promotion              |       300
 ship_mode              |        20
 warehouse              |         5
 catalog_page           |     11718
 household_demographics |      7200
 inventory              |  11745000
 reason                 |        35
 store                  |        12
 customer               |    100000
 customer_address       |     50000
 web_sales              |    719384
 date_dim               |     73049
 time_dim               |     86400
 web_site               |        30
 call_center            |         6
 income_band            |        20
 store_sales            |   2880404
 web_page               |        60
 catalog_sales          |   1441548
 customer_demographics  |   1920800
 item                   |     18000
 catalog_returns        |    144067
 web_returns            |     71654
 store_returns          |    287867
(24 rows)
```

### Running TPC-DS Queries

```sql
-- Run all 99 queries and show benchmark results
SELECT * FROM tpcds;

 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |      250.59 |      251.74 |   +0.46 | true
  02  |     1398.46 |     1525.93 |   +9.11 | true
  ...
  99  |      996.87 |     1023.06 |   +2.63 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |   143260.37 |   139667.93 |   -2.51 |

-- Run specific queries by ID
SELECT * FROM tpcds(1, 2, 3);

-- Run a single query directly
SELECT query FROM tpcds_queries(1); \gset
:query
```

**Column descriptions:**

| Column | Description |
|---|---|
| Stable(ms) | Best historical duration (from `tpcds.tpcds_query_stats`) |
| Current(ms) | Duration of the current run |
| Diff(%) | Percentage change: `(Current - Stable) / Stable * 100` |
| Result | Whether the query executed successfully |

- On the **first run**, `Stable(ms)` is empty — results are recorded as the baseline.
- On **subsequent runs**, if the current duration is faster, it replaces the stored baseline.

### Other Functions

```sql
-- List all 99 TPC-DS queries
SELECT * FROM tpcds_queries();

-- Get a specific query by ID (1-99)
SELECT * FROM tpcds_queries(42);

-- Truncate all TPC-DS table data
SELECT tpcds_cleanup();

-- Remove the extension (drops all tables, functions, and schema)
DROP EXTENSION pg_tpcds CASCADE;
```

### Customization

- **Queries**: 99 SQL statements are provided in `src/tpcds/queries/`. Modify them before building to customize the benchmark.
- **Schema**: Table DDL files are in `src/tpcds/schema/`. You can add primary keys, indexes, or change distribution keys.
- **Post-setup**: Add index creation or other post-load statements to `src/post_prepare.sql`, which runs after table creation.
