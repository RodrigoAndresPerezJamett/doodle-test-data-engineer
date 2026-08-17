# Toys Inc. - Data Modeling & Quality Assurance

Layered pipeline (bronze -> silver -> gold) that ingests three raw files, isolates data quality
issues, builds a dimensional model, and answers four business questions.

## Setup

Requires Python 3.12+ (developed on 3.14).

```bash
make setup    # creates .venv, installs requirements
make run      # runs the pipeline: data/raw -> bronze -> silver -> gold
make lab      # opens Jupyter
make clean    # removes generated layers (data/raw is untouched)
```

No 3.14? `make setup PYTHON=python3.12`

## Project structure

| Path | What it does |
|---|---|
| `Makefile` | Setup, run, clean targets |
| `requirements.txt` | Pinned dependencies |
| `config.yml` | Sources, expected fields, quality thresholds |
| `src/config.py` | Derives paths, loads `config.yml` |
| `src/ingest.py` | **Bronze.** Lands each source as text with lineage. Undoes transport malformations only. No casting, no dedup, no filtering. Closed schema, so a new upstream field can't widen the table |
| `src/silver.py` | **Silver.** One table per source: typed, snake_case, conformed, deduped, guaranteed PK. No joins. Business-rule violations are flagged, not filtered |
| `src/gold.py` | **Gold.** `fct_sales` (grain: one row per sale) + `dim_products` + `dim_customers`. Joins use `validate="many_to_one"` |
| `src/tests.py` | `unique`, `not_null`, `accepted_values`, `relationships` - dbt schema tests in pandas, run after each layer |
| `src/main.py` | Chains the layers |
| `notebooks/01_explore.ipynb` | Exploration that surfaced the issues below |
| `notebooks/analysis.ipynb` | The four business questions, with reasoning per question |
| `data/bronze/quarantine.parquet` | Every anomaly with its original payload, for raising upstream |

Each layer writes Parquet and the next reads from disk, so any layer can be reprocessed alone.

| Medallion | dbt equivalent |
|---|---|
| bronze | sources |
| silver | `stg_*` |
| gold | `fct_*` / `dim_*` |

## How to validate

`make run` is self-validating: it fails if any data test fails. A successful run prints
`bronze tests: 6 passed`, `silver tests: 9 passed`, `gold tests: 6 passed`, and produces
2,983 sales, 400 customers, 200 products.

## Data quality issues

### sales_data.json - 186 of 2,983 records malformed (6.2%)

**Structure**

| Issue | Records | Fix |
|---|---|---|
| Record serialised as a JSON string | 88 | `json.loads` at ingestion |
| Record wrapped in a random key | 94 | Unwrapped when the sole value is an object |
| Both combined | 4 | Same loop alternates between cases |

Payloads are intact; only the transport broke. Dropping them would have lost ~6% of sales.
One record was wrapped twice; the loop handles arbitrary depth up to 5.

**Values**

| Issue | Records | Fix |
|---|---|---|
| `ProductID` null | 39 | Left null, flagged `has_product` |
| `ProductID` = 99999 placeholder | 48 | Set to null in silver |
| `CustomerID` = 99989 placeholder | 53 | Set to null in silver |
| `TotalAmount` negative | 52 | Flagged `amount_is_valid`, excluded from revenue |

The 52 negatives match exactly the 52 rows where `TotalAmount != Quantity × Price`. That is why
they are treated as corrupt values, not returns. Excluded rather than recalculated, since
recalculating invents a figure the source never sent.

`SaleID` is unique with no nulls, `Quantity` is 1–10, all dates parse as ISO.

### customers.json

Structurally clean. `Region` null for 88 of 400 (22%), `SignUpDate` for 67. `CustomerID` and
`Email` unique. Nulls preserved, not imputed.

### products.csv

| Issue | Records | Fix |
|---|---|---|
| `Action-Figure` vs `Action Figure` | 15 of 51 | Separators normalised in silver |
| `ProductID` 254 duplicated | 1 | Rows verified identical, first kept |
| `ProductName` blank or whitespace | 50 | Trimmed to null |

Highest-impact issue in the dataset. Unconformed, it splits one category in two and drops a
third of the products from the Action Figure average, changing Q4 with nothing failing.

## Assumptions

| Ambiguity | Decision |
|---|---|
| "Latest 6 months in the dataset" | Anchored to `max(sale_date)` = 2023-08-09, not `today()`, which returns zero rows |
| Window definition | Rolling, `(anchor - N months, anchor]` |
| Region of a sale | No region column in sales, so taken from the customer record |
| "Sales volume" | Units and revenue both reported; units primary |
| "Average sales price" | Transaction price (`total_amount / quantity`) primary; catalogue price alongside |
| Negative amounts | Excluded from all revenue figures |
| Sales before signup | Excluded for Q2 per the brief; customers with null `SignUpDate` too |

## Results

More details on notebooks/analysis.ipynb

| # | Question | Answer |
|---|---|---|
| - | Total sales by category, latest 6 months | Window 2023-02-09 -> 2023-08-09; Puzzle leads |
| 1 | Highest sales volume, North, past 3 months | **Puzzle** (82 units), Doll close behind (79) |
| 2 | Top 5 customers by value since signup | Craig Pearson, Stephen Rivera, Zachary Rodgers, Michelle Taylor, Kathleen Lyons MD |
| 3 | Products never sold | **None.** All 200 catalogue products appear in sales |
| 4 | Average sales price, Action Figure | **$112.81** transaction; $111.31 catalogue, $113.72 weighted |

Caveats a stakeholder should know:

- Q1 rests on 47 transactions with a 3-unit gap between first and second. Not a stable trend.
- Q2's signup filter drops ~42% of sales. That rate is not plausible as real behaviour and
  suggests `SignUpDate` is unreliable, likely a migration date. Worth raising upstream.
- Regional figures exclude 706 of 2,983 sales (24%) with no region. Sales with no product
  reference are 2.6% of revenue: too small to shift a conclusion, reported anyway.

## In production

Layers would be orchestrated (Airflow, Dagster) with freshness checks on bronze, each source on
its own schedule. Bronze append-only, partitioned by `_batch_id`. `fct_sales` stays at sale grain
because aggregations derive from detail but not the reverse; aggregate marts would sit on top.