# Task 7 — Documentation

**Project:** Superstore Lakehouse Pipeline (Databricks Free Edition + Delta Lake)
**Team:** Filipovych Marharyta, Ksenia Hanziuk, Sofiia Churikova
**Dataset:** Kaggle Superstore dataset, normalized into a star schema of **8 raw tables** (1 fact + 7 dimensions)

---

## 1. Architecture Diagram


[mermaid URL](https://mermaid.live/edit#pako:eNqllv9u2kgQx19l5PwTroTD65-40klpQlIkgiVDm7seJ2uxF3BjvNbaTkOiSH2Ie8J7kpvFGOzKuouK_4DxzHxm19-dwbwoAQ-Z4ijLmH8L1lTkMLueJ4BXVixWgqZr8C7v_5wr-Ak3o_FwCuc2XE0_Z5258leZKa8wEizII57A2Dt6b1QkuQiZ8KOcbbJekD02sBtSJbTENIwFRZbzTWtYx3AqeFgEeUvUwGjMAyr31BI2ZW2asxUXEWuJWxgXbNUO2xjM1lHqb1C8Zpwl4TwpzQ-eO_kyxNTSmM-TheDJM_N_QQk7cM3inEJOFzHLMKY6KgQ83QJfAqPBWmqM7ncQJSuWyafw82iDFt2kO3_GCxEwfxnFzE_ohu22UK78-43r3eHCX3mUQE1-eIKDnmhX4qF5UApLp1RkDEIUJ-sCrpeEVITRM4OcPeUYD1lYpAzOPffen3y6-zD0gD8yAYJ_86OwgxmPNI4kD4siixKWZSAK-ZTHHR6aazoafx56uNfSaOg8daXOUYzF_bJH4J-_v8Ouulwta2Z7x2zBvmI3srCORckRhHdQpkhZBaMZT9pOcDS5wpoh3fqqPGT4FaRNara2O_x5cjf0boeYP3Oh1-uBO9mrgaH7j8MJTNwZ3F3Orj4Or2EmHaPJdOjNMHw-o9kDaA6MkkCwDUtyGsOY0xDPvdOm2K07vsZdya83TOCtnMAVj0M_o3gEfkijeNvgbkkzo-z6ZorWTNkPzg919Cqp6jF_w3IRBVmbtJeTy_Efs9HVFKGDjXLMeArGoTPfg7zf7whV5km-jreQC6wE5-PL28578IokQa0g56icFHT66Q5c7KVOSe9rQSpbtKpUa966yPJHTl4XF7_tx7c-yqV_N1y1Odun7_u36ptDmalb-sp46dud3c4rrSrzKEN15Pk2Zoc9AeCox86ZTg3DtLsBj7lwzpbLZTfLBX9gzpml2v2BWWdvVGiwmqWrhvpGlpzAaiew-gmscQJrnsBaJ7D2z7NVX1bsINAJaWWDvmEStc5WzVuxxDItfdDG6vZiYdl1tmrmAxsYxCatLBkMWGPdqdt8XrLQA4u9kfWarKUTSmgbuwxs1W6w1VD-v87mwiJ2v85WY3pcVydqv40NzQEjrMH-MIPWQl-q5I0sOYHVTmD1n2cPP2N7VqWaaYT_db5KV1mJKFScXBSsq-CbY0PlrfIi686VfM3k3xsH5LtYPMyVefKKTEqTL5xvKkzwYrVWnCWNM7wrUvkH5Dqi-NLcHLzyvcHEFS-SXHHUvtbfVVGcF-UJ73W7p6s4A6ZlawYZ6GZX2SrOha4bvQEZ9G3scdMgKnntKs-7hdWe2ddIX1MtSzdI37b0138BbvFm0A)
---

## 2. Data Flow Description

1. **Raw -> Bronze.** Each of the 8 source CSVs (`order_items`, `orders`, `customers`, `products`, `locations`, `categories`, `regions`, `ship_modes`) is read as-is with header inference and schema inference, then written to a Delta table named `bronze_<name>`. Two audit columns are added: `ingestion_timestamp` (load time) and `source_file_name` (originating file). No business logic or cleaning happens here - Bronze is a faithful, queryable copy of the source.

2. **Bronze -> Silver.** The fact table `bronze_order_items` is enriched with its dimensions: a left join to `customers` (customer name, segment), `products` (category, sub-category, product name), and `locations` (city, state, country, market, region). Dates stored as text (`dd-MM-yyyy`) are parsed into proper `date` columns, and text fields (`segment`, `region`, `ship_mode`, `category`, `product_name`) are trimmed/title-cased for consistency. Duplicate rows (same `row_id` loaded more than once) are removed by keeping the most recently ingested copy via `ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY ingestion_timestamp DESC)`. The cleaned rows are then split by validation rules into `silver_orders` (valid) and `silver_rejected_orders` (invalid, tagged with a `rejection_reason`).

3. **Incremental loads.** `day_1.csv`, `day_2.csv`, and `day_3.csv` simulate three days of new order arrivals landing in the same raw zone. Each file goes through the identical join/clean/validate logic as the initial Silver load, then valid rows are merged into `silver_orders` with `MERGE INTO ... ON t.row_id = s.row_id WHEN NOT MATCHED THEN INSERT`, so rows that were already loaded (e.g. `day_2` deliberately resends 3 rows from `day_1`) are not duplicated. Invalid rows (e.g. the 3 deliberately broken rows in `day_3`) are appended to `silver_rejected_orders` instead of being merged. Idempotency is verified by checking `COUNT(*) = COUNT(DISTINCT row_id)` on `silver_orders` after all three days are processed.

4. **Silver -> Gold.** Four aggregate tables are built directly from `silver_orders`, each rebuilt with `CREATE OR REPLACE TABLE ... AS SELECT` so they always reflect the latest Silver state: daily revenue, revenue by region, revenue by category, and per-customer revenue/order counts.

5. **Gold -> Analytics.** Spark SQL window-function queries answer the five required business questions (top products, top region, monthly trend with month-over-month change, running revenue total, and top product per region) directly against `silver_orders`.

6. **Performance check.** A join between `silver_orders` (large) and `regions` (13 rows) is analyzed with `.explain()` both with and without `broadcast()`, to compare a shuffle-based `SortMergeJoin` against a `BroadcastHashJoin`.

---

## 3. Data Quality Rules

Applied during the Bronze -> Silver transformation, before a row is allowed into `silver_orders`:

| Rule                  | Check                                                   | On failure                                                                           |
|-----------------------|---------------------------------------------------------|--------------------------------------------------------------------------------------|
| Non-null sales        | `sales IS NOT NULL`                                     | rejected, reason `sales is null`                                                     |
| Non-null quantity     | `quantity IS NOT NULL`                                  | rejected, reason `quantity is null`                                                  |
| Non-negative sales    | `sales >= 0`                                            | rejected, reason `sales < 0`                                                         |
| Positive quantity     | `quantity > 0`                                          | rejected, reason `quantity <= 0`                                                     |
| Valid shipping window | `ship_date >= order_date`                               | rejected, reason `ship_date < order_date`                                            |
| Uniqueness            | one row per `row_id`, latest `ingestion_timestamp` wins | older duplicate silently dropped (not rejected - treated as a re-send, not bad data) |

Rejected rows are never discarded — they are written to `silver_rejected_orders` with a `rejection_reason` column so they remain auditable rather than silently lost.

---

## 4. Assumptions

- `row_id` is a stable, unique business key for `order_items` across all files (initial load + all incremental days), and is the correct key to `MERGE INTO` on.
- Dates in every raw file are formatted as `dd-MM-yyyy`; any value that doesn't match this pattern becomes `null` after `to_date()`.
- "Revenue" is defined as `SUM(sales)` throughout Gold and Analytics (not `profit`).
- The CSVs are uploaded ahead of time into a Unity Catalog Volume at the path configured in `RAW_PATH` / `INC_PATH` — the notebook does not perform the upload itself.
- The dataset is small enough that `inferSchema` on read is acceptable; no explicit schema is declared for Bronze.
- Dimension tables (`customers`, `products`, `locations`) are treated as a single current snapshot - there is no historical/versioned view of a customer or product (no slowly-changing-dimension handling).
- A `LEFT JOIN` against dimensions is appropriate, i.e. an `order_items` row should still flow through (with nulls for missing dimension attributes) even if a dimension key doesn't resolve, rather than being dropped.
- Single-user, single-cluster Databricks Free Edition is sufficient — no concurrency, multi-cluster, or job-orchestration concerns are addressed.

---

## 5. Limitations

- **No enforced schema in Bronze.** Relying on `inferSchema` means a malformed CSV could silently produce wrong column types; there's no schema contract or `expectations`-style enforcement before Bronze.
- **No slowly-changing-dimension (SCD) support.** If a customer's segment or a product's category changes over time, history isn't preserved - Silver always reflects whatever the dimension tables currently say.
- **Gold tables are full rebuilds, not incremental.** `CREATE OR REPLACE TABLE ... AS SELECT` recomputes all aggregates from scratch on every run; this is fine at this data volume but wouldn't scale to a large fact table without an incremental/merge-based aggregation strategy.
- **Performance analysis covers only one join.** Only the `silver_orders ⋈ regions` join is profiled with `broadcast()`; other joins in the pipeline (e.g. the dimension joins in Silver) aren't analyzed the same way.
- **Limited real-world impact of the broadcast optimization here.** At ~51K rows, the dataset is small enough that the shuffle-vs-broadcast difference is mostly illustrative rather than performance-critical; the technique matters more at production scale.
- **Known column-naming wrinkle:** both `silver_orders` (via the `locations` join) and `bronze_regions` carry a `market` column, so the Task 6 join produces two ambiguous `market` columns in its result. This doesn't break `.explain()`, but would error if either `market` column were referenced directly afterward — worth renaming/disambiguating in a future pass.
- **No automated tests or CI.** Correctness is currently checked manually via `print()` row counts and ad-hoc `SELECT` statements in the notebook, not via a test suite.
