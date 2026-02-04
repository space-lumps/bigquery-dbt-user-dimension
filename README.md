# user\_base\_project — dbt + BigQuery

## Overview

This project builds a clean, BI‑ready table `user_base` in BigQuery by:

* Moving complex SQL out of BI and into dbt layers for **consistency, speed, and testability**.
* Building a robust **location dimension** from component tables and **deduping** per source location to avoid conflicting geographies.
* Unifying user linkage across roles/routes (learners, educators, invite codes, sites/partners) and preserving users without links via left joins, producing a **clear grain** for dashboards.
* Documenting lineage with dbt Docs and enforcing quality with dbt tests.

## Warehouse & naming

* **Project:** `oroboro-dw`
* **Datasets:** `bronze_raw` (sources), `analytics_dev` (models)
* **dbt source name:** `raw`

## Model layers

* **staging**: thin cleans of raw sources
* **intermediate**: reusable transforms and joins

  * `intermediate/locations_clean.sql` — one row per `from_location_id` with `city/county/state/country` + coordinates
  * `intermediate/stacked_users_partners.sql` — one row per user + partner/site/classroom
* **marts**: BI‑ready outputs

  * `marts/user_base.sql` — final table queried by BI

## Data flow

```
bronze_raw (raw sources)
  ├─ user_user
  ├─ location_* (components, types, base)
  └─ (classroom, invites, partners, sites)

intermediate views
  ├─ locations_clean               -- unique per from_location_id
  └─ stacked_users_partners        -- user ↔ partner/site/classroom

mart
  └─ user_base (table)             -- one row per user_id + partner_id + site_id
```

## Location dimension — why multi‑step

A single `from_location_id` can map to several `to_location_id`s representing different types (city/locality, county, state, country). The pipeline:

1. **Collects candidates** and tags by type.
2. **Ranks** city candidates (e.g., distance checks, non‑street heuristics).
3. **Picks one value per type** and emits a single, consistent row per source location.
   This prevents partial/ambiguous geography from appearing in BI and supports stable filtering and grouping.

## User linkage — how it’s unified

Users can be associated via multiple routes (classroom membership, invitations, partner codes, etc.). The pipeline:

* Normalizes these routes into a **stacked** intermediate with a consistent set of fields.
* Joins back to `user_user` and the location dimension in the final mart.
* Preserves users with **no current link** (via left joins) so global user counts remain accurate.

**Grain of `user_base`:** `(user_id, partner_id, site_id)`.

## Materialization strategy

* `intermediate` models: **views** (lightweight, always reflect raw)
* `marts.user_base`: **table** (fast dashboards, predictable cost)

Change to a view by editing `dbt_project.yml`:

```yaml
models:
  user_base_project:
    marts:
      +materialized: view
```

## Tests

`models/marts/marts_schema.yml` includes:

* `not_null` on `user_id`
* `dbt_utils.unique_combination_of_columns` on `(user_id, partner_id, site_id)`
  Will add staging tests (e.g., `unique`/`not_null` on natural keys) as needed.

## Build & run

```bash
pip install dbt-bigquery
# Configure ~/.dbt/profiles.yml from profiles.example.yml

dbt deps        # if using packages (dbt_utils)
dbt debug
# Build
dbt run --select intermediate.locations_clean intermediate.stacked_users_partners marts.user_base
# Test
dbt test
# Docs
dbt docs generate && dbt docs serve
```

## Metabase usage

* Connect Metabase to dataset `analytics_dev` and query `user_base`.
* Keep Metabase **dynamic filter** queries using **fully‑qualified names** when Metabase injects SQL. Aliases **inside dbt models** are fine and don’t affect this.

## Repo structure

```
.
├─ dbt_project.yml
├─ profiles.example.yml
├─ packages.yml
├─ macros/
│  └─ utils.sql
├─ models/
│  ├─ sources.yml
│  ├─ intermediate/
│  │  ├─ locations_clean.sql
│  │  └─ stacked_users_partners.sql
│  └─ marts/
│     ├─ user_base.sql
│     └─ marts_schema.yml
└─ README.md
```

## Compatibility

* dbt Core ≥ 1.5
* Adapter: `dbt-bigquery`
