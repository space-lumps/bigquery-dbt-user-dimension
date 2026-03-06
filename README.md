# dbt User 360 Dimension in BigQuery
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![dbt Core](https://img.shields.io/badge/dbt_Core-≥1.8-orange)](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/Older%20versions)
[![dbt-bigquery](https://img.shields.io/badge/Adapter-dbt--bigquery-blue)](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
[![Release](https://img.shields.io/github/v/release/space-lumps/bigquery-dbt-user-dimension?color=green)](https://github.com/space-lumps/bigquery-dbt-user-dimension/releases)

## Overview

This dbt project demonstrates a clean, maintainable user 360 dimension (`dim_users`) in BigQuery. It aggregates user identity with resolved location and multi-path attribution (sponsor/site/classroom) from anonymized platform data (professional networking/resource platform connecting career aspirants with field experts).

## Project Background & Evolution

This user dimension started as a single, monolithic SQL query in the BI tool that combined complex location normalization, multi-path attribution logic, and evolving user attributes. While functional for small daily updates (~once per day, capturing new users), the query became increasingly messy and error-prone.

### dbt was introduced to address these pain points by refactoring into a layered, modular pipeline:

- **Modularity & maintainability** — Static, rarely-changing logic (location cleaning, attribution unification) lives in dedicated intermediate models, protected from accidental edits during frequent dim_users updates (new columns, categorizations, calculations).
- **Testability** — Each layer is individually tested (uniqueness, not_null, relationships, custom logic), catching issues early rather than downstream in reports.
- **Consistency & performance** — Centralized transformations ensure the same clean logic across all consumers; incremental materialization keeps daily refreshes fast and cost-effective in BigQuery.

#### Result: A fragile query became a production-grade, testable, modular pipeline well-suited to a growing user 360 view.

## Key goals:
- Resolve hierarchical location data with deduplication and prioritization.
- Unify user associations across routes while preserving unlinked users.
- Enforce grain, schema tests, and documentation for BI/reporting reliability.

## Key Features

- Denormalized user dimension with identity + location + attribution context
- Full daily refresh (small scale <50k rows → no incrementals needed)
- Intermediate models materialized as tables for performance/debugging
- Schema tests (not_null, unique combination) and dbt docs support
- Genericized names and placeholders — no real data or proprietary identifiers

---

## Data Flow

This project follows a standard dbt layered architecture:

- **Sources**: Real production tables from the platform (users, locations, attributions), renamed and lightly anonymized for this public repo to remove proprietary naming while preserving realistic structure and relationships.
- **Intermediates**: Modular transformations for reusable logic:
  - `int_locations_clean`: Normalizes and deduplicates hierarchical location data, ensuring one best row per `from_location_id`.
  - `int_user_attributions`: Unifies multi-path attribution logic (classroom, invite, sponsor) into a single, clean association per user.
- **Marts**: The final `dim_users` model delivers a BI-ready user 360 view with enforced grain (`user_id` + optional `sponsor_id`/`site_id`), full tests, and table materialization.

```mermaid
graph TD
    A[Raw Sources<br>users, locations, attributions] --> B[int_locations_clean<br>• Normalize & deduplicate hierarchical locations<br>• One best row per from_location_id]
    A --> C[int_user_attributions<br>• Unify multi-path user associations<br>• Stack classroom/ invite/sponsor paths]
    B --> D[dim_users<br>• Final user 360 dimension<br>• Grain: user_id + optional sponsor_id/site_id]
    C --> D
    D --> E[BI Tools / Reporting<br>Consistent, testable, fast queries]

    style A fill:#757575,stroke:#424242,stroke-width:2px
    style B fill:#1e88e5,stroke:#0d47a1,stroke-width:2px,color:#fff
    style C fill:#1e88e5,stroke:#0d47a1,stroke-width:2px,color:#fff
    style D fill:#0d47a1,stroke:#003087,stroke-width:4px,color:#fff
    style E fill:#757575,stroke:#424242,stroke-width:2px
```

## Location Resolution Logic

A single `from_location_id` can map to multiple types (city, county, state, country) with potential duplicates or hierarchies. The pipeline collects candidates, ranks cities by distance (e.g., ST_DISTANCE in miles) + heuristics (e.g., regex checks for address-like strings), picks the best per type, and emits one consistent row per source location — preventing ambiguous geography in downstream queries.

This mapping prioritizes the nearest city if within ~10 miles (or if the original locale resembles a suburb/address), grouping users into standardized cities for better BI visualization. For example, scattered suburbs are aggregated under their parent city, enabling accurate mapping and regional analysis without fragmented data points.

## User Attribution Unification

Multiple paths (classroom membership, invitations, sponsor codes) are normalized into a stacked intermediate, then left-joined to user core and locations. Unlinked users preserved for accurate global counts.

## Materialization Strategy

- Intermediates → tables (persist complex transforms, faster downstream reads)
- Mart (`dim_users`) → table (optimized for BI queries)

Edit `dbt_project.yml` to change (e.g., to view).

## Tests & Quality

`marts/marts_schema.yml` includes:
- `not_null` on `user_id`
- `dbt_utils.unique_combination_of_columns` on [user_id, sponsor_id, site_id]

---

## Local Execution (Showcase Only)

This repo is designed for code review and demonstration. No raw data or live warehouse connection is provided.

- Local `dbt compile`, `dbt debug`, `dbt run`, and `dbt test` are expected to fail on authentication because dummy placeholder credentials are used (no real GCP/BigQuery access is included).
- This is intentional — the project demonstrates modeling patterns, structure, and best practices, not a runnable pipeline.
- The underlying logic was validated and executed successfully in BigQuery as a monolithic query before refactoring into dbt models.
- dbt layers were developed to modularize and improve maintainability; compilation and tests were verified in a controlled environment.
- For reviewers: inspect the source SQL files directly, review the schema tests, lineage, and comments to understand the logic and design choices.

### To experiment locally (optional):
1. Enable BigQuery in a GCP project (free tier ok).
2. Install Google Cloud SDK.
3. `gcloud auth application-default login`
4. Copy `profiles.example.yml` → `~/.dbt/profiles.yml`
5. Update project/dataset with real values.

See [dbt BigQuery docs](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup).

## Repo Structure

```
.
├── dbt_project.yml
├── profiles.example.yml
├── packages.yml
├── package-lock.yml
├── .gitignore
├── models/
│   ├── sources.yml
│   ├── intermediate/
│   │   ├── int_locations_clean.sql
│   │   └── int_user_attributions.sql
│   └── marts/
│       ├── dim_users.sql
│       └── marts_schema.yml
└── README.md
```

## Compatibility

* dbt Core ≥ 1.8
* Adapter: `dbt-bigquery`

---

## License

MIT License

Copyright (c) 2025-2026 Corin Stedman (space-lumps)

See the [LICENSE](LICENSE) file for full details.