# dbt User 360 Dimension in BigQuery
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![dbt Core](https://img.shields.io/badge/dbt_Core-≥1.8-orange)](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/Older%20versions)
[![dbt-bigquery](https://img.shields.io/badge/Adapter-dbt--bigquery-blue)](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
[![Release](https://img.shields.io/github/v/release/space-lumps/bigquery-dbt-user-dimension?color=green)](https://github.com/space-lumps/bigquery-dbt-user-dimension/releases)

## Overview

This dbt project demonstrates a clean, maintainable user 360 dimension (`dim_users`) in BigQuery. It aggregates user identity with resolved location and multi-path attribution (sponsor/site/classroom) from anonymized platform data (professional networking/resource platform connecting career aspirants with field experts).

## Project Background & Evolution

These models originated as a single, complex BigQuery SQL script that combined user core data, location resolution, and multi-route attribution in one query.

dbt was introduced afterward to improve the workflow by:
- Extracting location resolution logic into `int_locations_clean` (reusable, testable, easier to maintain)
- Separating user attribution unification into `int_user_attributions` (handles multiple paths cleanly)
- Keeping the final `dim_users` mart focused on output shape and grain
- Adding schema tests, documentation, and materialization control

This refactor makes the pipeline more modular, debuggable, and scalable while preserving the original business logic.

### Key goals:
- Shift complex logic from BI tools into dbt for consistency, speed, and testability.
- Resolve hierarchical location data with deduplication and prioritization.
- Unify user associations across routes while preserving unlinked users.
- Enforce grain, schema tests, and documentation for BI/reporting reliability.

## Key Features

- Denormalized user dimension with identity + location + attribution context
- Full daily refresh (small scale <50k rows → no incrementals needed)
- Intermediate models materialized as tables for performance/debugging
- Schema tests (not_null, unique combination) and dbt docs support
- Genericized names and placeholders — no real data or proprietary identifiers

## Data Flow

This project follows a standard dbt layered architecture:

- **Sources**: Raw platform tables (users, locations, attributions).
- **Intermediates**: Clean and transform static/reusable logic (location normalization, multi-path attribution unification).
- **Marts**: Final BI-ready dimension with enforced grain.

```mermaid
graph TD
    A[Raw Sources<br>users, locations, attributions] --> B[int_locations_clean<br>• Normalize & deduplicate hierarchical locations<br>• One best row per from_location_id]
    A --> C[int_user_attributions<br>• Unify multi-path user associations<br>• Stack classroom/ invite/sponsor paths]
    B --> D[dim_users<br>• Final user 360 dimension<br>• Grain: user_id + optional sponsor_id/site_id]
    C --> D
    D --> E[BI Tools / Reporting<br>Consistent, testable, fast queries]

    style B fill:#1e88e5,stroke:#0d47a1,stroke-width:2px,color:#fff
    style C fill:#1e88e5,stroke:#0d47a1,stroke-width:2px,color:#fff
    style D fill:#0d47a1,stroke:#003087,stroke-width:4px,color:#fff
```

## Model Layers

- **sources** — declared raw tables (generic placeholders)
- **intermediate** — reusable joins/transforms
  - `int_locations_clean.sql` — one row per `from_location_id` with best city/state/county/country + coordinates
  - `int_user_attributions.sql` — unified user ↔ sponsor/site/classroom associations
- **marts** — BI-ready output
  - `dim_users.sql` — final table (grain: user_id + optional sponsor_id + site_id)

## Location Resolution Logic

Single `from_location_id` can map to multiple types (city, county, state, country). The pipeline collects candidates, ranks cities (distance + heuristics), picks best per type, and emits one consistent row per source location — preventing ambiguous geography in downstream queries.

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

To experiment locally (optional):
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