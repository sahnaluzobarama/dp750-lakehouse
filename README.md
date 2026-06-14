# dp750-lakehouse
Azure Databricks medallion lakehouse tracking Ireland's grid surplus 
A medallion architecture lakehouse built on Azure Databricks, tracking 
Ireland's electricity generation and demand to answer one question:

**How often does wind generation alone exceed national demand, 
and is that surplus problem getting worse over time?**

## Data source
ENTSO-E Transparency Platform (transparency.entsoe.eu) — Ireland (IE), 
30-minute intervals, January 2022 to present, updated incrementally.

## Architecture
- **Bronze** — raw API data, append-only
- **Silver** — cleaned, deduplicated, with derived metrics 
  (surplus_mw, wind_coverage, is_surplus_event)
- **Gold** — aggregated insights (surplus hours per year, seasonal patterns)

## Status
🚧 Work in progress — built as part of an 11-week DP-750 study plan.

- [x] Week 1 — Environment setup
- [x] Week 2 — Bronze ingestion layer
- [ ] Week 7 — Silver layer
- [ ] Week 9 — Pipeline orchestration
