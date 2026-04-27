# NordicFlow Product Analytics Data Pipeline

## Overview
This pipeline ingests raw product events and user metadata, cleans them, and creates curated data marts for activation, engagement, and churn analysis. The processing is split into three distinct layers:

- **Raw** – source‑faithful staging tables
- **Transform** – cleaned, deduplicated, and joined data
- **Analytics** – business‑ready dimensions and fact tables

## Directory Structure
- `project-understanding.md` : Full business context, challenges, and why this pipeline matters.
- `01_raw/` : Staging tables that mirror source data exactly.
- `02_transform/` : Deduplicated, type‑cast, and joined data.
- `03_analytics/` : Business‑friendly dimensional and fact tables.

## Execution Order
1. **Raw layer**: Run `01_raw/stage_events.sql` then `01_raw/stage_users.sql` (order independent).
2. **Transform layer**: Run `02_transform/clean_users.sql` first, then `02_transform/normalize_events.sql` (events depend on users).
3. **Analytics layer**: Run `03_analytics/dim_accounts.sql`, then `03_analytics/fact_product_usage.sql`, and optionally `03_analytics/fact_weekly_account_usage.sql`.

## Key Definitions
- **Core Value Events**: `create_deal`, `update_deal_stage`, `pipeline_configured`, `workflow_automation_triggered`  
- **Surface Events**: `login`, `page_view`, `dashboard_click`  
- These definitions are explained in `project-understanding.md` (Section 2) and used in the analytics‑layer fact tables.

## Usage Notes
- The **transform** layer is appropriate for exploratory analysis.  
- The **analytics** layer is intended for company‑wide dashboards and cross‑team KPI reporting.  
- Any metric changes should be reflected first in this pipeline and documented here.