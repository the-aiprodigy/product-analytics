# NordicFlow CRM — Product Analytics Pipeline
## README: Architecture, Data Dictionary & Developer Guide
**Version:** 2.0  |  **Last Updated:** April 2026  |  **Maintained by:** Data & Analytics Team

---

## 1. Project Overview

This repository contains the full analytical data pipeline for the NordicFlow CRM Product Analytics Initiative. It transforms five raw source datasets into a structured, analytics-ready star schema that powers a Power BI executive dashboard and supports self-serve reporting across Product, Sales, Customer Success, and Growth teams.

---

## 2. Pipeline Architecture

The pipeline follows a strict four-layer architecture. Data flows in one direction only. No layer is skipped, and no reporting tool connects to anything other than the ANALYTICS_ layer.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SOURCE_        Raw immutable extracts                                      │
│                 Flat files loaded as-is from the ingestion tool.            │
│                 Never modified. Retained as audit trail.                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  STAGE_         Typed ingestion (Bronze-equivalent)                         │
│                 Source data typed into Oracle tables. All source columns    │
│                 preserved. No business logic. Includes batch_id and         │
│                 loaded_at metadata. TRANSFORM_ reads from here.             │
├─────────────────────────────────────────────────────────────────────────────┤
│  TRANSFORM_     Cleaned and enriched layer (Silver-equivalent)              │
│                 Materialised views built on STAGE_. Applies all data        │
│                 quality fixes: case normalisation, deduplication, null      │
│                 handling, canonical event mapping, EUR conversion,          │
│                 derived flags. ANALYTICS_ reads from here.                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  ANALYTICS_     Gold / Power BI reporting layer                             │
│                 Star schema of Dimension and Fact tables built from         │
│                 TRANSFORM_. Power BI connects exclusively here.             │
│                 All joins pre-built. Measures are simple aggregations.      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Source Data Inventory

| Source File | Format | Rows (approx.) | Load Frequency | Description |
|---|---|---|---|---|
| Accounts.csv | CSV | 120 | Daily full-reload | One record per customer account |
| Deals.csv | CSV | 6,443 | Daily full-reload | One record per CRM deal |
| ProductEvents.csv | CSV | 61,747 | Daily append | One row per product event fired |
| Users.csv | CSV | 400 | Daily full-reload | One record per platform user |
| Geography.xlsx | Excel | 8 (7 distinct) | On-change | Country → region/market/territory mapping |

---

## 4. Table Inventory by Layer

### STAGE_ Layer

| Table | Source | Grain | Notes |
|---|---|---|---|
| STAGE_ACCOUNTS | Accounts.csv | One row per source row | Full-reload daily |
| STAGE_DEALS | Deals.csv | One row per source row | Full-reload daily |
| STAGE_PRODUCT_EVENTS | ProductEvents.csv | One row per source row | Append daily |
| STAGE_USERS | Users.csv | One row per source row | Full-reload daily |
| STAGE_GEOGRAPHY | Geography.xlsx | One row per source row | On-change; includes duplicates |
| STAGE_LOAD_LOG | System | One row per table per run | ETL audit log |

### TRANSFORM_ Layer

| Object | Type | Source | Notes |
|---|---|---|---|
| TRANSFORM_GEOGRAPHY | Materialised View | STAGE_GEOGRAPHY | Deduped; NULL market defaulted |
| TRANSFORM_ACCOUNTS | Materialised View | STAGE_ACCOUNTS | Status normalised; flags added |
| TRANSFORM_USERS | Materialised View | STAGE_USERS | Role/status normalised; orphan flag |
| TRANSFORM_DEALS | Materialised View | STAGE_DEALS | EUR conversion; win/loss flags |
| TRANSFORM_PRODUCT_EVENTS | Materialised View | STAGE_PRODUCT_EVENTS | Test events excluded; canonical names |
| TRANSFORM_EVENT_CANONICAL_MAP | View (static) | Hardcoded | Raw event → canonical name mapping |
| TRANSFORM_CURRENCY_RATES | Table (reference) | Manual seed | Static EUR rates; updated quarterly |

### ANALYTICS_ Layer (Star Schema)

| Table | Type | Grain | Primary KPIs Served |
|---|---|---|---|
| ANALYTICS_DIM_DATE | Dimension | One row per calendar day | All time-series KPIs |
| ANALYTICS_DIM_GEOGRAPHY | Dimension | One row per country | Regional revenue, adoption by market |
| ANALYTICS_DIM_ACCOUNT | Dimension | One row per account | All account-level slicing |
| ANALYTICS_DIM_USER | Dimension | One row per user | WAU, seat utilisation, role adoption |
| ANALYTICS_DIM_EVENT_TYPE | Dimension | One row per canonical event | Feature adoption, funnel labelling |
| ANALYTICS_FACT_DEALS | Fact | One row per deal | Win rate, ADV, pipeline velocity |
| ANALYTICS_FACT_PRODUCT_EVENTS | Fact | One row per event | WAU, feature adoption, platform split |
| ANALYTICS_FACT_ACCOUNT_ACTIVATION | Fact | One row per account | Activation rate, TTFCA, health score |

---

## 5. Data Quality Issues & Resolutions

| Source | Issue | Detail | Resolution Layer |
|---|---|---|---|
| Geography | Duplicate country | FR appears twice | TRANSFORM_: ROW_NUMBER() dedup |
| Geography | Case inconsistency | `de` not `DE`, trailing spaces | TRANSFORM_: UPPER(TRIM()) |
| Geography | Null market | UK market NULL | TRANSFORM_: COALESCE to 'UK & Ireland' |
| Accounts | Mixed-case status | `ACTIVE` alongside `active` | TRANSFORM_: LOWER(TRIM()) |
| Accounts | Null trial dates | 27 accounts | TRANSFORM_: IS_TRIAL_DATE_MISSING flag |
| Accounts | Null industry | 4 accounts | TRANSFORM_: COALESCE to 'Unknown' |
| Deals | Mixed-case status | open/OPEN/Open etc. | TRANSFORM_: LOWER(TRIM()) |
| Deals | Null amount | 227 deals | TRANSFORM_: IS_AMOUNT_MISSING flag |
| Deals | Null stage | 112 deals | TRANSFORM_: IS_STAGE_MISSING flag |
| Product Events | Mixed-case event names | Login/login/user_login | TRANSFORM_: canonical map view |
| Product Events | Test events | ~1,195 rows | TRANSFORM_: WHERE IS_TEST_EVENT = 0 |
| Product Events | Null event_id | 625 rows | TRANSFORM_: SYS- surrogate key |
| Product Events | Null user_id | 1,181 rows | ANALYTICS_: retained; excluded from user KPIs |
| Product Events | Null account_id | 653 rows | ANALYTICS_: excluded from account KPIs |
| Users | Mixed-case status | Active/ACTIVE/active | TRANSFORM_: LOWER(TRIM()) |
| Users | Null account_id | 3 users | TRANSFORM_: IS_ORPHANED_USER flag |
| Users | Null job_role | 10 users | TRANSFORM_: COALESCE to 'unknown' |
| Users | Null last_seen_at | 15 users | TRANSFORM_: IS_NEVER_SEEN flag; recency = NULL |

---

## 6. Canonical Event Name Mapping

The following raw event name variants are mapped to their canonical forms in TRANSFORM_EVENT_CANONICAL_MAP. This view is the single source of truth for event standardisation.

| Raw Name (examples) | Canonical Name | Feature Category | Is Core Event |
|---|---|---|---|
| login, Login, user_login | login | Authentication | No |
| create_deal, Create_Deal | create_deal | Pipeline Management | Yes |
| log_activity, Log_Activity | log_activity | Activity Logging | Yes |
| move_deal_stage, Move_Deal_Stage | move_deal_stage | Pipeline Management | Yes |
| enable_automation, Enable_Automation | enable_automation | Workflow Automation | Yes |
| view_dashboard, ViewDashboard | view_dashboard | Reporting | No |
| invite_user, Invite_User | invite_user | Collaboration | No |

To add a new mapping: add a UNION ALL branch to TRANSFORM_EVENT_CANONICAL_MAP and a corresponding row to ANALYTICS_DIM_EVENT_TYPE.

---

## 7. EUR Currency Conversion

All deal amounts in ANALYTICS_FACT_DEALS are converted to EUR using static rates stored in TRANSFORM_CURRENCY_RATES. Only the row with EFFECTIVE_TO IS NULL is considered active.

| Currency | Rate to EUR | Last Updated |
|---|---|---|
| EUR | 1.0000 | Q1 2026 |
| GBP | 1.1700 | Q1 2026 |
| DKK | 0.1340 | Q1 2026 |
| NOK | 0.0870 | Q1 2026 |
| SEK | 0.0880 | Q1 2026 |

To update rates: INSERT a new row with the new EFFECTIVE_FROM date and UPDATE the prior row's EFFECTIVE_TO to the same date. Do not delete historical rates.

---

## 8. ETL Execution Order

```
STAGE_ Loads (run by ingestion tool / flat file loader)
  ├── STAGE_GEOGRAPHY
  ├── STAGE_ACCOUNTS
  ├── STAGE_USERS
  ├── STAGE_DEALS
  └── STAGE_PRODUCT_EVENTS

TRANSFORM_ Refresh  (REFRESH_TRANSFORM_LAYER procedure)
  ├── TRANSFORM_GEOGRAPHY          (no dependencies)
  ├── TRANSFORM_ACCOUNTS           (requires TRANSFORM_GEOGRAPHY)
  ├── TRANSFORM_USERS              (requires TRANSFORM_ACCOUNTS)
  ├── TRANSFORM_DEALS              (requires TRANSFORM_ACCOUNTS, TRANSFORM_CURRENCY_RATES)
  └── TRANSFORM_PRODUCT_EVENTS     (requires TRANSFORM_EVENT_CANONICAL_MAP)

ANALYTICS_ Refresh  (REFRESH_ANALYTICS_LAYER procedure)
  ├── ANALYTICS_DIM_GEOGRAPHY      (requires TRANSFORM_GEOGRAPHY)
  ├── ANALYTICS_DIM_ACCOUNT        (requires TRANSFORM_ACCOUNTS, DIM_GEOGRAPHY)
  ├── ANALYTICS_DIM_USER           (requires TRANSFORM_USERS, DIM_ACCOUNT)
  ├── ANALYTICS_FACT_DEALS         (requires TRANSFORM_DEALS)
  ├── ANALYTICS_FACT_PRODUCT_EVENTS (requires TRANSFORM_PRODUCT_EVENTS)
  └── ANALYTICS_FACT_ACCOUNT_ACTIVATION (requires all FACT + DIM tables above)
```

The master procedure **RUN_NORDICFLOW_PIPELINE** calls both sub-procedures in the correct order and is scheduled via DBMS_SCHEDULER to run at 03:00 UTC daily.

---

## 9. Power BI Data Model

### Relationships

| From | Column | To | Column | Cardinality |
|---|---|---|---|---|
| FACT_PRODUCT_EVENTS | ACCOUNT_ID | DIM_ACCOUNT | ACCOUNT_ID | Many-to-One |
| FACT_PRODUCT_EVENTS | USER_ID | DIM_USER | USER_ID | Many-to-One |
| FACT_PRODUCT_EVENTS | EVENT_DATE_SK | DIM_DATE | DATE_SK | Many-to-One |
| FACT_PRODUCT_EVENTS | EVENT_NAME_CANONICAL | DIM_EVENT_TYPE | EVENT_NAME_CANONICAL | Many-to-One |
| FACT_PRODUCT_EVENTS | COUNTRY_CODE | DIM_GEOGRAPHY | COUNTRY_CODE | Many-to-One |
| FACT_DEALS | ACCOUNT_ID | DIM_ACCOUNT | ACCOUNT_ID | Many-to-One |
| FACT_DEALS | CREATED_DATE_SK | DIM_DATE | DATE_SK | Many-to-One |
| FACT_DEALS | COUNTRY_CODE | DIM_GEOGRAPHY | COUNTRY_CODE | Many-to-One |
| FACT_ACCOUNT_ACTIVATION | ACCOUNT_ID | DIM_ACCOUNT | ACCOUNT_ID | One-to-One |
| DIM_USER | ACCOUNT_ID | DIM_ACCOUNT | ACCOUNT_ID | Many-to-One |
| DIM_ACCOUNT | COUNTRY_CODE | DIM_GEOGRAPHY | COUNTRY_CODE | Many-to-One |

### Recommended Dashboard Pages

| Page | Primary Table(s) | Audience |
|---|---|---|
| Executive Summary | FACT_ACCOUNT_ACTIVATION, FACT_DEALS | CEO / CFO |
| Activation Funnel | FACT_ACCOUNT_ACTIVATION, FACT_PRODUCT_EVENTS | Product, CS |
| Feature Adoption | FACT_PRODUCT_EVENTS, DIM_EVENT_TYPE | Product |
| Pipeline & Revenue | FACT_DEALS, DIM_ACCOUNT, DIM_GEOGRAPHY | Sales |
| Account Health | FACT_ACCOUNT_ACTIVATION, DIM_ACCOUNT | Customer Success |
| Geography | FACT_DEALS, FACT_PRODUCT_EVENTS, DIM_GEOGRAPHY | All |
| Data Quality Monitor | STAGE_LOAD_LOG, TRANSFORM_ flag columns | RevOps |

---

## 10. File Structure

```
nordicflow_analytics/
├── README.md
├── requirements/
│   └── stakeholder_requirements.md
└── sql/
    ├── 01_stage_layer.sql         ← STAGE_ DDL + indexes + load log
    ├── 02_transform_layer.sql     ← TRANSFORM_ materialised views + refresh proc
    └── 03_analytics_layer.sql     ← ANALYTICS_ star schema + refresh proc + scheduler
```

---

## 11. Key Assumptions

- Fiscal year aligns with the calendar year (January–December). Confirm with Finance before publishing quarterly P&L dashboards.
- The activation definition (≥1 core event within 7 days of trial start) is a working assumption. Validate against conversion data once live and recalibrate if needed.
- The engagement score weighting (25 pts per component) is equally weighted. Customer Success should recalibrate once correlations with actual churn are measurable.
- EUR conversion rates are static. Introduce a live exchange rate dimension in a future phase.
- Events with no account_id (~653 rows) are automation or backend events not attributable to a specific account. They are retained in TRANSFORM_ and ANALYTICS_FACT_PRODUCT_EVENTS but excluded from all account-scoped measures via WHERE ACCOUNT_ID IS NOT NULL in Power BI measures.
- The catch-all geography row (country_code = 'ZZ') prevents join failures for events/deals referencing unmapped country codes. These should be investigated and resolved via STAGE_LOAD_LOG monitoring.
