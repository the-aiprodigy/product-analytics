# NordicFlow CRM — Product Analytics Pipeline
## README: Architecture, Data Dictionary & Developer Guide
**Version:** 1.0  
**Last Updated:** April 2026  
**Maintained by:** Data & Analytics Team  

---

## 1. Project Overview

This repository contains the full analytical data pipeline for the NordicFlow CRM Product Analytics Initiative. It transforms five raw source datasets into a structured, analytics-ready star schema that powers a Power BI executive dashboard and supports self-serve reporting by Product, Sales, Customer Success, and Growth teams.

The pipeline is structured in three explicit layers:

```
SOURCE FILES  →  STAGE  →  CLEAN  →  FACT / DIMENSION
```

Each layer has a distinct responsibility. Raw data is never written to or queried from directly by reporting tools. All Power BI datasets connect exclusively to the Fact and Dimension layer.

---

## 2. Source Data Inventory

Five source files feed this pipeline. All are assumed to be loaded into an Oracle staging schema via a scheduled ingestion job (flat file load or API extract).

| Source File | Format | Rows | Load Frequency | Description |
|---|---|---|---|---|
| `Accounts.csv` | CSV | 120 | Daily | One record per customer account — includes segment, country, trial dates, and acquisition channel |
| `Deals.csv` | CSV | 6,443 | Daily | One record per CRM deal — includes pipeline, stage, status, amount, and currency |
| `ProductEvents.csv` | CSV | 61,747 | Daily | One row per product event fired — the core behavioural dataset |
| `Users.csv` | CSV | 400 | Daily | One record per platform user — includes role, status, and last activity |
| `Geography.xlsx` | Excel | 8 (7 distinct) | On-change | Reference table mapping country codes to regions, markets, and sales territories |

---

## 3. Layer Architecture

### Layer 1: Stage

**Purpose:** Exact structural copy of the source file. No transformations. No filtering. The stage layer acts as an audit trail and reload point.

**Naming Convention:** `STAGE_<source_name>` (e.g., `STAGE_ACCOUNTS`)

**Characteristics:**
- All columns VARCHAR2 to avoid type coercion errors on load
- Includes a system-added `loaded_at` timestamp (SYSDATE at insert time)
- Includes a `batch_id` column for tracking which load run produced each row
- No primary key constraints — duplicates are permitted at this layer
- Never truncated without explicit sign-off — used for debugging and reload

**Tables:**
- `STAGE_ACCOUNTS`
- `STAGE_DEALS`
- `STAGE_PRODUCT_EVENTS`
- `STAGE_USERS`
- `STAGE_GEOGRAPHY`

---

### Layer 2: Clean

**Purpose:** Resolves all data quality issues identified during profiling. Standardises values, removes test data, deduplicates, handles nulls, and applies canonical mappings. The clean layer is the authoritative, trusted version of each source entity.

**Naming Convention:** `CLEAN_<entity_name>` (e.g., `CLEAN_ACCOUNTS`)

**Characteristics:**
- Typed columns (DATE, NUMBER, VARCHAR2 with defined lengths)
- All string fields normalised: LOWER(TRIM(...)) applied consistently
- Canonical event name mapping applied (e.g., ViewDashboard → view_dashboard)
- Test events excluded (is_test_event = FALSE)
- Duplicate geography rows removed
- Flag columns added for key data quality issues (is_amount_missing, is_trial_date_missing, etc.)
- Surrogate keys assigned where natural keys are missing

**Tables:**
- `CLEAN_ACCOUNTS`
- `CLEAN_DEALS`
- `CLEAN_PRODUCT_EVENTS`
- `CLEAN_USERS`
- `CLEAN_GEOGRAPHY`

---

### Layer 3: Fact & Dimension (Star Schema)

**Purpose:** The analytics-optimised presentation layer. Organises cleaned data into a star schema for efficient Power BI querying. Fact tables contain measurable events and transactions. Dimension tables contain descriptive attributes used for slicing and filtering.

**Naming Convention:** `DIM_<entity>` and `FACT_<entity>`

**Dimension Tables:**
- `DIM_DATE` — Calendar spine with day, week, month, quarter, year attributes
- `DIM_ACCOUNT` — One row per account with all descriptive attributes
- `DIM_USER` — One row per user with role, status, and admin flag
- `DIM_GEOGRAPHY` — One row per country code with region, market, and sales territory
- `DIM_EVENT_TYPE` — Canonical event name reference with feature category classification

**Fact Tables:**
- `FACT_PRODUCT_EVENTS` — One row per clean product event; grain: event
- `FACT_DEALS` — One row per deal with EUR-converted amount; grain: deal
- `FACT_ACCOUNT_ACTIVATION` — One row per account summarising activation milestone dates and flags; grain: account

---

## 4. Full Data Dictionary

### STAGE_ACCOUNTS / CLEAN_ACCOUNTS / DIM_ACCOUNT

| Column | Type (Clean/Dim) | Source | Description | Quality Notes |
|---|---|---|---|---|
| account_id | VARCHAR2(50) | Accounts.csv | Unique account identifier | No nulls in source |
| account_name | VARCHAR2(255) | Accounts.csv | Legal or trading name of the customer | No nulls |
| country_code | VARCHAR2(10) | Accounts.csv | ISO country code | Joined to DIM_GEOGRAPHY |
| city | VARCHAR2(100) | Accounts.csv | City of primary office | No nulls |
| industry | VARCHAR2(100) | Accounts.csv | Industry vertical | 4 nulls → defaulted to 'unknown' |
| employee_band | VARCHAR2(20) | Accounts.csv | Headcount band (20-49, 50-99, 100-199, 200-300) | Clean in source |
| segment | VARCHAR2(50) | Accounts.csv | Commercial segment: SMB or Mid-Market | Clean in source |
| created_at | DATE | Accounts.csv | Account creation timestamp | No nulls |
| trial_start_date | DATE | Accounts.csv | Date trial period began | 27 nulls → is_trial_date_missing flag |
| trial_end_date | DATE | Accounts.csv | Date trial period ended | 27 nulls → is_trial_date_missing flag |
| account_status | VARCHAR2(50) | Accounts.csv | Current status: active, churned, cancelled | Mixed case → LOWER(TRIM()) applied |
| acquisition_channel | VARCHAR2(50) | Accounts.csv | How the account was acquired | Values: sales-led, content, partner, product-led, paid_search |
| is_trial_date_missing | NUMBER(1) | Derived | 1 if trial dates are null | Clean layer flag |
| loaded_at | DATE | System | Timestamp of stage load | Added at ingest |

---

### STAGE_DEALS / CLEAN_DEALS / FACT_DEALS

| Column | Type (Clean/Fact) | Source | Description | Quality Notes |
|---|---|---|---|---|
| deal_id | VARCHAR2(50) | Deals.csv | Unique deal identifier | No nulls |
| account_id | VARCHAR2(50) | Deals.csv | FK to account | No nulls |
| owner_user_id | VARCHAR2(50) | Deals.csv | FK to user who owns this deal | No nulls |
| pipeline_id | VARCHAR2(50) | Deals.csv | FK to pipeline configuration | 8 distinct pipelines |
| current_stage_id | VARCHAR2(50) | Deals.csv | Current stage within pipeline | 112 nulls → is_stage_missing flag |
| status | VARCHAR2(20) | Deals.csv | Deal status: open, won, lost | Mixed case → LOWER(TRIM()) applied |
| created_at | DATE | Deals.csv | Deal creation timestamp | No nulls |
| closed_at | DATE | Deals.csv | Deal close timestamp | NULL for open deals — expected |
| last_stage_changed_at | DATE | Deals.csv | Last stage transition timestamp | No nulls |
| amount | NUMBER(15,2) | Deals.csv | Deal value in original currency | 227 nulls → is_amount_missing flag |
| currency | VARCHAR2(10) | Deals.csv | Deal currency: DKK, EUR, GBP, NOK, SEK | No nulls |
| amount_eur | NUMBER(15,2) | Derived | Amount converted to EUR using static rates | Null where amount is null |
| country_code | VARCHAR2(10) | Deals.csv | Country where deal was created | No nulls |
| source_system | VARCHAR2(50) | Deals.csv | How the deal was created: manual_import, integration, crm_backend | No nulls |
| days_to_close | NUMBER(10) | Derived | Closed_at minus created_at in days | Null for open deals |
| is_amount_missing | NUMBER(1) | Derived | 1 if amount is null | Clean layer flag |
| is_stage_missing | NUMBER(1) | Derived | 1 if current_stage_id is null | Clean layer flag |

**EUR Conversion Rates (Static Reference — update quarterly):**

| Currency | Rate to EUR |
|---|---|
| EUR | 1.0000 |
| GBP | 1.1700 |
| DKK | 0.1340 |
| NOK | 0.0870 |
| SEK | 0.0880 |

---

### STAGE_PRODUCT_EVENTS / CLEAN_PRODUCT_EVENTS / FACT_PRODUCT_EVENTS

| Column | Type (Clean/Fact) | Source | Description | Quality Notes |
|---|---|---|---|---|
| event_id | VARCHAR2(100) | ProductEvents.csv | UUID of the event | 625 nulls → surrogate key assigned |
| event_name | VARCHAR2(100) | ProductEvents.csv | Name of the product event | Mixed case → canonical mapping applied |
| event_name_canonical | VARCHAR2(100) | Derived | Standardised event name | See canonical mapping table below |
| feature_category | VARCHAR2(50) | Derived | High-level feature grouping | From DIM_EVENT_TYPE |
| is_core_event | NUMBER(1) | Derived | 1 if event is a core activation event | True for: create_deal, log_activity, move_deal_stage, enable_automation |
| user_id | VARCHAR2(50) | ProductEvents.csv | FK to user | 1,181 nulls — anonymous events retained |
| account_id | VARCHAR2(50) | ProductEvents.csv | FK to account | 653 nulls → excluded from account KPIs |
| deal_id | VARCHAR2(50) | ProductEvents.csv | FK to deal (where applicable) | 24,752 nulls — not all events relate to deals |
| event_timestamp | DATE | ProductEvents.csv | When the event occurred | No nulls |
| ingested_at | DATE | ProductEvents.csv | When the event was ingested | No nulls |
| event_date | DATE | ProductEvents.csv | Date portion of event_timestamp | 4,941 nulls → derived from event_timestamp |
| platform | VARCHAR2(20) | ProductEvents.csv | web or mobile | No nulls |
| device_type | VARCHAR2(20) | ProductEvents.csv | desktop or mobile | No nulls |
| app_version | VARCHAR2(20) | ProductEvents.csv | Application version at time of event | No nulls |
| country_code | VARCHAR2(10) | ProductEvents.csv | Country associated with the event | No nulls |
| is_test_event | NUMBER(1) | ProductEvents.csv | 1 if this is a test event | 1,195 test events excluded at clean layer |
| source_system | VARCHAR2(50) | ProductEvents.csv | frontend, automation, or backend | No nulls |

**Canonical Event Name Mapping:**

| Raw Event Name | Canonical Name | Feature Category | Is Core Event |
|---|---|---|---|
| login | login | Authentication | No |
| Login | login | Authentication | No |
| user_login | login | Authentication | No |
| create_deal | create_deal | Pipeline Management | Yes |
| log_activity | log_activity | Activity Logging | Yes |
| move_deal_stage | move_deal_stage | Pipeline Management | Yes |
| enable_automation | enable_automation | Workflow Automation | Yes |
| view_dashboard | view_dashboard | Reporting | No |
| ViewDashboard | view_dashboard | Reporting | No |
| invite_user | invite_user | Collaboration | No |

---

### STAGE_USERS / CLEAN_USERS / DIM_USER

| Column | Type (Clean/Dim) | Source | Description | Quality Notes |
|---|---|---|---|---|
| user_id | VARCHAR2(50) | Users.csv | Unique user identifier | No nulls |
| account_id | VARCHAR2(50) | Users.csv | FK to account | 3 nulls → is_orphaned_user flag |
| full_name | VARCHAR2(255) | Users.csv | User display name | No nulls |
| email | VARCHAR2(255) | Users.csv | User email address | No nulls |
| job_role | VARCHAR2(50) | Users.csv | Role: sales_rep, sales_manager, revops, customer_success, admin | 10 nulls → defaulted to 'unknown' |
| user_status | VARCHAR2(20) | Users.csv | active, inactive, invited | Mixed case → LOWER(TRIM()) applied |
| created_at | DATE | Users.csv | User creation timestamp | No nulls |
| last_seen_at | DATE | Users.csv | Last platform activity timestamp | 15 nulls — treated as never seen |
| timezone | VARCHAR2(50) | Users.csv | User timezone | No nulls |
| locale | VARCHAR2(10) | Users.csv | Locale code (e.g., en-GB) | No nulls |
| is_admin | NUMBER(1) | Users.csv | 1 if user has admin privileges | No nulls |
| is_orphaned_user | NUMBER(1) | Derived | 1 if account_id is null | Clean layer flag |
| days_since_last_seen | NUMBER(10) | Derived | SYSDATE - last_seen_at | Null if last_seen_at is null |

---

### STAGE_GEOGRAPHY / CLEAN_GEOGRAPHY / DIM_GEOGRAPHY

| Column | Type (Clean/Dim) | Source | Description | Quality Notes |
|---|---|---|---|---|
| country_code | VARCHAR2(10) | Geography.xlsx | ISO country code (PK) | Deduped; normalised to UPPER(TRIM()) |
| country_name | VARCHAR2(100) | Geography.xlsx | Full country name | No nulls after dedup |
| region | VARCHAR2(100) | Geography.xlsx | Geographic region (Nordics, DACH, Benelux, etc.) | No nulls |
| market | VARCHAR2(100) | Geography.xlsx | Commercial market grouping | UK null → defaulted to 'UK & Ireland' |
| currency | VARCHAR2(10) | Geography.xlsx | Local currency code | No nulls |
| sales_region | VARCHAR2(100) | Geography.xlsx | NordicFlow sales territory | No nulls |

---

### DIM_DATE (Generated)

| Column | Type | Description |
|---|---|---|
| date_id | NUMBER(8) | YYYYMMDD integer key |
| full_date | DATE | Calendar date |
| day_of_week | NUMBER(1) | 1 = Monday, 7 = Sunday |
| day_name | VARCHAR2(10) | Monday, Tuesday, etc. |
| week_number | NUMBER(2) | ISO week number |
| month_number | NUMBER(2) | Month 1–12 |
| month_name | VARCHAR2(10) | January, February, etc. |
| quarter_number | NUMBER(1) | 1–4 |
| year_number | NUMBER(4) | Full year |
| is_weekend | NUMBER(1) | 1 if Saturday or Sunday |
| fiscal_year | NUMBER(4) | Fiscal year (assumed Jan–Dec) |

---

### DIM_EVENT_TYPE

| Column | Type | Description |
|---|---|---|
| event_name_canonical | VARCHAR2(100) | PK — standardised event name |
| feature_category | VARCHAR2(50) | High-level grouping |
| is_core_event | NUMBER(1) | 1 if part of activation definition |
| is_authentication | NUMBER(1) | 1 if login-type event |
| display_label | VARCHAR2(100) | Human-readable label for dashboards |

---

### FACT_ACCOUNT_ACTIVATION

This table pre-computes key activation milestones per account to support efficient KPI calculation without scanning the full events table at query time.

| Column | Type | Description |
|---|---|---|
| account_id | VARCHAR2(50) | PK — one row per account |
| trial_start_date | DATE | From DIM_ACCOUNT |
| first_login_at | DATE | Timestamp of first login event |
| first_core_event_at | DATE | Timestamp of first core activation event |
| first_core_event_name | VARCHAR2(100) | Which event fired first |
| hours_to_first_login | NUMBER(10,2) | Hours from trial start to first login |
| hours_to_first_core_event | NUMBER(10,2) | Hours from trial start to first core event |
| is_activated_7d | NUMBER(1) | 1 if core event fired within 7 days of trial start |
| is_activated_14d | NUMBER(1) | 1 if core event fired within 14 days of trial start |
| has_created_deal | NUMBER(1) | 1 if account has ever fired create_deal |
| has_logged_activity | NUMBER(1) | 1 if account has ever fired log_activity |
| has_moved_stage | NUMBER(1) | 1 if account has ever fired move_deal_stage |
| has_enabled_automation | NUMBER(1) | 1 if account has ever fired enable_automation |
| distinct_features_used | NUMBER(3) | Count of distinct canonical event types fired |
| active_users_30d | NUMBER(5) | Distinct users with an event in last 30 days |
| last_core_event_at | DATE | Most recent core event — used for churn risk scoring |
| days_since_last_core_event | NUMBER(10) | SYSDATE minus last_core_event_at |
| engagement_score | NUMBER(5,2) | Composite 0–100 score |
| health_band | VARCHAR2(10) | Red, Amber, or Green |

---

## 5. ETL Execution Order

The following sequence must be respected. Each layer depends on the one before it.

```
Step 1:  Load STAGE_GEOGRAPHY
Step 2:  Load STAGE_ACCOUNTS
Step 3:  Load STAGE_USERS
Step 4:  Load STAGE_DEALS
Step 5:  Load STAGE_PRODUCT_EVENTS

Step 6:  Build CLEAN_GEOGRAPHY
Step 7:  Build CLEAN_ACCOUNTS
Step 8:  Build CLEAN_USERS
Step 9:  Build CLEAN_DEALS
Step 10: Build CLEAN_PRODUCT_EVENTS

Step 11: Build DIM_DATE         (independent — can run anytime)
Step 12: Build DIM_GEOGRAPHY    (depends on CLEAN_GEOGRAPHY)
Step 13: Build DIM_ACCOUNT      (depends on CLEAN_ACCOUNTS, DIM_GEOGRAPHY)
Step 14: Build DIM_USER         (depends on CLEAN_USERS, DIM_ACCOUNT)
Step 15: Build DIM_EVENT_TYPE   (static seed — insert once)
Step 16: Build FACT_DEALS       (depends on DIM_ACCOUNT, DIM_GEOGRAPHY)
Step 17: Build FACT_PRODUCT_EVENTS (depends on DIM_ACCOUNT, DIM_USER, DIM_EVENT_TYPE)
Step 18: Build FACT_ACCOUNT_ACTIVATION (depends on FACT_PRODUCT_EVENTS, DIM_ACCOUNT)
```

---

## 6. Power BI Data Model Recommendations

### Relationships to Define in Power BI

| From Table | From Column | To Table | To Column | Cardinality |
|---|---|---|---|---|
| FACT_PRODUCT_EVENTS | account_id | DIM_ACCOUNT | account_id | Many-to-One |
| FACT_PRODUCT_EVENTS | user_id | DIM_USER | user_id | Many-to-One |
| FACT_PRODUCT_EVENTS | event_date | DIM_DATE | full_date | Many-to-One |
| FACT_PRODUCT_EVENTS | event_name_canonical | DIM_EVENT_TYPE | event_name_canonical | Many-to-One |
| FACT_PRODUCT_EVENTS | country_code | DIM_GEOGRAPHY | country_code | Many-to-One |
| FACT_DEALS | account_id | DIM_ACCOUNT | account_id | Many-to-One |
| FACT_DEALS | country_code | DIM_GEOGRAPHY | country_code | Many-to-One |
| FACT_DEALS | created_at (date) | DIM_DATE | full_date | Many-to-One |
| FACT_ACCOUNT_ACTIVATION | account_id | DIM_ACCOUNT | account_id | One-to-One |
| DIM_ACCOUNT | country_code | DIM_GEOGRAPHY | country_code | Many-to-One |
| DIM_USER | account_id | DIM_ACCOUNT | account_id | Many-to-One |

### Recommended Dashboard Pages

| Page | Audience | Key Visuals |
|---|---|---|
| Executive Summary | C-Suite | MRR trend, churn rate, trial conversion, active accounts by region |
| Activation Funnel | Product, CS | Funnel chart by step, TTFCA distribution, activation rate by channel |
| Engagement & Adoption | Product, CS | Feature adoption heatmap, WAU trend, engagement score distribution |
| Pipeline & Revenue | Sales | Win rate, ADV, pipeline velocity, open pipeline by stage |
| Account Health | Customer Success | Health band table, days since last activity, churn risk ranking |
| Geography | All | Map visual, KPIs by country/region/sales_region |
| Data Quality Monitor | RevOps | Null rates, test event volume, canonical mapping gaps |

---

## 7. Folder Structure

```
nordicflow_analytics/
├── README.md                          ← This file
├── requirements/
│   └── stakeholder_requirements.md   ← Stakeholder KPI definitions
├── sql/
│   ├── 01_stage/
│   │   ├── create_stage_accounts.sql
│   │   ├── create_stage_deals.sql
│   │   ├── create_stage_product_events.sql
│   │   ├── create_stage_users.sql
│   │   └── create_stage_geography.sql
│   ├── 02_clean/
│   │   ├── create_clean_accounts.sql
│   │   ├── create_clean_deals.sql
│   │   ├── create_clean_product_events.sql
│   │   ├── create_clean_users.sql
│   │   └── create_clean_geography.sql
│   └── 03_fact_dimension/
│       ├── create_dim_date.sql
│       ├── create_dim_geography.sql
│       ├── create_dim_account.sql
│       ├── create_dim_user.sql
│       ├── create_dim_event_type.sql
│       ├── create_fact_deals.sql
│       ├── create_fact_product_events.sql
│       └── create_fact_account_activation.sql
└── powerbi/
    └── NordicFlow_Analytics.pbix
```

---

## 8. Assumptions & Constraints

- All currency conversion rates are static. A future enhancement would replace these with a live exchange rate dimension updated daily.
- The fiscal year is assumed to align with the calendar year (January–December). Confirm with Finance before publishing P&L dashboards.
- The activation definition (≥1 core event within 7 days) is an initial working assumption. It should be validated against historical conversion data once the pipeline is live and adjusted if needed.
- The engagement score weighting (25 pts per component) is equally weighted as a starting point. Customer Success should review and recalibrate based on which signals most closely predict churn.
- Geography data covers 7 distinct countries. Events and deals referencing country codes not in DIM_GEOGRAPHY will be assigned to a catch-all 'Unknown' geography record to prevent join failures.
- Product Events data contains events sourced from frontend, automation, and backend systems. Backend and automation events are included in all KPIs unless otherwise noted.
