# NordicFlow CRM — Product Analytics Initiative
## Stakeholder Requirements Document
**Version:** 1.0  
**Date:** April 2026  
**Prepared by:** Data & Analytics Team  
**Classification:** Internal — Analytics Engagement  

---

## 1. Executive Summary

This document formalises the analytical requirements for the NordicFlow CRM Product Analytics Initiative. It translates business challenges raised by leadership into measurable KPIs, maps each metric to the source data required to produce it, and defines the analytical data architecture that will power a Power BI executive dashboard.

NordicFlow operates in a competitive European B2B SaaS market. Leadership has identified four priority problems: inconsistent trial activation, shallow feature adoption, early-stage churn — particularly among SMB customers — and conflicting reporting across teams. This initiative directly addresses all four by establishing a single, trusted analytical layer from which all reporting will be derived.

---

## 2. Stakeholder Registry

| Stakeholder Group | Representative Role | Primary Interest | Reporting Cadence |
|---|---|---|---|
| Executive Leadership | CEO / CFO | Revenue growth, churn, pipeline health | Monthly |
| Product | Head of Product | Feature adoption, activation funnel | Weekly |
| Growth & Marketing | VP Growth | Acquisition channel ROI, trial conversion | Weekly |
| Customer Success | VP Customer Success | Engagement risk, health scoring | Weekly |
| Sales | Head of Sales | Win rates, pipeline velocity, deal value | Daily/Weekly |
| Revenue Operations | RevOps Lead | Data quality, funnel integrity, attribution | Ongoing |

---

## 3. Business Questions → KPI Translation

Each business question raised by stakeholders has been translated into a formally defined KPI with a clear calculation, the source tables it depends on, and the audience it serves.

---

### 3.1 Activation & Onboarding

**Business Question:** Are new trial accounts actually getting started with the product, or are they signing up and going dark?

#### KPI 1 — Trial Activation Rate

| Attribute | Definition |
|---|---|
| **Metric Name** | Trial Activation Rate |
| **Plain English** | The percentage of trial accounts where at least one user performs a defined "activation event" (create_deal, log_activity, move_deal_stage, or enable_automation) within 7 days of trial start |
| **Formula** | `Activated Accounts ÷ Total Trial Accounts Started × 100` |
| **Activation Threshold** | ≥ 1 core product event within 7 days of trial_start_date |
| **Core Events** | create_deal, log_activity, move_deal_stage, enable_automation |
| **Source Tables** | clean_accounts, fact_product_events |
| **Segmentation** | Segment (SMB / Mid-Market), Acquisition Channel, Country, Industry |
| **Audience** | Product, Growth, Customer Success |
| **Target** | ≥ 65% (to be baselined from historical data) |
| **Refresh** | Daily |

---

#### KPI 2 — Time to First Core Action (TTFCA)

| Attribute | Definition |
|---|---|
| **Metric Name** | Time to First Core Action |
| **Plain English** | The number of hours between an account's trial start and the first core product event fired by any user in that account |
| **Formula** | `MIN(event_timestamp) - trial_start_date` (hours, for activated accounts only) |
| **Source Tables** | clean_accounts, fact_product_events |
| **Segmentation** | Segment, Acquisition Channel, Industry |
| **Audience** | Product, Customer Success |
| **Target** | Median < 24 hours |
| **Refresh** | Daily |

---

#### KPI 3 — Activation Funnel Step Completion

| Attribute | Definition |
|---|---|
| **Metric Name** | Activation Funnel Completion by Step |
| **Plain English** | For each of the four core activation events, the proportion of trial accounts that have ever fired that event, enabling a funnel view of where drop-off occurs |
| **Steps (in order)** | 1. login → 2. create_deal → 3. log_activity → 4. move_deal_stage → 5. enable_automation |
| **Formula** | `Accounts with event ÷ Accounts entered trial × 100` |
| **Source Tables** | clean_accounts, fact_product_events |
| **Audience** | Product |
| **Refresh** | Weekly |

---

### 3.2 Engagement & Retention

**Business Question:** Beyond logging in, are users actually working inside the product? Are teams showing the kind of consistent usage that predicts long-term retention?

#### KPI 4 — Weekly Active Users (WAU) by Account

| Attribute | Definition |
|---|---|
| **Metric Name** | Weekly Active Users |
| **Plain English** | The number of distinct users per account who fire at least one non-login product event in a given ISO week |
| **Formula** | `COUNT(DISTINCT user_id) WHERE event_name != 'login' GROUP BY account_id, iso_week` |
| **Source Tables** | fact_product_events, dim_user, dim_account |
| **Segmentation** | Account, Segment, Country, Job Role |
| **Audience** | Customer Success, Product |
| **Refresh** | Daily |

---

#### KPI 5 — Feature Adoption Rate

| Attribute | Definition |
|---|---|
| **Metric Name** | Feature Adoption Rate |
| **Plain English** | The percentage of active accounts that have used each named feature (pipeline management, deal progression, activity logging, automation) at least once in the rolling 30-day window |
| **Features tracked** | create_deal, log_activity, move_deal_stage, enable_automation, view_dashboard, invite_user |
| **Formula** | `Accounts using feature ÷ Total active accounts × 100` |
| **Source Tables** | fact_product_events, dim_account |
| **Segmentation** | Feature, Segment, Country, Acquisition Channel |
| **Audience** | Product, Customer Success |
| **Refresh** | Weekly |

---

#### KPI 6 — Account Engagement Score (Composite)

| Attribute | Definition |
|---|---|
| **Metric Name** | Account Engagement Score |
| **Plain English** | A composite 0–100 score per account reflecting breadth and depth of product usage. Inputs: number of active users (30d), number of distinct feature types used (30d), deal activity in pipeline, and login recency |
| **Scoring Components** | Active users (25 pts), Feature breadth — distinct event types (25 pts), Deal pipeline activity (25 pts), Last login recency (25 pts) |
| **Formula** | Weighted sum of normalised component scores, capped at 100 |
| **Source Tables** | fact_product_events, fact_deals, dim_user, dim_account |
| **Output Use** | Customer health banding: Red (<40), Amber (40–69), Green (≥70) |
| **Audience** | Customer Success |
| **Refresh** | Weekly |

---

### 3.3 Revenue & Commercial Health

**Business Question:** What is happening in the sales pipeline? Are deals moving, and at what value?

#### KPI 7 — Deal Win Rate

| Attribute | Definition |
|---|---|
| **Metric Name** | Deal Win Rate |
| **Plain English** | The proportion of closed deals (won + lost) that resulted in a win |
| **Formula** | `Won Deals ÷ (Won Deals + Lost Deals) × 100` |
| **Note** | Excludes open/in-progress deals from the denominator |
| **Source Tables** | fact_deals, dim_account |
| **Segmentation** | Pipeline, Country, Segment, Quarter, Acquisition Channel |
| **Audience** | Sales, Executive Leadership |
| **Refresh** | Daily |

---

#### KPI 8 — Average Deal Value (ADV)

| Attribute | Definition |
|---|---|
| **Metric Name** | Average Deal Value |
| **Plain English** | The mean value of won deals converted to EUR for cross-currency comparability |
| **Formula** | `SUM(amount_eur) ÷ COUNT(won deals)` |
| **Currency Handling** | Convert all deal amounts to EUR using static reference rates by currency code |
| **Source Tables** | fact_deals, dim_account, dim_geography |
| **Segmentation** | Segment, Country, Industry, Pipeline, Quarter |
| **Audience** | Sales, Executive Leadership |
| **Refresh** | Daily |

---

#### KPI 9 — Pipeline Velocity

| Attribute | Definition |
|---|---|
| **Metric Name** | Pipeline Velocity |
| **Plain English** | The average number of days between deal creation and close (won or lost), indicating how quickly deals move through the sales process |
| **Formula** | `AVG(closed_at - created_at)` in days, for closed deals only |
| **Source Tables** | fact_deals, dim_account |
| **Segmentation** | Pipeline, Segment, Country, Quarter |
| **Audience** | Sales, RevOps |
| **Refresh** | Weekly |

---

#### KPI 10 — Open Pipeline Value

| Attribute | Definition |
|---|---|
| **Metric Name** | Open Pipeline Value |
| **Plain English** | The total EUR value of all currently open deals, segmented by pipeline stage |
| **Formula** | `SUM(amount_eur) WHERE status = 'open'` |
| **Source Tables** | fact_deals, dim_account, dim_geography |
| **Segmentation** | Pipeline, Stage, Country, Owner (User), Segment |
| **Audience** | Sales, Executive Leadership |
| **Refresh** | Daily |

---

### 3.4 Churn & Account Health

**Business Question:** Which accounts are at risk of churning? Can we identify warning signals early enough to intervene?

#### KPI 11 — Monthly Churn Rate

| Attribute | Definition |
|---|---|
| **Metric Name** | Monthly Account Churn Rate |
| **Plain English** | The percentage of accounts active at the start of a month that became churned or cancelled by end of month |
| **Formula** | `Accounts churned in month ÷ Accounts active at start of month × 100` |
| **Churned Status** | account_status IN ('churned', 'cancelled') |
| **Source Tables** | dim_account, fact_account_activation (for first active date) |
| **Segmentation** | Segment, Country, Acquisition Channel, Industry, Employee Band |
| **Audience** | Executive Leadership, Customer Success |
| **Refresh** | Monthly |

---

#### KPI 12 — Days Since Last Activity (Per Account)

| Attribute | Definition |
|---|---|
| **Metric Name** | Days Since Last Core Activity |
| **Plain English** | The number of days since any user in a given account last fired a core product event. A leading indicator of churn risk. |
| **Formula** | `CURRENT_DATE - MAX(event_date) WHERE event_name IN (core events)` per account |
| **Source Tables** | fact_product_events, dim_account |
| **Risk Bands** | Green: <14 days, Amber: 14–30 days, Red: >30 days |
| **Audience** | Customer Success |
| **Refresh** | Daily |

---

### 3.5 Acquisition & Growth

**Business Question:** Which channels are producing accounts that convert and retain? Is product-led growth outperforming paid acquisition?

#### KPI 13 — Trial-to-Paid Conversion Rate by Channel

| Attribute | Definition |
|---|---|
| **Metric Name** | Trial-to-Paid Conversion Rate |
| **Plain English** | The proportion of trial accounts that converted to active/paid status, broken down by acquisition channel |
| **Formula** | `Active accounts (post-trial) ÷ Total trial accounts started × 100` |
| **Source Tables** | dim_account |
| **Segmentation** | Acquisition Channel, Segment, Country, Cohort Month |
| **Audience** | Growth, Executive Leadership |
| **Refresh** | Monthly |

---

#### KPI 14 — New Account Growth (MoM)

| Attribute | Definition |
|---|---|
| **Metric Name** | Net New Accounts Month-on-Month |
| **Plain English** | The number of new accounts created in a given month, and the percentage change versus the prior month |
| **Formula** | `COUNT(account_id) WHERE created_at IN month` and `(current - prior) ÷ prior × 100` |
| **Source Tables** | dim_account |
| **Segmentation** | Country, Segment, Acquisition Channel, Industry |
| **Audience** | Growth, Executive Leadership |
| **Refresh** | Monthly |

---

### 3.6 Additional Recommended KPIs for Power BI Dashboard

The following metrics are not part of the original stakeholder brief but are strongly recommended based on the data available. They add significant analytical depth and are frequently requested once baseline dashboards are in production.

| KPI | Rationale | Tables Required |
|---|---|---|
| **Seat Utilisation Rate** | `Active users ÷ Total provisioned users` — identifies accounts paying for seats that go unused, a churn precursor | dim_user, fact_product_events |
| **Invite Acceptance Rate** | `Users with status = 'active' ÷ Users invited` — low acceptance signals onboarding friction at team level | dim_user |
| **Multi-Feature Adoption** | Accounts using 3+ distinct feature types — a strong predictor of long-term retention | fact_product_events |
| **Pipeline Stage Conversion Funnel** | The percentage of deals advancing from each stage to the next — identifies bottlenecks in the sales process | fact_deals |
| **Mobile vs Web Usage Split** | Engagement by platform (web/mobile) — informs product investment decisions | fact_product_events |
| **Admin User Engagement** | Whether admin users are configuring pipelines/automation — correlates with account maturity | dim_user, fact_product_events |
| **Cohort Retention** | Week-over-week retention by account creation cohort — the most reliable long-term health signal in SaaS | fact_product_events, dim_account |
| **Revenue by Market/Region** | Deal value aggregated by geography.sales_region — enables regional P&L view | fact_deals, dim_geography |
| **Deal Source Attribution** | Win rate and ADV by deal source_system (manual vs integration vs backend) — data quality signal | fact_deals |
| **User Last Seen Recency Distribution** | Distribution of days since last_seen_at across users — identifies accounts with dormant teams | dim_user |

---

## 4. Source-to-KPI Dependency Matrix

| KPI | stage_accounts | stage_deals | stage_product_events | stage_users | stage_geography |
|---|:---:|:---:|:---:|:---:|:---:|
| Trial Activation Rate | ✓ | | ✓ | | |
| Time to First Core Action | ✓ | | ✓ | | |
| Activation Funnel | ✓ | | ✓ | | |
| WAU by Account | | | ✓ | ✓ | |
| Feature Adoption Rate | | | ✓ | | |
| Account Engagement Score | | ✓ | ✓ | ✓ | |
| Deal Win Rate | ✓ | ✓ | | | |
| Average Deal Value | | ✓ | | | ✓ |
| Pipeline Velocity | | ✓ | | | |
| Open Pipeline Value | | ✓ | | | ✓ |
| Monthly Churn Rate | ✓ | | | | |
| Days Since Last Activity | ✓ | | ✓ | | |
| Trial-to-Paid Conversion | ✓ | | | | |
| New Account Growth | ✓ | | | | ✓ |

---

## 5. Data Quality Issues Identified in Source Data

The following data quality issues were identified during profiling of the five source files. All must be resolved in the **clean layer** before analytical tables are built.

| Source Table | Issue | Detail | Resolution |
|---|---|---|---|
| Geography | Duplicate country | FR (France) appears twice | Deduplicate on ROWNUM / ROW_NUMBER() |
| Geography | Case inconsistency | `de` instead of `DE`, `NL ` has trailing space | UPPER(TRIM(country_code)) |
| Geography | Null market | UK has NULL in market column | Default to 'UK & Ireland' |
| Accounts | Mixed case status | `ACTIVE` alongside `active` | LOWER(TRIM(account_status)) |
| Accounts | Null trial dates | 27 accounts with no trial_start_date / trial_end_date | Flag as is_trial_date_missing; exclude from conversion KPIs |
| Accounts | Null industry | 4 accounts missing industry | Default to 'Unknown' |
| Deals | Mixed case status | open/OPEN/Open, won/WON/Won, lost/LOST/Lost | LOWER(TRIM(status)) |
| Deals | Null amount | 227 deals with no amount value | Flag as is_amount_missing; exclude from revenue KPIs |
| Deals | Null current_stage_id | 112 deals with no stage | Flag; retain in pipeline counts but exclude from stage funnel |
| Deals | Null closed_at | 4,500 deals — expected for open deals | No fix needed; closed_at NULL = open deal |
| Product Events | Mixed case event names | ViewDashboard, Login, user_login alongside view_dashboard, login | LOWER(TRIM()) + canonical mapping |
| Product Events | Test events | 1,195 events with is_test_event = TRUE | Exclude from all analytics |
| Product Events | Null event_id | 625 events with no event_id | Assign surrogate key via ROWNUM |
| Product Events | Null user_id | 1,181 events with no user_id | Retain for account-level metrics; exclude from user-level metrics |
| Product Events | Null account_id | 653 events with no account_id | Exclude from all account-scoped KPIs |
| Users | Mixed case status | Active/ACTIVE alongside active | LOWER(TRIM(user_status)) |
| Users | Null account_id | 3 users unlinked from any account | Flag as orphaned; exclude from account metrics |
| Users | Null job_role | 10 users with no role | Default to 'unknown' |
| Users | Null last_seen_at | 15 users with no last seen date | Retain; treat as never seen for recency KPIs |
