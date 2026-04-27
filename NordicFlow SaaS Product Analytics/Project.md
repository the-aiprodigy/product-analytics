# NordicFlow CRM – Project Understanding & Business Context

## 1. Company & Market Position
**NordicFlow CRM** is a European B2B SaaS company headquartered in Copenhagen, founded in 2019.  
It targets **SMBs (20–300 employees)** across industries like SaaS, professional services, logistics, and manufacturing, with distributed sales teams commonly operating in multiple European countries.

The CRM market is mature and fiercely competitive. NordicFlow does not try to win on feature parity. Instead, it positions itself as a **GDPR‑first, workflow‑driven** alternative to US‑based platforms. This privacy‑centric value proposition resonates with European customers but also raises expectations: the product must deliver demonstrable, measurable value to justify its selection over familiar incumbents.

## 2. Product & Value Creation
The product is a browser‑based CRM structured around the daily workflow of sales teams:
- **Core capabilities:** lead management, pipeline configuration, deal tracking, activity logging, task management, workspace collaboration, and configurable workflow automation.
- **Design philosophy:** Flexible enough to adapt to different sales processes, yet opinionated enough to guide users toward best practices through structure and automation.

A fundamental distinction exists between **activity** and **value**:
- *Activity:* Logging in, browsing dashboards, or isolated clicks are not inherently valuable.
- *Value:* Only when teams consistently manage their sales pipeline, track deals, log meaningful interactions, and automate workflows does the product deliver real business impact.

This distinction is not academic. Confusing surface activity with genuine engagement leads to inflated metrics, misdiagnosed churn, and poor product decisions. It must be encoded directly into measurement logic.

## 3. Users & Usage Reality
Three primary user groups interact with the product differently:
- **Sales Representatives** – frequent daily users who create deals, log activities, move deals through stages.
- **Sales Managers** – monitor pipeline health, review reports, and manage team performance.
- **Operations/RevOps** – configure pipelines, set automation rules, and maintain the CRM structure.

Adoption maturity varies widely. Some teams go deep, while others remain on the surface. Smaller companies are particularly vulnerable: they have low tolerance for onboarding friction and need to experience value quickly, or they churn. This makes **early behaviour** a critical predictor of long‑term success.

## 4. Internal Organizational Landscape
NordicFlow has transitioned from startup to scale‑up. Functional teams (Product, Growth & Marketing, Sales, Customer Success, Executive Leadership) all rely on product data—but often with conflicting definitions.  
Without shared metrics and aligned interpretations, the same event stream can produce contradictory narratives. Leadership cannot confidently prioritise initiatives or measure the impact of product changes.

## 5. Business Challenges Driving the Analytics Initiative
Four interlinked problems prompted this analytics investment:
1. **Inconsistent activation** – many trial sign‑ups never reach the point where the CRM becomes embedded in the team’s workflow.
2. **Surface‑level usage mistaken for engagement** – logins and page views are treated as success signals, masking low adoption of value‑driving features.
3. **Early‑stage churn** – particularly among smaller accounts, driven by failure to realise value quickly, not a lack of features.
4. **Fragmented analytics landscape** – different dashboards and reports tell conflicting stories, preventing aligned decision‑making.

## 6. Strategic Ambitions
- **Short term:** Improve onboarding, increase activation consistency, reduce early churn, and build confidence in cross‑team metrics.
- **Medium term:** Expand into new European markets, strengthen product‑led growth, introduce advanced automation, and evolve Customer Success into a proactive, data‑driven function.
- **Long term:** Become the leading European CRM platform, competing credibly with global incumbents. Product analytics is seen as a strategic capability to enable focus, scale, and sustained advantage.

## 7. Role of Analytics & the Data Professional
Analytics is positioned as a **decision‑support function**, not merely a reporting factory. Our responsibility is to:
- Create clarity where understanding is fragmented.
- Define trusted, shared metrics.
- Make assumptions explicit, especially the activity‑vs‑value distinction.
- Ensure that analytical outputs can be interpreted consistently by Product, Growth, Sales, and Customer Success.

We do not own product strategy or feature design. We own the evidence and reasoning that underpin good decisions.

## 8. Implications for Data Modeling & Metrics
Every downstream choice in the pipeline is shaped by this understanding:

| Business Insight | Data Modeling Consequence |
|------------------|----------------------------|
| Activity ≠ Value | Events must be classified into *surface* and *core value* categories. Gold‑layer fact tables must make this distinction computable. |
| Activation is team‑based, not just user‑based | The account (workspace) is the unit of analysis for activation and churn. Metrics must aggregate user behaviour to the account level. |
| Early behaviour predicts churn | Time‑windowed metrics (first 7 days, first 14 days) are essential. Cohort‑based analysis is required. |
| Different teams need consistent definitions | Core definitions (e.g., “value‑active user”, “activated account”) must be centralised in the gold layer and reused across dashboards. |
| Source data may be noisy | A Bronze‑Silver‑Gold architecture preserves raw data, allows auditable transformations, and ensures reproducibility. |

## 9. From Understanding to Action
The data pipeline (see `README.md`) operationalises this understanding:
- **Bronze** retains raw events and user metadata exactly as sourced.
- **Silver** cleans, deduplicates, and joins the data, while preserving the `event_type` column that is later classified.
- **Gold** produces:
  - `dim_accounts`: account‑level dimension with plan and user counts.
  - `fact_product_usage`: daily user‑level usage, clearly separating `core_value_events` from `surface_events`.
  - (optional) `fact_weekly_account_usage`: aggregated account‑level metrics used directly for activation and churn analysis.

Every KPI (activation rate, value‑user ratio, churn risk) can now be calculated from a single source of truth, closing the gap between data and decision‑making that prompted this initiative.