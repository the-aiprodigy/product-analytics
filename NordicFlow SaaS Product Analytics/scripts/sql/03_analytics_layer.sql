/* ============================================================
   NORDICFLOW CRM — ANALYTICS PIPELINE
   FILE:    03_analytics_layer.sql
   LAYER:   ANALYTICS_ (Gold / Power BI Reporting Layer)
   PURPOSE: Star schema of dimension and fact tables built from
            TRANSFORM_ materialised views. Power BI connects
            exclusively to this layer. All joins are pre-built;
            Power BI measures use simple aggregations.
   AUTHOR:  Data & Analytics Team
   VERSION: 1.0  |  April 2026
   ============================================================ */

-- ============================================================
-- HOUSEKEEPING — DROP ORDER (reverse dependency)
-- ============================================================
/*
DROP TABLE ANALYTICS_FACT_ACCOUNT_ACTIVATION PURGE;
DROP TABLE ANALYTICS_FACT_PRODUCT_EVENTS     PURGE;
DROP TABLE ANALYTICS_FACT_DEALS              PURGE;
DROP TABLE ANALYTICS_DIM_EVENT_TYPE          PURGE;
DROP TABLE ANALYTICS_DIM_USER               PURGE;
DROP TABLE ANALYTICS_DIM_ACCOUNT            PURGE;
DROP TABLE ANALYTICS_DIM_GEOGRAPHY          PURGE;
DROP TABLE ANALYTICS_DIM_DATE               PURGE;
DROP SEQUENCE SEQ_DATE_SK;
*/

-- ============================================================
-- SECTION A: DIMENSION TABLES
-- ============================================================

-- ------------------------------------------------------------
-- A1. ANALYTICS_DIM_DATE
--     Calendar spine 2019-01-01 → 2030-12-31
--     Generated once; never refreshed unless range extended
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_DIM_DATE (
    DATE_SK         NUMBER(8)       NOT NULL,   -- YYYYMMDD surrogate key
    FULL_DATE       DATE            NOT NULL,
    DAY_OF_WEEK_NUM NUMBER(1)       NOT NULL,   -- 1=Mon … 7=Sun (ISO)
    DAY_NAME        VARCHAR2(10)    NOT NULL,
    DAY_OF_MONTH    NUMBER(2)       NOT NULL,
    WEEK_OF_YEAR    NUMBER(2)       NOT NULL,   -- ISO week
    ISO_WEEK_LABEL  VARCHAR2(10)    NOT NULL,   -- 'YYYY-WW' for slicers
    MONTH_NUMBER    NUMBER(2)       NOT NULL,
    MONTH_NAME      VARCHAR2(10)    NOT NULL,
    MONTH_LABEL     VARCHAR2(10)    NOT NULL,   -- 'YYYY-MM' for slicers
    QUARTER_NUMBER  NUMBER(1)       NOT NULL,
    QUARTER_LABEL   VARCHAR2(10)    NOT NULL,   -- 'YYYY-Q1' etc.
    YEAR_NUMBER     NUMBER(4)       NOT NULL,
    IS_WEEKEND      NUMBER(1)       NOT NULL,
    IS_WEEKDAY      NUMBER(1)       NOT NULL,
    FISCAL_YEAR     NUMBER(4)       NOT NULL,   -- Assumes Jan-Dec fiscal year
    FISCAL_QUARTER  NUMBER(1)       NOT NULL,
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_SK)
)
TABLESPACE USERS;

-- Generate calendar rows 2019-01-01 through 2030-12-31
INSERT INTO ANALYTICS_DIM_DATE
WITH DATES AS (
    SELECT DATE '2019-01-01' + (LEVEL - 1) AS DT
    FROM DUAL
    CONNECT BY LEVEL <= (DATE '2030-12-31' - DATE '2019-01-01' + 1)
)
SELECT
    TO_NUMBER(TO_CHAR(DT, 'YYYYMMDD'))              AS DATE_SK,
    DT                                              AS FULL_DATE,
    -- Oracle: 1=Sun in TO_CHAR; convert to ISO (1=Mon)
    MOD(TO_NUMBER(TO_CHAR(DT, 'D')) + 5, 7) + 1   AS DAY_OF_WEEK_NUM,
    TO_CHAR(DT, 'Day')                             AS DAY_NAME,
    TO_NUMBER(TO_CHAR(DT, 'DD'))                   AS DAY_OF_MONTH,
    TO_NUMBER(TO_CHAR(DT, 'IW'))                   AS WEEK_OF_YEAR,
    TO_CHAR(DT, 'IYYY') || '-' ||
        LPAD(TO_CHAR(DT,'IW'),2,'0')               AS ISO_WEEK_LABEL,
    TO_NUMBER(TO_CHAR(DT, 'MM'))                   AS MONTH_NUMBER,
    TO_CHAR(DT, 'Month')                           AS MONTH_NAME,
    TO_CHAR(DT, 'YYYY-MM')                         AS MONTH_LABEL,
    TO_NUMBER(TO_CHAR(DT, 'Q'))                    AS QUARTER_NUMBER,
    TO_CHAR(DT, 'YYYY') || '-Q' ||
        TO_CHAR(DT,'Q')                            AS QUARTER_LABEL,
    TO_NUMBER(TO_CHAR(DT, 'YYYY'))                 AS YEAR_NUMBER,
    CASE WHEN TO_CHAR(DT,'D') IN ('1','7')
         THEN 1 ELSE 0 END                         AS IS_WEEKEND,
    CASE WHEN TO_CHAR(DT,'D') NOT IN ('1','7')
         THEN 1 ELSE 0 END                         AS IS_WEEKDAY,
    TO_NUMBER(TO_CHAR(DT, 'YYYY'))                 AS FISCAL_YEAR,
    TO_NUMBER(TO_CHAR(DT, 'Q'))                    AS FISCAL_QUARTER
FROM DATES;

COMMIT;

CREATE INDEX IDX_ADIM_DATE_FULL    ON ANALYTICS_DIM_DATE (FULL_DATE);
CREATE INDEX IDX_ADIM_DATE_MONTH   ON ANALYTICS_DIM_DATE (MONTH_LABEL);
CREATE INDEX IDX_ADIM_DATE_ISOWEEK ON ANALYTICS_DIM_DATE (ISO_WEEK_LABEL);

COMMENT ON TABLE ANALYTICS_DIM_DATE IS 'Calendar dimension 2019–2030. Grain: one row per calendar day. Power BI connects FACT tables via DATE_SK (YYYYMMDD integer).';

-- ------------------------------------------------------------
-- A2. ANALYTICS_DIM_GEOGRAPHY
--     One row per country code with all regional attributes
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_DIM_GEOGRAPHY AS
SELECT
    COUNTRY_CODE,
    COUNTRY_NAME,
    REGION,
    MARKET,
    CURRENCY,
    SALES_REGION
FROM TRANSFORM_GEOGRAPHY;

-- Catch-all row for events/deals with unmapped country codes
INSERT INTO ANALYTICS_DIM_GEOGRAPHY VALUES (
    'ZZ', 'Unknown', 'Unknown', 'Unknown', 'EUR', 'Unknown'
);
COMMIT;

ALTER TABLE ANALYTICS_DIM_GEOGRAPHY
    ADD CONSTRAINT PK_DIM_GEO PRIMARY KEY (COUNTRY_CODE);

CREATE INDEX IDX_ADIM_GEO_REGION ON ANALYTICS_DIM_GEOGRAPHY (REGION);
CREATE INDEX IDX_ADIM_GEO_MKTRT  ON ANALYTICS_DIM_GEOGRAPHY (MARKET);
CREATE INDEX IDX_ADIM_GEO_SREG   ON ANALYTICS_DIM_GEOGRAPHY (SALES_REGION);

COMMENT ON TABLE ANALYTICS_DIM_GEOGRAPHY IS 'Geography dimension. One row per country code plus ZZ catch-all. Source: TRANSFORM_GEOGRAPHY.';

-- ------------------------------------------------------------
-- A3. ANALYTICS_DIM_ACCOUNT
--     One row per account — all descriptive attributes for
--     slicing across all fact tables
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_DIM_ACCOUNT AS
SELECT
    ta.ACCOUNT_ID,
    ta.ACCOUNT_NAME,

    -- Geography enrichment
    COALESCE(tg.COUNTRY_NAME,  'Unknown')       AS COUNTRY_NAME,
    COALESCE(ta.COUNTRY_CODE,  'ZZ')            AS COUNTRY_CODE,
    COALESCE(tg.REGION,        'Unknown')       AS REGION,
    COALESCE(tg.MARKET,        'Unknown')       AS MARKET,
    COALESCE(tg.SALES_REGION,  'Unknown')       AS SALES_REGION,

    ta.CITY,
    ta.INDUSTRY,
    ta.EMPLOYEE_BAND,
    ta.SEGMENT,
    ta.ACQUISITION_CHANNEL,
    ta.ACCOUNT_STATUS,
    ta.CREATED_AT                               AS ACCOUNT_CREATED_AT,

    -- Date SK for Power BI relationship
    TO_NUMBER(TO_CHAR(ta.CREATED_AT, 'YYYYMMDD')) AS ACCOUNT_CREATED_DATE_SK,

    ta.TRIAL_START_DATE,
    ta.TRIAL_END_DATE,
    ta.TRIAL_DURATION_DAYS,
    ta.ACCOUNT_AGE_DAYS,
    ta.IS_TRIAL_DATE_MISSING,
    ta.IS_INDUSTRY_IMPUTED,
    ta.IS_COUNTRY_UNMAPPED,

    -- ---- Derived Account Flags ----
    -- Is this a churned or cancelled account?
    CASE
        WHEN ta.ACCOUNT_STATUS IN ('churned','cancelled') THEN 1 ELSE 0
    END AS IS_CHURNED,

    -- Is trial still active (between trial_start and trial_end)?
    CASE
        WHEN ta.TRIAL_START_DATE <= SYSDATE
         AND ta.TRIAL_END_DATE   >= SYSDATE
        THEN 1 ELSE 0
    END AS IS_IN_TRIAL,

    -- Cohort month label for cohort retention analysis
    TO_CHAR(ta.CREATED_AT, 'YYYY-MM')           AS COHORT_MONTH,
    TO_NUMBER(TO_CHAR(ta.CREATED_AT, 'YYYY'))   AS COHORT_YEAR,
    TO_NUMBER(TO_CHAR(ta.CREATED_AT, 'Q'))      AS COHORT_QUARTER

FROM TRANSFORM_ACCOUNTS ta
LEFT JOIN TRANSFORM_GEOGRAPHY tg
    ON ta.COUNTRY_CODE = tg.COUNTRY_CODE;

ALTER TABLE ANALYTICS_DIM_ACCOUNT
    ADD CONSTRAINT PK_DIM_ACCOUNT PRIMARY KEY (ACCOUNT_ID);

CREATE INDEX IDX_ADIM_ACC_STATUS ON ANALYTICS_DIM_ACCOUNT (ACCOUNT_STATUS);
CREATE INDEX IDX_ADIM_ACC_SEG    ON ANALYTICS_DIM_ACCOUNT (SEGMENT);
CREATE INDEX IDX_ADIM_ACC_CHAN   ON ANALYTICS_DIM_ACCOUNT (ACQUISITION_CHANNEL);
CREATE INDEX IDX_ADIM_ACC_CC     ON ANALYTICS_DIM_ACCOUNT (COUNTRY_CODE);
CREATE INDEX IDX_ADIM_ACC_COHORT ON ANALYTICS_DIM_ACCOUNT (COHORT_MONTH);

COMMENT ON TABLE ANALYTICS_DIM_ACCOUNT IS 'Account dimension with geography enrichment, derived status flags, and cohort labels. One row per account. Source: TRANSFORM_ACCOUNTS + TRANSFORM_GEOGRAPHY.';

-- ------------------------------------------------------------
-- A4. ANALYTICS_DIM_USER
--     One row per user — role, status, admin flag, recency
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_DIM_USER AS
SELECT
    tu.USER_ID,
    tu.ACCOUNT_ID,
    tu.FULL_NAME,
    tu.EMAIL,
    tu.JOB_ROLE,
    tu.USER_STATUS,
    tu.IS_ADMIN,
    tu.TIMEZONE,
    tu.LOCALE,
    tu.CREATED_AT                               AS USER_CREATED_AT,
    tu.LAST_SEEN_AT,
    tu.DAYS_SINCE_LAST_SEEN,
    tu.RECENCY_BAND,
    tu.IS_ORPHANED_USER,
    tu.IS_NEVER_SEEN,
    tu.IS_ROLE_IMPUTED,

    -- Account segment join for user-level segment slicing
    da.SEGMENT                                  AS ACCOUNT_SEGMENT,
    da.ACQUISITION_CHANNEL                      AS ACCOUNT_ACQUISITION_CHANNEL,
    da.COUNTRY_CODE                             AS ACCOUNT_COUNTRY_CODE,
    da.REGION                                   AS ACCOUNT_REGION

FROM TRANSFORM_USERS tu
LEFT JOIN ANALYTICS_DIM_ACCOUNT da
    ON tu.ACCOUNT_ID = da.ACCOUNT_ID;

ALTER TABLE ANALYTICS_DIM_USER
    ADD CONSTRAINT PK_DIM_USER PRIMARY KEY (USER_ID);

CREATE INDEX IDX_ADIM_USR_ACCID  ON ANALYTICS_DIM_USER (ACCOUNT_ID);
CREATE INDEX IDX_ADIM_USR_STATUS ON ANALYTICS_DIM_USER (USER_STATUS);
CREATE INDEX IDX_ADIM_USR_ROLE   ON ANALYTICS_DIM_USER (JOB_ROLE);
CREATE INDEX IDX_ADIM_USR_ADMIN  ON ANALYTICS_DIM_USER (IS_ADMIN);

COMMENT ON TABLE ANALYTICS_DIM_USER IS 'User dimension. One row per user. Account segment and region joined in for cross-filtering in Power BI. Source: TRANSFORM_USERS.';

-- ------------------------------------------------------------
-- A5. ANALYTICS_DIM_EVENT_TYPE
--     Static canonical event reference — seeded once
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_DIM_EVENT_TYPE (
    EVENT_NAME_CANONICAL    VARCHAR2(100)   NOT NULL,
    FEATURE_CATEGORY        VARCHAR2(100)   NOT NULL,
    IS_CORE_EVENT           NUMBER(1)       NOT NULL,
    IS_AUTH_EVENT           NUMBER(1)       NOT NULL,
    DISPLAY_LABEL           VARCHAR2(150)   NOT NULL,
    SORT_ORDER              NUMBER(3)       NOT NULL,
    CONSTRAINT PK_DIM_EVENT_TYPE PRIMARY KEY (EVENT_NAME_CANONICAL)
);

INSERT ALL
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('login',           'Authentication',    0, 1, 'Login',             10)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('create_deal',     'Pipeline Mgmt',     1, 0, 'Create Deal',        20)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('log_activity',    'Activity Logging',  1, 0, 'Log Activity',       30)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('move_deal_stage', 'Pipeline Mgmt',     1, 0, 'Move Deal Stage',    40)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('enable_automation','Workflow Auto.',   1, 0, 'Enable Automation',  50)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('view_dashboard',  'Reporting',         0, 0, 'View Dashboard',     60)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('invite_user',     'Collaboration',     0, 0, 'Invite User',        70)
    INTO ANALYTICS_DIM_EVENT_TYPE VALUES ('unmapped',        'Unmapped',          0, 0, 'Unmapped Event',    999)
SELECT 1 FROM DUAL;
COMMIT;

COMMENT ON TABLE ANALYTICS_DIM_EVENT_TYPE IS 'Static canonical event type reference. Seeded once. Add new rows when new canonical event names are introduced. SORT_ORDER controls display order in Power BI visuals.';

-- ============================================================
-- SECTION B: FACT TABLES
-- ============================================================

-- ------------------------------------------------------------
-- B1. ANALYTICS_FACT_DEALS
--     Grain: one row per deal
--     Supports: Win rate, ADV, pipeline velocity, open pipeline
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_FACT_DEALS (
    -- Natural key
    DEAL_ID                 VARCHAR2(100)   NOT NULL,

    -- Foreign keys (Power BI relationships)
    ACCOUNT_ID              VARCHAR2(100)   NOT NULL,   -- → ANALYTICS_DIM_ACCOUNT
    OWNER_USER_ID           VARCHAR2(100),              -- → ANALYTICS_DIM_USER
    COUNTRY_CODE            VARCHAR2(10)    NOT NULL,   -- → ANALYTICS_DIM_GEOGRAPHY
    CREATED_DATE_SK         NUMBER(8)       NOT NULL,   -- → ANALYTICS_DIM_DATE
    CLOSED_DATE_SK          NUMBER(8),                  -- → ANALYTICS_DIM_DATE (nullable)

    -- Deal attributes
    PIPELINE_ID             VARCHAR2(100),
    CURRENT_STAGE_ID        VARCHAR2(100),
    STATUS                  VARCHAR2(20),
    SOURCE_SYSTEM           VARCHAR2(100),

    -- Measures (all numeric — Power BI aggregates directly)
    AMOUNT                  NUMBER(18,4),
    CURRENCY                VARCHAR2(10),
    AMOUNT_EUR              NUMBER(18,2),
    DAYS_TO_CLOSE           NUMBER(10),
    DAYS_SINCE_STAGE_CHANGE NUMBER(10),

    -- Pre-computed binary flags for fast measure calculation
    IS_WON                  NUMBER(1)       DEFAULT 0,
    IS_LOST                 NUMBER(1)       DEFAULT 0,
    IS_OPEN                 NUMBER(1)       DEFAULT 0,
    IS_CLOSED               NUMBER(1)       DEFAULT 0,
    IS_AMOUNT_MISSING       NUMBER(1)       DEFAULT 0,
    IS_STAGE_MISSING        NUMBER(1)       DEFAULT 0,

    -- Time attributes (denormalised for performance)
    CREATED_YEAR_MONTH      NUMBER(6),
    CREATED_YEAR            NUMBER(4),
    CREATED_QUARTER         VARCHAR2(10),

    -- Pipeline metadata
    LOADED_AT               DATE,
    CONSTRAINT PK_FACT_DEALS PRIMARY KEY (DEAL_ID)
)
TABLESPACE USERS;

-- Populate
INSERT INTO ANALYTICS_FACT_DEALS
SELECT
    td.DEAL_ID,
    COALESCE(td.ACCOUNT_ID, 'UNKNOWN'),
    td.OWNER_USER_ID,
    COALESCE(td.COUNTRY_CODE, 'ZZ'),
    TO_NUMBER(TO_CHAR(td.CREATED_AT, 'YYYYMMDD')),
    CASE
        WHEN td.CLOSED_AT IS NOT NULL
        THEN TO_NUMBER(TO_CHAR(td.CLOSED_AT, 'YYYYMMDD'))
        ELSE NULL
    END,
    td.PIPELINE_ID,
    td.CURRENT_STAGE_ID,
    td.STATUS,
    td.SOURCE_SYSTEM,
    td.AMOUNT,
    td.CURRENCY,
    td.AMOUNT_EUR,
    td.DAYS_TO_CLOSE,
    td.DAYS_SINCE_STAGE_CHANGE,
    td.IS_WON,
    td.IS_LOST,
    td.IS_OPEN,
    td.IS_CLOSED,
    td.IS_AMOUNT_MISSING,
    td.IS_STAGE_MISSING,
    td.CREATED_YEAR_MONTH,
    td.CREATED_YEAR,
    td.CREATED_QUARTER,
    SYSDATE
FROM TRANSFORM_DEALS td;

COMMIT;

-- Indexes
CREATE INDEX IDX_AFACT_DEAL_ACCID    ON ANALYTICS_FACT_DEALS (ACCOUNT_ID);
CREATE INDEX IDX_AFACT_DEAL_STATUS   ON ANALYTICS_FACT_DEALS (STATUS);
CREATE INDEX IDX_AFACT_DEAL_CDATE    ON ANALYTICS_FACT_DEALS (CREATED_DATE_SK);
CREATE INDEX IDX_AFACT_DEAL_CLDATE   ON ANALYTICS_FACT_DEALS (CLOSED_DATE_SK);
CREATE INDEX IDX_AFACT_DEAL_PIPE     ON ANALYTICS_FACT_DEALS (PIPELINE_ID);
CREATE INDEX IDX_AFACT_DEAL_CC       ON ANALYTICS_FACT_DEALS (COUNTRY_CODE);
CREATE INDEX IDX_AFACT_DEAL_ISWON    ON ANALYTICS_FACT_DEALS (IS_WON, IS_LOST, IS_OPEN);

COMMENT ON TABLE ANALYTICS_FACT_DEALS IS 'Deal fact table. Grain: one row per deal. Pre-computed win/loss flags for Power BI measure simplicity. EUR-converted amounts. All FK columns guaranteed non-null (catch-all ZZ/UNKNOWN).';

-- ------------------------------------------------------------
-- B2. ANALYTICS_FACT_PRODUCT_EVENTS
--     Grain: one row per product event (test events excluded)
--     Supports: WAU, feature adoption, activation funnel
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_FACT_PRODUCT_EVENTS (
    -- Natural key
    EVENT_ID                VARCHAR2(200)   NOT NULL,

    -- Foreign keys (Power BI relationships)
    ACCOUNT_ID              VARCHAR2(100),              -- → ANALYTICS_DIM_ACCOUNT (nullable — anonymous)
    USER_ID                 VARCHAR2(100),              -- → ANALYTICS_DIM_USER (nullable — anonymous)
    EVENT_NAME_CANONICAL    VARCHAR2(100),              -- → ANALYTICS_DIM_EVENT_TYPE
    EVENT_DATE_SK           NUMBER(8)       NOT NULL,   -- → ANALYTICS_DIM_DATE
    COUNTRY_CODE            VARCHAR2(10),               -- → ANALYTICS_DIM_GEOGRAPHY

    -- Event attributes
    EVENT_NAME_RAW          VARCHAR2(200),
    FEATURE_CATEGORY        VARCHAR2(100),
    DEAL_ID                 VARCHAR2(100),
    SOURCE_SYSTEM           VARCHAR2(100),
    PLATFORM                VARCHAR2(50),
    DEVICE_TYPE             VARCHAR2(50),
    APP_VERSION             VARCHAR2(50),
    ISO_WEEK                VARCHAR2(10),
    EVENT_YEAR_MONTH        NUMBER(6),

    -- Pre-computed binary flags
    IS_CORE_EVENT           NUMBER(1)       DEFAULT 0,
    IS_AUTH_EVENT           NUMBER(1)       DEFAULT 0,
    IS_ANONYMOUS_EVENT      NUMBER(1)       DEFAULT 0,
    IS_ACCOUNT_MISSING      NUMBER(1)       DEFAULT 0,
    IS_EVENT_UNMAPPED       NUMBER(1)       DEFAULT 0,

    -- Measure column (all events = 1 — summed for event count)
    EVENT_COUNT             NUMBER(1)       DEFAULT 1,

    -- Timestamps
    EVENT_TIMESTAMP         DATE,
    LOADED_AT               DATE,
    CONSTRAINT PK_FACT_EVENTS PRIMARY KEY (EVENT_ID)
)
TABLESPACE USERS;

-- Populate
INSERT INTO ANALYTICS_FACT_PRODUCT_EVENTS
SELECT
    tpe.EVENT_ID,
    tpe.ACCOUNT_ID,
    tpe.USER_ID,
    COALESCE(tpe.EVENT_NAME_CANONICAL, 'unmapped'),
    TO_NUMBER(TO_CHAR(tpe.EVENT_DATE, 'YYYYMMDD')),
    COALESCE(tpe.COUNTRY_CODE, 'ZZ'),
    tpe.EVENT_NAME_RAW,
    COALESCE(tpe.FEATURE_CATEGORY, 'Unmapped'),
    tpe.DEAL_ID,
    tpe.SOURCE_SYSTEM,
    tpe.PLATFORM,
    tpe.DEVICE_TYPE,
    tpe.APP_VERSION,
    tpe.ISO_WEEK,
    tpe.EVENT_YEAR_MONTH,
    tpe.IS_CORE_EVENT,
    tpe.IS_AUTH_EVENT,
    tpe.IS_ANONYMOUS_EVENT,
    tpe.IS_ACCOUNT_MISSING,
    tpe.IS_EVENT_UNMAPPED,
    1,          -- EVENT_COUNT always 1; SUM in Power BI gives total events
    tpe.EVENT_TIMESTAMP,
    SYSDATE
FROM TRANSFORM_PRODUCT_EVENTS tpe;

COMMIT;

-- Indexes — prioritise the access patterns Power BI uses most
CREATE INDEX IDX_AFACT_EVT_ACCID    ON ANALYTICS_FACT_PRODUCT_EVENTS (ACCOUNT_ID);
CREATE INDEX IDX_AFACT_EVT_USRID    ON ANALYTICS_FACT_PRODUCT_EVENTS (USER_ID);
CREATE INDEX IDX_AFACT_EVT_DATESK   ON ANALYTICS_FACT_PRODUCT_EVENTS (EVENT_DATE_SK);
CREATE INDEX IDX_AFACT_EVT_ISOWEEK  ON ANALYTICS_FACT_PRODUCT_EVENTS (ISO_WEEK);
CREATE INDEX IDX_AFACT_EVT_CANON    ON ANALYTICS_FACT_PRODUCT_EVENTS (EVENT_NAME_CANONICAL);
CREATE INDEX IDX_AFACT_EVT_ISCORE   ON ANALYTICS_FACT_PRODUCT_EVENTS (IS_CORE_EVENT);
CREATE INDEX IDX_AFACT_EVT_ACCCORE  ON ANALYTICS_FACT_PRODUCT_EVENTS (ACCOUNT_ID, IS_CORE_EVENT, EVENT_DATE_SK);
CREATE INDEX IDX_AFACT_EVT_CC       ON ANALYTICS_FACT_PRODUCT_EVENTS (COUNTRY_CODE);
CREATE INDEX IDX_AFACT_EVT_PLATF    ON ANALYTICS_FACT_PRODUCT_EVENTS (PLATFORM);
CREATE INDEX IDX_AFACT_EVT_YRMON    ON ANALYTICS_FACT_PRODUCT_EVENTS (EVENT_YEAR_MONTH);

COMMENT ON TABLE ANALYTICS_FACT_PRODUCT_EVENTS IS 'Product event fact table. Grain: one row per cleaned, non-test event. Pre-computed flags for activation, WAU, and feature adoption measures. EVENT_COUNT=1 on every row; SUM gives totals.';

-- ------------------------------------------------------------
-- B3. ANALYTICS_FACT_ACCOUNT_ACTIVATION
--     Grain: one row per account
--     Pre-aggregated activation milestones and engagement score
--     Avoids expensive full event-table scans at query time
-- ------------------------------------------------------------
CREATE TABLE ANALYTICS_FACT_ACCOUNT_ACTIVATION AS
WITH
-- ---- Step 1: First events per account ----
FIRST_EVENTS AS (
    SELECT
        ACCOUNT_ID,
        MIN(CASE WHEN EVENT_NAME_CANONICAL = 'login'            THEN EVENT_TIMESTAMP END) AS FIRST_LOGIN_AT,
        MIN(CASE WHEN IS_CORE_EVENT = 1                         THEN EVENT_TIMESTAMP END) AS FIRST_CORE_EVENT_AT,
        MIN(CASE WHEN IS_CORE_EVENT = 1                         THEN EVENT_NAME_CANONICAL END)
            KEEP (DENSE_RANK FIRST ORDER BY EVENT_TIMESTAMP)                              AS FIRST_CORE_EVENT_NAME,
        MAX(CASE WHEN IS_CORE_EVENT = 1                         THEN EVENT_TIMESTAMP END) AS LAST_CORE_EVENT_AT,
        MAX(EVENT_TIMESTAMP)                                                              AS LAST_ANY_EVENT_AT,
        COUNT(*)                                                                          AS TOTAL_EVENT_COUNT,

        -- Activation event flags (has the account ever fired each one?)
        MAX(CASE WHEN EVENT_NAME_CANONICAL = 'create_deal'      THEN 1 ELSE 0 END) AS HAS_CREATED_DEAL,
        MAX(CASE WHEN EVENT_NAME_CANONICAL = 'log_activity'     THEN 1 ELSE 0 END) AS HAS_LOGGED_ACTIVITY,
        MAX(CASE WHEN EVENT_NAME_CANONICAL = 'move_deal_stage'  THEN 1 ELSE 0 END) AS HAS_MOVED_STAGE,
        MAX(CASE WHEN EVENT_NAME_CANONICAL = 'enable_automation' THEN 1 ELSE 0 END) AS HAS_ENABLED_AUTOMATION,
        MAX(CASE WHEN EVENT_NAME_CANONICAL = 'view_dashboard'   THEN 1 ELSE 0 END) AS HAS_VIEWED_DASHBOARD,
        MAX(CASE WHEN EVENT_NAME_CANONICAL = 'invite_user'      THEN 1 ELSE 0 END) AS HAS_INVITED_USER,

        -- Feature breadth: distinct canonical event types (excluding login)
        COUNT(DISTINCT CASE WHEN EVENT_NAME_CANONICAL != 'login' THEN EVENT_NAME_CANONICAL END)
                                                                                          AS DISTINCT_FEATURES_USED

    FROM ANALYTICS_FACT_PRODUCT_EVENTS
    WHERE ACCOUNT_ID IS NOT NULL
    GROUP BY ACCOUNT_ID
),

-- ---- Step 2: Active users in rolling 30 days ----
ACTIVE_USERS_30D AS (
    SELECT
        ACCOUNT_ID,
        COUNT(DISTINCT USER_ID)  AS ACTIVE_USERS_30D
    FROM ANALYTICS_FACT_PRODUCT_EVENTS
    WHERE ACCOUNT_ID IS NOT NULL
      AND USER_ID    IS NOT NULL
      AND EVENT_DATE_SK >= TO_NUMBER(TO_CHAR(SYSDATE - 30, 'YYYYMMDD'))
    GROUP BY ACCOUNT_ID
),

-- ---- Step 3: Deal pipeline activity ----
DEAL_ACTIVITY AS (
    SELECT
        ACCOUNT_ID,
        COUNT(*)                                                   AS TOTAL_DEALS,
        SUM(IS_OPEN)                                              AS OPEN_DEALS,
        SUM(IS_WON)                                               AS WON_DEALS,
        SUM(AMOUNT_EUR)                                           AS TOTAL_PIPELINE_EUR,
        MAX(DAYS_SINCE_STAGE_CHANGE)                              AS MAX_DAYS_SINCE_STAGE_CHANGE
    FROM ANALYTICS_FACT_DEALS
    GROUP BY ACCOUNT_ID
),

-- ---- Step 4: Total provisioned users per account ----
USER_SEATS AS (
    SELECT
        ACCOUNT_ID,
        COUNT(*)                                                   AS TOTAL_USERS_PROVISIONED,
        SUM(CASE WHEN USER_STATUS = 'active' THEN 1 ELSE 0 END)  AS ACTIVE_USERS_TOTAL,
        SUM(IS_ADMIN)                                             AS ADMIN_USER_COUNT
    FROM ANALYTICS_DIM_USER
    WHERE IS_ORPHANED_USER = 0
    GROUP BY ACCOUNT_ID
),

-- ---- Step 5: Assemble activation record ----
ACTIVATION AS (
    SELECT
        da.ACCOUNT_ID,
        da.ACCOUNT_NAME,
        da.SEGMENT,
        da.ACQUISITION_CHANNEL,
        da.COUNTRY_CODE,
        da.REGION,
        da.ACCOUNT_STATUS,
        da.IS_CHURNED,
        da.TRIAL_START_DATE,
        da.TRIAL_END_DATE,
        da.IS_TRIAL_DATE_MISSING,
        da.COHORT_MONTH,
        da.ACCOUNT_CREATED_AT,

        fe.FIRST_LOGIN_AT,
        fe.FIRST_CORE_EVENT_AT,
        fe.FIRST_CORE_EVENT_NAME,
        fe.LAST_CORE_EVENT_AT,
        fe.LAST_ANY_EVENT_AT,
        fe.TOTAL_EVENT_COUNT,
        fe.HAS_CREATED_DEAL,
        fe.HAS_LOGGED_ACTIVITY,
        fe.HAS_MOVED_STAGE,
        fe.HAS_ENABLED_AUTOMATION,
        fe.HAS_VIEWED_DASHBOARD,
        fe.HAS_INVITED_USER,
        fe.DISTINCT_FEATURES_USED,

        COALESCE(au.ACTIVE_USERS_30D, 0)    AS ACTIVE_USERS_30D,

        de.TOTAL_DEALS,
        de.OPEN_DEALS,
        de.WON_DEALS,
        de.TOTAL_PIPELINE_EUR,

        us.TOTAL_USERS_PROVISIONED,
        us.ACTIVE_USERS_TOTAL,
        us.ADMIN_USER_COUNT,

        -- ---- Time-to-activate (hours) ----
        CASE
            WHEN da.TRIAL_START_DATE IS NOT NULL AND fe.FIRST_LOGIN_AT IS NOT NULL
            THEN ROUND((fe.FIRST_LOGIN_AT - da.TRIAL_START_DATE) * 24, 1)
            ELSE NULL
        END AS HOURS_TO_FIRST_LOGIN,

        CASE
            WHEN da.TRIAL_START_DATE IS NOT NULL AND fe.FIRST_CORE_EVENT_AT IS NOT NULL
            THEN ROUND((fe.FIRST_CORE_EVENT_AT - da.TRIAL_START_DATE) * 24, 1)
            ELSE NULL
        END AS HOURS_TO_FIRST_CORE_EVENT,

        -- ---- Activation flags (7d and 14d windows) ----
        CASE
            WHEN da.TRIAL_START_DATE IS NOT NULL
             AND fe.FIRST_CORE_EVENT_AT IS NOT NULL
             AND (fe.FIRST_CORE_EVENT_AT - da.TRIAL_START_DATE) <= 7
            THEN 1 ELSE 0
        END AS IS_ACTIVATED_7D,

        CASE
            WHEN da.TRIAL_START_DATE IS NOT NULL
             AND fe.FIRST_CORE_EVENT_AT IS NOT NULL
             AND (fe.FIRST_CORE_EVENT_AT - da.TRIAL_START_DATE) <= 14
            THEN 1 ELSE 0
        END AS IS_ACTIVATED_14D,

        -- ---- Days since last core event ----
        CASE
            WHEN fe.LAST_CORE_EVENT_AT IS NOT NULL
            THEN ROUND(SYSDATE - fe.LAST_CORE_EVENT_AT)
            ELSE NULL
        END AS DAYS_SINCE_LAST_CORE_EVENT,

        -- ---- Seat utilisation rate ----
        CASE
            WHEN us.TOTAL_USERS_PROVISIONED > 0
            THEN ROUND(COALESCE(au.ACTIVE_USERS_30D, 0) / us.TOTAL_USERS_PROVISIONED * 100, 1)
            ELSE NULL
        END AS SEAT_UTILISATION_RATE,

        -- ---- Composite Engagement Score (0–100) ----
        -- Component 1: Active users in 30d (max 25 pts; benchmarked against 5+ = full)
        LEAST(COALESCE(au.ACTIVE_USERS_30D, 0), 5) / 5.0 * 25 +
        -- Component 2: Feature breadth distinct event types (max 25 pts; 6+ = full)
        LEAST(COALESCE(fe.DISTINCT_FEATURES_USED, 0), 6) / 6.0 * 25 +
        -- Component 3: Deal pipeline activity (25 pts if ≥1 open deal)
        CASE WHEN COALESCE(de.OPEN_DEALS, 0) > 0 THEN 25 ELSE 0 END +
        -- Component 4: Login recency (25 pts if last event < 7d; 12 if 7-30d; 0 if 30d+)
        CASE
            WHEN fe.LAST_ANY_EVENT_AT IS NULL              THEN 0
            WHEN SYSDATE - fe.LAST_ANY_EVENT_AT <= 7      THEN 25
            WHEN SYSDATE - fe.LAST_ANY_EVENT_AT <= 30     THEN 12
            ELSE 0
        END                                                                     AS ENGAGEMENT_SCORE_RAW

    FROM ANALYTICS_DIM_ACCOUNT da
    LEFT JOIN FIRST_EVENTS       fe ON da.ACCOUNT_ID = fe.ACCOUNT_ID
    LEFT JOIN ACTIVE_USERS_30D   au ON da.ACCOUNT_ID = au.ACCOUNT_ID
    LEFT JOIN DEAL_ACTIVITY      de ON da.ACCOUNT_ID = de.ACCOUNT_ID
    LEFT JOIN USER_SEATS         us ON da.ACCOUNT_ID = us.ACCOUNT_ID
)
SELECT
    a.*,
    -- ---- Health band derived from engagement score ----
    CASE
        WHEN a.IS_CHURNED = 1                     THEN 'Churned'
        WHEN a.ENGAGEMENT_SCORE_RAW >= 70         THEN 'Green'
        WHEN a.ENGAGEMENT_SCORE_RAW >= 40         THEN 'Amber'
        ELSE                                           'Red'
    END AS HEALTH_BAND,

    -- ---- Days since last activity risk band ----
    CASE
        WHEN a.DAYS_SINCE_LAST_CORE_EVENT IS NULL   THEN 'No Activity'
        WHEN a.DAYS_SINCE_LAST_CORE_EVENT <= 13     THEN 'Active (<14d)'
        WHEN a.DAYS_SINCE_LAST_CORE_EVENT <= 30     THEN 'At Risk (14-30d)'
        ELSE                                              'Churning (30d+)'
    END AS ACTIVITY_RISK_BAND,

    -- ---- Activation funnel stage (highest completed step) ----
    CASE
        WHEN a.HAS_ENABLED_AUTOMATION = 1 THEN '5 — Full Adoption'
        WHEN a.HAS_MOVED_STAGE        = 1 THEN '4 — Pipeline Active'
        WHEN a.HAS_LOGGED_ACTIVITY    = 1 THEN '3 — Activity Logged'
        WHEN a.HAS_CREATED_DEAL       = 1 THEN '2 — Deal Created'
        WHEN a.FIRST_LOGIN_AT        IS NOT NULL THEN '1 — Logged In'
        ELSE                                          '0 — No Activity'
    END AS ACTIVATION_FUNNEL_STAGE,

    SYSDATE AS LOADED_AT

FROM ACTIVATION a;

-- Primary key and indexes
ALTER TABLE ANALYTICS_FACT_ACCOUNT_ACTIVATION
    ADD CONSTRAINT PK_FACT_ACTVN PRIMARY KEY (ACCOUNT_ID);

CREATE INDEX IDX_AFACT_ACTVN_STATUS   ON ANALYTICS_FACT_ACCOUNT_ACTIVATION (ACCOUNT_STATUS);
CREATE INDEX IDX_AFACT_ACTVN_SEG      ON ANALYTICS_FACT_ACCOUNT_ACTIVATION (SEGMENT);
CREATE INDEX IDX_AFACT_ACTVN_HEALTH   ON ANALYTICS_FACT_ACCOUNT_ACTIVATION (HEALTH_BAND);
CREATE INDEX IDX_AFACT_ACTVN_ACT7D    ON ANALYTICS_FACT_ACCOUNT_ACTIVATION (IS_ACTIVATED_7D);
CREATE INDEX IDX_AFACT_ACTVN_COHORT   ON ANALYTICS_FACT_ACCOUNT_ACTIVATION (COHORT_MONTH);
CREATE INDEX IDX_AFACT_ACTVN_FUNNEL   ON ANALYTICS_FACT_ACCOUNT_ACTIVATION (ACTIVATION_FUNNEL_STAGE);

COMMENT ON TABLE ANALYTICS_FACT_ACCOUNT_ACTIVATION IS 'Account-grain activation and health summary. One row per account. Pre-computes engagement score, health band, activation flags, seat utilisation, and funnel stage. Power BI Activation and Health dashboards read from this table.';

-- ============================================================
-- SECTION C: REFRESH STORED PROCEDURE
--    Truncates and reloads all ANALYTICS_ tables in order.
--    Called by scheduler after REFRESH_TRANSFORM_LAYER completes.
-- ============================================================
CREATE OR REPLACE PROCEDURE REFRESH_ANALYTICS_LAYER
AS
    v_step      VARCHAR2(100);
    v_start     DATE := SYSDATE;
BEGIN
    -- 1. Dimension tables (no dependencies on each other except DIM_ACCOUNT)
    v_step := 'ANALYTICS_DIM_GEOGRAPHY';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ANALYTICS_DIM_GEOGRAPHY';
    INSERT INTO ANALYTICS_DIM_GEOGRAPHY
    SELECT COUNTRY_CODE, COUNTRY_NAME, REGION, MARKET, CURRENCY, SALES_REGION
    FROM TRANSFORM_GEOGRAPHY;
    -- Re-insert catch-all
    INSERT INTO ANALYTICS_DIM_GEOGRAPHY VALUES ('ZZ','Unknown','Unknown','Unknown','EUR','Unknown');

    v_step := 'ANALYTICS_DIM_ACCOUNT';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ANALYTICS_DIM_ACCOUNT';
    INSERT INTO ANALYTICS_DIM_ACCOUNT
    SELECT
        ta.ACCOUNT_ID, ta.ACCOUNT_NAME,
        COALESCE(tg.COUNTRY_NAME,'Unknown'), COALESCE(ta.COUNTRY_CODE,'ZZ'),
        COALESCE(tg.REGION,'Unknown'), COALESCE(tg.MARKET,'Unknown'), COALESCE(tg.SALES_REGION,'Unknown'),
        ta.CITY, ta.INDUSTRY, ta.EMPLOYEE_BAND, ta.SEGMENT, ta.ACQUISITION_CHANNEL,
        ta.ACCOUNT_STATUS,
        ta.CREATED_AT, TO_NUMBER(TO_CHAR(ta.CREATED_AT,'YYYYMMDD')),
        ta.TRIAL_START_DATE, ta.TRIAL_END_DATE, ta.TRIAL_DURATION_DAYS,
        ta.ACCOUNT_AGE_DAYS, ta.IS_TRIAL_DATE_MISSING, ta.IS_INDUSTRY_IMPUTED, ta.IS_COUNTRY_UNMAPPED,
        CASE WHEN ta.ACCOUNT_STATUS IN ('churned','cancelled') THEN 1 ELSE 0 END,
        CASE WHEN ta.TRIAL_START_DATE <= SYSDATE AND ta.TRIAL_END_DATE >= SYSDATE THEN 1 ELSE 0 END,
        TO_CHAR(ta.CREATED_AT,'YYYY-MM'),
        TO_NUMBER(TO_CHAR(ta.CREATED_AT,'YYYY')),
        TO_NUMBER(TO_CHAR(ta.CREATED_AT,'Q'))
    FROM TRANSFORM_ACCOUNTS ta
    LEFT JOIN TRANSFORM_GEOGRAPHY tg ON ta.COUNTRY_CODE = tg.COUNTRY_CODE;

    v_step := 'ANALYTICS_DIM_USER';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ANALYTICS_DIM_USER';
    INSERT INTO ANALYTICS_DIM_USER
    SELECT
        tu.USER_ID, tu.ACCOUNT_ID, tu.FULL_NAME, tu.EMAIL, tu.JOB_ROLE,
        tu.USER_STATUS, tu.IS_ADMIN, tu.TIMEZONE, tu.LOCALE,
        tu.CREATED_AT, tu.LAST_SEEN_AT, tu.DAYS_SINCE_LAST_SEEN, tu.RECENCY_BAND,
        tu.IS_ORPHANED_USER, tu.IS_NEVER_SEEN, tu.IS_ROLE_IMPUTED,
        da.SEGMENT, da.ACQUISITION_CHANNEL, da.COUNTRY_CODE, da.REGION
    FROM TRANSFORM_USERS tu
    LEFT JOIN ANALYTICS_DIM_ACCOUNT da ON tu.ACCOUNT_ID = da.ACCOUNT_ID;

    -- 2. Fact tables
    v_step := 'ANALYTICS_FACT_DEALS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ANALYTICS_FACT_DEALS';
    INSERT INTO ANALYTICS_FACT_DEALS
    SELECT
        DEAL_ID, COALESCE(ACCOUNT_ID,'UNKNOWN'), OWNER_USER_ID,
        COALESCE(COUNTRY_CODE,'ZZ'),
        TO_NUMBER(TO_CHAR(CREATED_AT,'YYYYMMDD')),
        CASE WHEN CLOSED_AT IS NOT NULL THEN TO_NUMBER(TO_CHAR(CLOSED_AT,'YYYYMMDD')) ELSE NULL END,
        PIPELINE_ID, CURRENT_STAGE_ID, STATUS, SOURCE_SYSTEM,
        AMOUNT, CURRENCY, AMOUNT_EUR,
        DAYS_TO_CLOSE, DAYS_SINCE_STAGE_CHANGE,
        IS_WON, IS_LOST, IS_OPEN, IS_CLOSED,
        IS_AMOUNT_MISSING, IS_STAGE_MISSING,
        CREATED_YEAR_MONTH, CREATED_YEAR, CREATED_QUARTER,
        SYSDATE
    FROM TRANSFORM_DEALS;

    v_step := 'ANALYTICS_FACT_PRODUCT_EVENTS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ANALYTICS_FACT_PRODUCT_EVENTS';
    INSERT INTO ANALYTICS_FACT_PRODUCT_EVENTS
    SELECT
        EVENT_ID, ACCOUNT_ID, USER_ID,
        COALESCE(EVENT_NAME_CANONICAL,'unmapped'),
        TO_NUMBER(TO_CHAR(EVENT_DATE,'YYYYMMDD')),
        COALESCE(COUNTRY_CODE,'ZZ'),
        EVENT_NAME_RAW, COALESCE(FEATURE_CATEGORY,'Unmapped'),
        DEAL_ID, SOURCE_SYSTEM, PLATFORM, DEVICE_TYPE, APP_VERSION,
        ISO_WEEK, EVENT_YEAR_MONTH,
        IS_CORE_EVENT, IS_AUTH_EVENT, IS_ANONYMOUS_EVENT,
        IS_ACCOUNT_MISSING, IS_EVENT_UNMAPPED,
        1, EVENT_TIMESTAMP, SYSDATE
    FROM TRANSFORM_PRODUCT_EVENTS;

    -- 3. Activation fact — rebuild last (depends on DIM_ACCOUNT, FACT_DEALS, FACT_EVENTS)
    v_step := 'ANALYTICS_FACT_ACCOUNT_ACTIVATION';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ANALYTICS_FACT_ACCOUNT_ACTIVATION';

    -- Re-insert using the same CTE logic as the CREATE TABLE AS SELECT above
    -- (Wrapped into a procedure insert for operational refresh)
    INSERT INTO ANALYTICS_FACT_ACCOUNT_ACTIVATION
    WITH FIRST_EVENTS AS (
        SELECT ACCOUNT_ID,
            MIN(CASE WHEN EVENT_NAME_CANONICAL='login'             THEN EVENT_TIMESTAMP END) AS FIRST_LOGIN_AT,
            MIN(CASE WHEN IS_CORE_EVENT=1                          THEN EVENT_TIMESTAMP END) AS FIRST_CORE_EVENT_AT,
            MIN(CASE WHEN IS_CORE_EVENT=1                          THEN EVENT_NAME_CANONICAL END)
                KEEP (DENSE_RANK FIRST ORDER BY EVENT_TIMESTAMP)                             AS FIRST_CORE_EVENT_NAME,
            MAX(CASE WHEN IS_CORE_EVENT=1                          THEN EVENT_TIMESTAMP END) AS LAST_CORE_EVENT_AT,
            MAX(EVENT_TIMESTAMP)                                                             AS LAST_ANY_EVENT_AT,
            COUNT(*)                                                                         AS TOTAL_EVENT_COUNT,
            MAX(CASE WHEN EVENT_NAME_CANONICAL='create_deal'       THEN 1 ELSE 0 END) AS HAS_CREATED_DEAL,
            MAX(CASE WHEN EVENT_NAME_CANONICAL='log_activity'      THEN 1 ELSE 0 END) AS HAS_LOGGED_ACTIVITY,
            MAX(CASE WHEN EVENT_NAME_CANONICAL='move_deal_stage'   THEN 1 ELSE 0 END) AS HAS_MOVED_STAGE,
            MAX(CASE WHEN EVENT_NAME_CANONICAL='enable_automation' THEN 1 ELSE 0 END) AS HAS_ENABLED_AUTOMATION,
            MAX(CASE WHEN EVENT_NAME_CANONICAL='view_dashboard'    THEN 1 ELSE 0 END) AS HAS_VIEWED_DASHBOARD,
            MAX(CASE WHEN EVENT_NAME_CANONICAL='invite_user'       THEN 1 ELSE 0 END) AS HAS_INVITED_USER,
            COUNT(DISTINCT CASE WHEN EVENT_NAME_CANONICAL!='login' THEN EVENT_NAME_CANONICAL END) AS DISTINCT_FEATURES_USED
        FROM ANALYTICS_FACT_PRODUCT_EVENTS
        WHERE ACCOUNT_ID IS NOT NULL
        GROUP BY ACCOUNT_ID
    ),
    AU30 AS (
        SELECT ACCOUNT_ID, COUNT(DISTINCT USER_ID) AS ACTIVE_USERS_30D
        FROM ANALYTICS_FACT_PRODUCT_EVENTS
        WHERE ACCOUNT_ID IS NOT NULL AND USER_ID IS NOT NULL
          AND EVENT_DATE_SK >= TO_NUMBER(TO_CHAR(SYSDATE-30,'YYYYMMDD'))
        GROUP BY ACCOUNT_ID
    ),
    DA AS (
        SELECT ACCOUNT_ID,
            COUNT(*) AS TOTAL_DEALS, SUM(IS_OPEN) AS OPEN_DEALS, SUM(IS_WON) AS WON_DEALS,
            SUM(AMOUNT_EUR) AS TOTAL_PIPELINE_EUR,
            MAX(DAYS_SINCE_STAGE_CHANGE) AS MAX_DAYS_SINCE_STAGE_CHANGE
        FROM ANALYTICS_FACT_DEALS GROUP BY ACCOUNT_ID
    ),
    US AS (
        SELECT ACCOUNT_ID,
            COUNT(*) AS TOTAL_USERS_PROVISIONED,
            SUM(CASE WHEN USER_STATUS='active' THEN 1 ELSE 0 END) AS ACTIVE_USERS_TOTAL,
            SUM(IS_ADMIN) AS ADMIN_USER_COUNT
        FROM ANALYTICS_DIM_USER WHERE IS_ORPHANED_USER=0
        GROUP BY ACCOUNT_ID
    ),
    BASE AS (
        SELECT
            da2.ACCOUNT_ID, da2.ACCOUNT_NAME, da2.SEGMENT, da2.ACQUISITION_CHANNEL,
            da2.COUNTRY_CODE, da2.REGION, da2.ACCOUNT_STATUS, da2.IS_CHURNED,
            da2.TRIAL_START_DATE, da2.TRIAL_END_DATE, da2.IS_TRIAL_DATE_MISSING,
            da2.COHORT_MONTH, da2.ACCOUNT_CREATED_AT,
            fe.FIRST_LOGIN_AT, fe.FIRST_CORE_EVENT_AT, fe.FIRST_CORE_EVENT_NAME,
            fe.LAST_CORE_EVENT_AT, fe.LAST_ANY_EVENT_AT, fe.TOTAL_EVENT_COUNT,
            fe.HAS_CREATED_DEAL, fe.HAS_LOGGED_ACTIVITY, fe.HAS_MOVED_STAGE,
            fe.HAS_ENABLED_AUTOMATION, fe.HAS_VIEWED_DASHBOARD, fe.HAS_INVITED_USER,
            fe.DISTINCT_FEATURES_USED,
            COALESCE(au.ACTIVE_USERS_30D,0) AS ACTIVE_USERS_30D,
            de.TOTAL_DEALS, de.OPEN_DEALS, de.WON_DEALS, de.TOTAL_PIPELINE_EUR,
            us2.TOTAL_USERS_PROVISIONED, us2.ACTIVE_USERS_TOTAL, us2.ADMIN_USER_COUNT,
            CASE WHEN da2.TRIAL_START_DATE IS NOT NULL AND fe.FIRST_LOGIN_AT IS NOT NULL
                 THEN ROUND((fe.FIRST_LOGIN_AT-da2.TRIAL_START_DATE)*24,1) END AS HOURS_TO_FIRST_LOGIN,
            CASE WHEN da2.TRIAL_START_DATE IS NOT NULL AND fe.FIRST_CORE_EVENT_AT IS NOT NULL
                 THEN ROUND((fe.FIRST_CORE_EVENT_AT-da2.TRIAL_START_DATE)*24,1) END AS HOURS_TO_FIRST_CORE_EVENT,
            CASE WHEN da2.TRIAL_START_DATE IS NOT NULL AND fe.FIRST_CORE_EVENT_AT IS NOT NULL
                  AND (fe.FIRST_CORE_EVENT_AT-da2.TRIAL_START_DATE)<=7 THEN 1 ELSE 0 END AS IS_ACTIVATED_7D,
            CASE WHEN da2.TRIAL_START_DATE IS NOT NULL AND fe.FIRST_CORE_EVENT_AT IS NOT NULL
                  AND (fe.FIRST_CORE_EVENT_AT-da2.TRIAL_START_DATE)<=14 THEN 1 ELSE 0 END AS IS_ACTIVATED_14D,
            CASE WHEN fe.LAST_CORE_EVENT_AT IS NOT NULL
                 THEN ROUND(SYSDATE-fe.LAST_CORE_EVENT_AT) END AS DAYS_SINCE_LAST_CORE_EVENT,
            CASE WHEN us2.TOTAL_USERS_PROVISIONED>0
                 THEN ROUND(COALESCE(au.ACTIVE_USERS_30D,0)/us2.TOTAL_USERS_PROVISIONED*100,1) END AS SEAT_UTILISATION_RATE,
            LEAST(COALESCE(au.ACTIVE_USERS_30D,0),5)/5.0*25 +
            LEAST(COALESCE(fe.DISTINCT_FEATURES_USED,0),6)/6.0*25 +
            CASE WHEN COALESCE(de.OPEN_DEALS,0)>0 THEN 25 ELSE 0 END +
            CASE WHEN fe.LAST_ANY_EVENT_AT IS NULL THEN 0
                 WHEN SYSDATE-fe.LAST_ANY_EVENT_AT<=7 THEN 25
                 WHEN SYSDATE-fe.LAST_ANY_EVENT_AT<=30 THEN 12 ELSE 0 END AS ENGAGEMENT_SCORE_RAW
        FROM ANALYTICS_DIM_ACCOUNT da2
        LEFT JOIN FIRST_EVENTS fe ON da2.ACCOUNT_ID=fe.ACCOUNT_ID
        LEFT JOIN AU30          au ON da2.ACCOUNT_ID=au.ACCOUNT_ID
        LEFT JOIN DA            de ON da2.ACCOUNT_ID=de.ACCOUNT_ID
        LEFT JOIN US            us2 ON da2.ACCOUNT_ID=us2.ACCOUNT_ID
    )
    SELECT
        b.*,
        CASE WHEN b.IS_CHURNED=1 THEN 'Churned'
             WHEN b.ENGAGEMENT_SCORE_RAW>=70 THEN 'Green'
             WHEN b.ENGAGEMENT_SCORE_RAW>=40 THEN 'Amber'
             ELSE 'Red' END AS HEALTH_BAND,
        CASE WHEN b.DAYS_SINCE_LAST_CORE_EVENT IS NULL THEN 'No Activity'
             WHEN b.DAYS_SINCE_LAST_CORE_EVENT<=13 THEN 'Active (<14d)'
             WHEN b.DAYS_SINCE_LAST_CORE_EVENT<=30 THEN 'At Risk (14-30d)'
             ELSE 'Churning (30d+)' END AS ACTIVITY_RISK_BAND,
        CASE WHEN b.HAS_ENABLED_AUTOMATION=1 THEN '5 — Full Adoption'
             WHEN b.HAS_MOVED_STAGE=1        THEN '4 — Pipeline Active'
             WHEN b.HAS_LOGGED_ACTIVITY=1    THEN '3 — Activity Logged'
             WHEN b.HAS_CREATED_DEAL=1       THEN '2 — Deal Created'
             WHEN b.FIRST_LOGIN_AT IS NOT NULL THEN '1 — Logged In'
             ELSE '0 — No Activity' END AS ACTIVATION_FUNNEL_STAGE,
        SYSDATE AS LOADED_AT
    FROM BASE b;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('ANALYTICS_ refresh completed in '
        || ROUND((SYSDATE - v_start) * 86400) || ' seconds.');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(
            -20002,
            'REFRESH_ANALYTICS_LAYER failed at step [' || v_step || ']: ' || SQLERRM
        );
END REFRESH_ANALYTICS_LAYER;
/

-- ============================================================
-- SECTION D: MASTER PIPELINE ORCHESTRATION PROCEDURE
--    Call this single procedure from the scheduler.
--    It refreshes TRANSFORM_ then ANALYTICS_ in sequence.
-- ============================================================
CREATE OR REPLACE PROCEDURE RUN_NORDICFLOW_PIPELINE
AS
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== NordicFlow Pipeline Started: ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') || ' ===');

    DBMS_OUTPUT.PUT_LINE('[1/2] Refreshing TRANSFORM_ layer...');
    REFRESH_TRANSFORM_LAYER;

    DBMS_OUTPUT.PUT_LINE('[2/2] Refreshing ANALYTICS_ layer...');
    REFRESH_ANALYTICS_LAYER;

    DBMS_OUTPUT.PUT_LINE('=== NordicFlow Pipeline Completed: ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') || ' ===');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(
            -20003,
            'RUN_NORDICFLOW_PIPELINE failed: ' || SQLERRM
        );
END RUN_NORDICFLOW_PIPELINE;
/

-- ============================================================
-- SECTION E: SCHEDULER JOB SETUP
--    Runs daily at 03:00 UTC — after source file delivery
-- ============================================================
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'JOB_NORDICFLOW_PIPELINE',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'RUN_NORDICFLOW_PIPELINE',
        start_date      => SYSTIMESTAMP AT TIME ZONE 'UTC',
        repeat_interval => 'FREQ=DAILY; BYHOUR=3; BYMINUTE=0; BYSECOND=0',
        enabled         => TRUE,
        comments        => 'Daily refresh of NordicFlow TRANSFORM_ and ANALYTICS_ layers. Runs at 03:00 UTC after STAGE_ source file loads complete.'
    );
END;
/
