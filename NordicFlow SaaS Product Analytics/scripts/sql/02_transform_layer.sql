/* ============================================================
   NORDICFLOW CRM — ANALYTICS PIPELINE
   FILE:    02_transform_layer.sql
   LAYER:   TRANSFORM_ (Cleaned & Enriched / Silver-Equivalent)
   PURPOSE: Views and materialised views built on top of STAGE_.
            Applies all data quality fixes: case normalisation,
            deduplication, null handling, canonical event name
            mapping, EUR currency conversion, derived flags,
            and surrogate key assignment.
            ANALYTICS_ (gold) reads exclusively from TRANSFORM_.
   AUTHOR:  Data & Analytics Team
   VERSION: 1.0  |  April 2026
   ============================================================ */

-- ============================================================
-- HOUSEKEEPING — DROP ORDER (reverse dependency)
-- ============================================================
/*
DROP MATERIALIZED VIEW TRANSFORM_PRODUCT_EVENTS;
DROP MATERIALIZED VIEW TRANSFORM_DEALS;
DROP MATERIALIZED VIEW TRANSFORM_USERS;
DROP MATERIALIZED VIEW TRANSFORM_ACCOUNTS;
DROP MATERIALIZED VIEW TRANSFORM_GEOGRAPHY;
DROP VIEW              TRANSFORM_EVENT_CANONICAL_MAP;
DROP TABLE             TRANSFORM_CURRENCY_RATES PURGE;
*/

-- ============================================================
-- 1. REFERENCE DATA: CURRENCY CONVERSION RATES
--    Static EUR rates — update quarterly
-- ============================================================
CREATE TABLE TRANSFORM_CURRENCY_RATES (
    CURRENCY_CODE       VARCHAR2(10)    NOT NULL,
    RATE_TO_EUR         NUMBER(12,6)    NOT NULL,
    EFFECTIVE_FROM      DATE            NOT NULL,
    EFFECTIVE_TO        DATE,
    NOTES               VARCHAR2(500),
    CONSTRAINT PK_CURRENCY_RATES PRIMARY KEY (CURRENCY_CODE, EFFECTIVE_FROM)
);

INSERT INTO TRANSFORM_CURRENCY_RATES VALUES ('EUR', 1.000000, DATE '2024-01-01', NULL, 'Euro base currency');
INSERT INTO TRANSFORM_CURRENCY_RATES VALUES ('GBP', 1.170000, DATE '2024-01-01', NULL, 'British Pound – Q1 2026 rate');
INSERT INTO TRANSFORM_CURRENCY_RATES VALUES ('DKK', 0.134000, DATE '2024-01-01', NULL, 'Danish Krone – Q1 2026 rate');
INSERT INTO TRANSFORM_CURRENCY_RATES VALUES ('NOK', 0.087000, DATE '2024-01-01', NULL, 'Norwegian Krone – Q1 2026 rate');
INSERT INTO TRANSFORM_CURRENCY_RATES VALUES ('SEK', 0.088000, DATE '2024-01-01', NULL, 'Swedish Krona – Q1 2026 rate');
COMMIT;

COMMENT ON TABLE TRANSFORM_CURRENCY_RATES IS 'Static EUR conversion rates. Only the row with EFFECTIVE_TO IS NULL is active. Update quarterly — insert new row with new EFFECTIVE_FROM and set prior row EFFECTIVE_TO.';

-- ============================================================
-- 2. REFERENCE VIEW: CANONICAL EVENT NAME MAPPING
--    Maps raw inconsistent event names from STAGE_ to a
--    single canonical form used throughout the pipeline.
-- ============================================================
CREATE OR REPLACE VIEW TRANSFORM_EVENT_CANONICAL_MAP AS
--
-- Each UNION ALL branch maps one raw variant to its canonical name.
-- To add new mappings: add another UNION ALL branch.
--
SELECT 'login'              AS RAW_EVENT_NAME, 'login'              AS CANONICAL_NAME, 'Authentication'      AS FEATURE_CATEGORY, 0 AS IS_CORE_EVENT FROM DUAL UNION ALL
SELECT 'Login',                                'login',                'Authentication',                        0                   FROM DUAL UNION ALL
SELECT 'user_login',                           'login',                'Authentication',                        0                   FROM DUAL UNION ALL
SELECT 'USER_LOGIN',                           'login',                'Authentication',                        0                   FROM DUAL UNION ALL
SELECT 'create_deal',                          'create_deal',          'Pipeline Management',                   1                   FROM DUAL UNION ALL
SELECT 'Create_Deal',                          'create_deal',          'Pipeline Management',                   1                   FROM DUAL UNION ALL
SELECT 'CREATE_DEAL',                          'create_deal',          'Pipeline Management',                   1                   FROM DUAL UNION ALL
SELECT 'log_activity',                         'log_activity',         'Activity Logging',                      1                   FROM DUAL UNION ALL
SELECT 'Log_Activity',                         'log_activity',         'Activity Logging',                      1                   FROM DUAL UNION ALL
SELECT 'LOG_ACTIVITY',                         'log_activity',         'Activity Logging',                      1                   FROM DUAL UNION ALL
SELECT 'move_deal_stage',                      'move_deal_stage',      'Pipeline Management',                   1                   FROM DUAL UNION ALL
SELECT 'Move_Deal_Stage',                      'move_deal_stage',      'Pipeline Management',                   1                   FROM DUAL UNION ALL
SELECT 'MOVE_DEAL_STAGE',                      'move_deal_stage',      'Pipeline Management',                   1                   FROM DUAL UNION ALL
SELECT 'enable_automation',                    'enable_automation',    'Workflow Automation',                   1                   FROM DUAL UNION ALL
SELECT 'Enable_Automation',                    'enable_automation',    'Workflow Automation',                   1                   FROM DUAL UNION ALL
SELECT 'ENABLE_AUTOMATION',                    'enable_automation',    'Workflow Automation',                   1                   FROM DUAL UNION ALL
SELECT 'view_dashboard',                       'view_dashboard',       'Reporting',                             0                   FROM DUAL UNION ALL
SELECT 'ViewDashboard',                        'view_dashboard',       'Reporting',                             0                   FROM DUAL UNION ALL
SELECT 'VIEW_DASHBOARD',                       'view_dashboard',       'Reporting',                             0                   FROM DUAL UNION ALL
SELECT 'invite_user',                          'invite_user',          'Collaboration',                         0                   FROM DUAL UNION ALL
SELECT 'Invite_User',                          'invite_user',          'Collaboration',                         0                   FROM DUAL UNION ALL
SELECT 'INVITE_USER',                          'invite_user',          'Collaboration',                         0                   FROM DUAL;

COMMENT ON TABLE TRANSFORM_EVENT_CANONICAL_MAP IS 'Reference view mapping all known raw event name variants to their canonical form, feature category, and core-event flag. Add new rows here when new event variants are discovered in STAGE_PRODUCT_EVENTS.';

-- ============================================================
-- 3. TRANSFORM_GEOGRAPHY
--    Deduplicates source, normalises country codes,
--    defaults NULL market values
-- ============================================================
CREATE MATERIALIZED VIEW TRANSFORM_GEOGRAPHY
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH DEDUPED AS (
    -- Assign row number to remove duplicate country codes.
    -- Geography.xlsx contains a duplicate FR row — keep first occurrence only.
    SELECT
        UPPER(TRIM(COUNTRY_CODE))   AS COUNTRY_CODE,
        TRIM(COUNTRY_NAME)          AS COUNTRY_NAME,
        TRIM(REGION)                AS REGION,
        -- UK market is NULL in source — default to 'UK & Ireland'
        COALESCE(TRIM(MARKET), 'UK & Ireland') AS MARKET,
        TRIM(CURRENCY)              AS CURRENCY,
        TRIM(SALES_REGION)          AS SALES_REGION,
        BATCH_ID,
        LOADED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(COUNTRY_CODE))
            ORDER BY LOADED_AT DESC
        ) AS RN
    FROM STAGE_GEOGRAPHY
    WHERE COUNTRY_CODE IS NOT NULL
)
SELECT
    COUNTRY_CODE,
    COUNTRY_NAME,
    REGION,
    MARKET,
    CURRENCY,
    SALES_REGION,
    BATCH_ID,
    LOADED_AT
FROM DEDUPED
WHERE RN = 1;

CREATE UNIQUE INDEX IDX_TGEO_CC   ON TRANSFORM_GEOGRAPHY (COUNTRY_CODE);
CREATE INDEX        IDX_TGEO_REG  ON TRANSFORM_GEOGRAPHY (REGION);

COMMENT ON TABLE TRANSFORM_GEOGRAPHY IS 'Deduplicated, normalised geography reference. One row per country code. Duplicate FR row from source removed. NULL UK market defaulted.';

-- ============================================================
-- 4. TRANSFORM_ACCOUNTS
--    Normalises status and channel, flags null trial dates
--    and missing industries, validates country code FK
-- ============================================================
CREATE MATERIALIZED VIEW TRANSFORM_ACCOUNTS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH LATEST_BATCH AS (
    -- Full-reload source — always take the latest batch only
    SELECT MAX(BATCH_ID) AS MAX_BATCH FROM STAGE_ACCOUNTS
),
NORMED AS (
    SELECT
        TRIM(sa.ACCOUNT_ID)                             AS ACCOUNT_ID,
        TRIM(sa.ACCOUNT_NAME)                           AS ACCOUNT_NAME,
        UPPER(TRIM(sa.COUNTRY_CODE))                    AS COUNTRY_CODE,
        TRIM(sa.CITY)                                   AS CITY,

        -- NULL industry defaulted to 'Unknown'
        COALESCE(TRIM(sa.INDUSTRY), 'Unknown')          AS INDUSTRY,

        TRIM(sa.EMPLOYEE_BAND)                          AS EMPLOYEE_BAND,
        LOWER(TRIM(sa.SEGMENT))                         AS SEGMENT,
        sa.CREATED_AT,
        sa.TRIAL_START_DATE,
        sa.TRIAL_END_DATE,

        -- Status normalised to lowercase
        LOWER(TRIM(sa.ACCOUNT_STATUS))                  AS ACCOUNT_STATUS,

        -- Acquisition channel — normalise spacing
        LOWER(TRIM(REPLACE(sa.ACQUISITION_CHANNEL, '-', '_'))) AS ACQUISITION_CHANNEL,

        sa.BATCH_ID,
        sa.LOADED_AT,

        -- ---- Derived quality flags ----
        CASE WHEN sa.TRIAL_START_DATE IS NULL THEN 1 ELSE 0 END AS IS_TRIAL_DATE_MISSING,
        CASE WHEN sa.TRIAL_END_DATE   IS NULL THEN 1 ELSE 0 END AS IS_TRIAL_END_MISSING,
        CASE WHEN sa.INDUSTRY         IS NULL THEN 1 ELSE 0 END AS IS_INDUSTRY_IMPUTED,

        -- Flag accounts whose country code is not in TRANSFORM_GEOGRAPHY
        CASE
            WHEN tg.COUNTRY_CODE IS NULL THEN 1 ELSE 0
        END AS IS_COUNTRY_UNMAPPED,

        -- Computed tenure in days from account creation to today
        ROUND(SYSDATE - sa.CREATED_AT)                 AS ACCOUNT_AGE_DAYS,

        -- Trial duration in days (NULL if dates missing)
        CASE
            WHEN sa.TRIAL_START_DATE IS NOT NULL
             AND sa.TRIAL_END_DATE   IS NOT NULL
            THEN ROUND(sa.TRIAL_END_DATE - sa.TRIAL_START_DATE)
            ELSE NULL
        END AS TRIAL_DURATION_DAYS

    FROM STAGE_ACCOUNTS sa
    JOIN LATEST_BATCH lb
        ON sa.BATCH_ID = lb.MAX_BATCH
    LEFT JOIN TRANSFORM_GEOGRAPHY tg
        ON UPPER(TRIM(sa.COUNTRY_CODE)) = tg.COUNTRY_CODE
)
SELECT * FROM NORMED;

CREATE UNIQUE INDEX IDX_TACC_ID     ON TRANSFORM_ACCOUNTS (ACCOUNT_ID);
CREATE INDEX        IDX_TACC_STATUS ON TRANSFORM_ACCOUNTS (ACCOUNT_STATUS);
CREATE INDEX        IDX_TACC_SEG    ON TRANSFORM_ACCOUNTS (SEGMENT);
CREATE INDEX        IDX_TACC_CHAN   ON TRANSFORM_ACCOUNTS (ACQUISITION_CHANNEL);
CREATE INDEX        IDX_TACC_CC     ON TRANSFORM_ACCOUNTS (COUNTRY_CODE);

COMMENT ON TABLE TRANSFORM_ACCOUNTS IS 'Cleaned and enriched account data. One row per account. Status and channel normalised. NULL industries imputed. Trial date flags added. Country FK validated.';

-- ============================================================
-- 5. TRANSFORM_USERS
--    Normalises status and role, flags orphaned users (no
--    account_id), computes days since last seen
-- ============================================================
CREATE MATERIALIZED VIEW TRANSFORM_USERS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH LATEST_BATCH AS (
    SELECT MAX(BATCH_ID) AS MAX_BATCH FROM STAGE_USERS
)
SELECT
    TRIM(su.USER_ID)                                AS USER_ID,
    TRIM(su.ACCOUNT_ID)                             AS ACCOUNT_ID,
    TRIM(su.FULL_NAME)                              AS FULL_NAME,
    LOWER(TRIM(su.EMAIL))                           AS EMAIL,

    -- NULL job_role defaulted to 'unknown'
    COALESCE(LOWER(TRIM(su.JOB_ROLE)), 'unknown')  AS JOB_ROLE,

    -- Status normalised to lowercase
    LOWER(TRIM(su.USER_STATUS))                     AS USER_STATUS,

    su.CREATED_AT,
    su.LAST_SEEN_AT,
    TRIM(su.TIMEZONE)                               AS TIMEZONE,
    LOWER(TRIM(su.LOCALE))                          AS LOCALE,
    su.IS_ADMIN,
    su.BATCH_ID,
    su.LOADED_AT,

    -- ---- Derived quality flags ----
    CASE WHEN su.ACCOUNT_ID   IS NULL THEN 1 ELSE 0 END AS IS_ORPHANED_USER,
    CASE WHEN su.JOB_ROLE     IS NULL THEN 1 ELSE 0 END AS IS_ROLE_IMPUTED,
    CASE WHEN su.LAST_SEEN_AT IS NULL THEN 1 ELSE 0 END AS IS_NEVER_SEEN,

    -- Recency: days since last seen (NULL = never seen)
    CASE
        WHEN su.LAST_SEEN_AT IS NOT NULL
        THEN ROUND(SYSDATE - su.LAST_SEEN_AT)
        ELSE NULL
    END AS DAYS_SINCE_LAST_SEEN,

    -- Recency band for dashboard slicing
    CASE
        WHEN su.LAST_SEEN_AT IS NULL          THEN 'Never'
        WHEN SYSDATE - su.LAST_SEEN_AT <= 7  THEN 'Active (0-7d)'
        WHEN SYSDATE - su.LAST_SEEN_AT <= 14 THEN 'Recent (8-14d)'
        WHEN SYSDATE - su.LAST_SEEN_AT <= 30 THEN 'Fading (15-30d)'
        ELSE 'Dormant (30d+)'
    END AS RECENCY_BAND,

    -- Account link validated against TRANSFORM_ACCOUNTS
    CASE
        WHEN su.ACCOUNT_ID IS NOT NULL
         AND ta.ACCOUNT_ID IS NULL THEN 1 ELSE 0
    END AS IS_ACCOUNT_UNMAPPED

FROM STAGE_USERS su
JOIN (SELECT MAX(BATCH_ID) AS MAX_BATCH FROM STAGE_USERS) lb
    ON su.BATCH_ID = lb.MAX_BATCH
LEFT JOIN TRANSFORM_ACCOUNTS ta
    ON TRIM(su.ACCOUNT_ID) = ta.ACCOUNT_ID;

CREATE UNIQUE INDEX IDX_TUSR_ID     ON TRANSFORM_USERS (USER_ID);
CREATE INDEX        IDX_TUSR_ACCID  ON TRANSFORM_USERS (ACCOUNT_ID);
CREATE INDEX        IDX_TUSR_STATUS ON TRANSFORM_USERS (USER_STATUS);
CREATE INDEX        IDX_TUSR_ROLE   ON TRANSFORM_USERS (JOB_ROLE);

COMMENT ON TABLE TRANSFORM_USERS IS 'Cleaned user data. One row per user. Status and role normalised. Orphaned users flagged. Days since last seen computed. Account FK validated.';

-- ============================================================
-- 6. TRANSFORM_DEALS
--    Normalises status, applies EUR currency conversion,
--    computes pipeline velocity, flags missing data
-- ============================================================
CREATE MATERIALIZED VIEW TRANSFORM_DEALS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH LATEST_BATCH AS (
    SELECT MAX(BATCH_ID) AS MAX_BATCH FROM STAGE_DEALS
),
-- Get active EUR rates (EFFECTIVE_TO IS NULL = current rate)
ACTIVE_RATES AS (
    SELECT CURRENCY_CODE, RATE_TO_EUR
    FROM TRANSFORM_CURRENCY_RATES
    WHERE EFFECTIVE_TO IS NULL
)
SELECT
    TRIM(sd.DEAL_ID)                                AS DEAL_ID,
    TRIM(sd.ACCOUNT_ID)                             AS ACCOUNT_ID,
    TRIM(sd.OWNER_USER_ID)                          AS OWNER_USER_ID,
    TRIM(sd.PIPELINE_ID)                            AS PIPELINE_ID,
    TRIM(sd.CURRENT_STAGE_ID)                       AS CURRENT_STAGE_ID,

    -- Status normalised to lowercase
    LOWER(TRIM(sd.STATUS))                          AS STATUS,

    sd.CREATED_AT,
    sd.CLOSED_AT,
    sd.LAST_STAGE_CHANGED_AT,
    sd.AMOUNT,
    UPPER(TRIM(sd.CURRENCY))                        AS CURRENCY,
    UPPER(TRIM(sd.COUNTRY_CODE))                    AS COUNTRY_CODE,
    LOWER(TRIM(sd.SOURCE_SYSTEM))                   AS SOURCE_SYSTEM,
    sd.BATCH_ID,
    sd.LOADED_AT,

    -- ---- EUR Conversion ----
    -- Multiply amount by currency rate; NULL if amount is missing
    CASE
        WHEN sd.AMOUNT IS NOT NULL AND ar.RATE_TO_EUR IS NOT NULL
        THEN ROUND(sd.AMOUNT * ar.RATE_TO_EUR, 2)
        ELSE NULL
    END AS AMOUNT_EUR,

    -- ---- Derived date fields ----
    TRUNC(sd.CREATED_AT)                            AS CREATED_DATE,
    TRUNC(sd.CLOSED_AT)                             AS CLOSED_DATE,
    TO_NUMBER(TO_CHAR(sd.CREATED_AT, 'YYYYMM'))    AS CREATED_YEAR_MONTH,
    TO_NUMBER(TO_CHAR(sd.CREATED_AT, 'YYYY'))      AS CREATED_YEAR,
    TO_CHAR(sd.CREATED_AT, 'YYYY-Q"Q"')            AS CREATED_QUARTER,

    -- ---- Pipeline Velocity (closed deals only) ----
    CASE
        WHEN sd.CLOSED_AT IS NOT NULL
        THEN ROUND(sd.CLOSED_AT - sd.CREATED_AT)
        ELSE NULL
    END AS DAYS_TO_CLOSE,

    -- Days since last stage change (open deal freshness)
    ROUND(SYSDATE - sd.LAST_STAGE_CHANGED_AT)      AS DAYS_SINCE_STAGE_CHANGE,

    -- ---- Derived flags ----
    CASE WHEN sd.AMOUNT           IS NULL THEN 1 ELSE 0 END AS IS_AMOUNT_MISSING,
    CASE WHEN sd.CURRENT_STAGE_ID IS NULL THEN 1 ELSE 0 END AS IS_STAGE_MISSING,
    CASE WHEN sd.CLOSED_AT        IS NOT NULL THEN 1 ELSE 0 END AS IS_CLOSED,

    -- Win/Loss flags for easy aggregation
    CASE WHEN LOWER(TRIM(sd.STATUS)) = 'won'  THEN 1 ELSE 0 END AS IS_WON,
    CASE WHEN LOWER(TRIM(sd.STATUS)) = 'lost' THEN 1 ELSE 0 END AS IS_LOST,
    CASE WHEN LOWER(TRIM(sd.STATUS)) = 'open' THEN 1 ELSE 0 END AS IS_OPEN,

    -- Account FK validation
    CASE
        WHEN ta.ACCOUNT_ID IS NULL THEN 1 ELSE 0
    END AS IS_ACCOUNT_UNMAPPED,

    -- Currency rate found
    CASE
        WHEN ar.RATE_TO_EUR IS NULL THEN 1 ELSE 0
    END AS IS_CURRENCY_UNMAPPED

FROM STAGE_DEALS sd
JOIN (SELECT MAX(BATCH_ID) AS MAX_BATCH FROM STAGE_DEALS) lb
    ON sd.BATCH_ID = lb.MAX_BATCH
LEFT JOIN ACTIVE_RATES ar
    ON UPPER(TRIM(sd.CURRENCY)) = ar.CURRENCY_CODE
LEFT JOIN TRANSFORM_ACCOUNTS ta
    ON TRIM(sd.ACCOUNT_ID) = ta.ACCOUNT_ID;

CREATE UNIQUE INDEX IDX_TDEAL_ID      ON TRANSFORM_DEALS (DEAL_ID);
CREATE INDEX        IDX_TDEAL_ACCID   ON TRANSFORM_DEALS (ACCOUNT_ID);
CREATE INDEX        IDX_TDEAL_STATUS  ON TRANSFORM_DEALS (STATUS);
CREATE INDEX        IDX_TDEAL_PIPE    ON TRANSFORM_DEALS (PIPELINE_ID);
CREATE INDEX        IDX_TDEAL_CDATE   ON TRANSFORM_DEALS (CREATED_DATE);
CREATE INDEX        IDX_TDEAL_CLDATE  ON TRANSFORM_DEALS (CLOSED_DATE);
CREATE INDEX        IDX_TDEAL_ISWON   ON TRANSFORM_DEALS (IS_WON);

COMMENT ON TABLE TRANSFORM_DEALS IS 'Cleaned deal data with EUR conversion, pipeline velocity, win/loss flags, and data quality indicators. One row per deal.';

-- ============================================================
-- 7. TRANSFORM_PRODUCT_EVENTS
--    Excludes test events, assigns surrogate key to null
--    event_ids, applies canonical event name mapping,
--    derives is_core_event and feature_category
-- ============================================================
CREATE MATERIALIZED VIEW TRANSFORM_PRODUCT_EVENTS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
WITH RAW_EVENTS AS (
    SELECT
        -- Surrogate key: use existing EVENT_ID if present, otherwise generate
        COALESCE(
            TRIM(spe.EVENT_ID),
            'SYS-' || TO_CHAR(ROWNUM, 'FM0000000')
        )                                               AS EVENT_ID,

        TRIM(spe.EVENT_NAME)                            AS EVENT_NAME_RAW,

        -- Canonical name from mapping view; fall back to LOWER(TRIM()) if unmapped
        COALESCE(
            ecm.CANONICAL_NAME,
            LOWER(TRIM(spe.EVENT_NAME))
        )                                               AS EVENT_NAME_CANONICAL,

        COALESCE(ecm.FEATURE_CATEGORY, 'Unmapped')     AS FEATURE_CATEGORY,
        COALESCE(ecm.IS_CORE_EVENT, 0)                 AS IS_CORE_EVENT,

        TRIM(spe.USER_ID)                               AS USER_ID,
        TRIM(spe.ACCOUNT_ID)                            AS ACCOUNT_ID,
        TRIM(spe.DEAL_ID)                               AS DEAL_ID,

        spe.EVENT_TIMESTAMP,
        spe.INGESTED_AT,

        -- EVENT_DATE: use source value if present, else derive from EVENT_TIMESTAMP
        COALESCE(
            TRUNC(spe.EVENT_DATE),
            TRUNC(spe.EVENT_TIMESTAMP)
        )                                               AS EVENT_DATE,

        -- ISO week for WAU calculations
        TO_CHAR(
            COALESCE(TRUNC(spe.EVENT_DATE), TRUNC(spe.EVENT_TIMESTAMP)),
            'IYYY-IW'
        )                                               AS ISO_WEEK,

        -- Month for monthly aggregations
        TO_NUMBER(TO_CHAR(
            COALESCE(TRUNC(spe.EVENT_DATE), TRUNC(spe.EVENT_TIMESTAMP)),
            'YYYYMM'
        ))                                              AS EVENT_YEAR_MONTH,

        LOWER(TRIM(spe.PLATFORM))                       AS PLATFORM,
        LOWER(TRIM(spe.DEVICE_TYPE))                    AS DEVICE_TYPE,
        TRIM(spe.APP_VERSION)                           AS APP_VERSION,
        UPPER(TRIM(spe.COUNTRY_CODE))                   AS COUNTRY_CODE,
        LOWER(TRIM(spe.SOURCE_SYSTEM))                  AS SOURCE_SYSTEM,
        spe.BATCH_ID,
        spe.LOADED_AT,

        -- ---- Derived flags ----
        CASE WHEN spe.USER_ID    IS NULL THEN 1 ELSE 0 END AS IS_ANONYMOUS_EVENT,
        CASE WHEN spe.ACCOUNT_ID IS NULL THEN 1 ELSE 0 END AS IS_ACCOUNT_MISSING,
        CASE WHEN spe.DEAL_ID    IS NULL THEN 1 ELSE 0 END AS IS_DEAL_UNLINKED,
        CASE WHEN ecm.CANONICAL_NAME IS NULL THEN 1 ELSE 0 END AS IS_EVENT_UNMAPPED,

        -- Authentication events for WAU exclusion
        CASE
            WHEN LOWER(TRIM(spe.EVENT_NAME)) IN ('login','user_login') THEN 1 ELSE 0
        END AS IS_AUTH_EVENT

    FROM STAGE_PRODUCT_EVENTS spe
    LEFT JOIN TRANSFORM_EVENT_CANONICAL_MAP ecm
        ON TRIM(spe.EVENT_NAME) = ecm.RAW_EVENT_NAME

    -- *** Exclude test events at this layer — they never appear in ANALYTICS_ ***
    WHERE spe.IS_TEST_EVENT = 0
       OR spe.IS_TEST_EVENT IS NULL
)
SELECT * FROM RAW_EVENTS;

CREATE INDEX IDX_TEVT_EVTID    ON TRANSFORM_PRODUCT_EVENTS (EVENT_ID);
CREATE INDEX IDX_TEVT_ACCID    ON TRANSFORM_PRODUCT_EVENTS (ACCOUNT_ID);
CREATE INDEX IDX_TEVT_USRID    ON TRANSFORM_PRODUCT_EVENTS (USER_ID);
CREATE INDEX IDX_TEVT_EVTDATE  ON TRANSFORM_PRODUCT_EVENTS (EVENT_DATE);
CREATE INDEX IDX_TEVT_ISOWEEK  ON TRANSFORM_PRODUCT_EVENTS (ISO_WEEK);
CREATE INDEX IDX_TEVT_CANON    ON TRANSFORM_PRODUCT_EVENTS (EVENT_NAME_CANONICAL);
CREATE INDEX IDX_TEVT_ISCORE   ON TRANSFORM_PRODUCT_EVENTS (IS_CORE_EVENT);
CREATE INDEX IDX_TEVT_ACCCORE  ON TRANSFORM_PRODUCT_EVENTS (ACCOUNT_ID, IS_CORE_EVENT, EVENT_DATE);

COMMENT ON TABLE TRANSFORM_PRODUCT_EVENTS IS 'Cleaned product events. Test events excluded. Surrogate keys assigned. Canonical event names applied. Core event flag derived. One row per product event.';

-- ============================================================
-- 8. REFRESH PROCEDURE
--    Refreshes all TRANSFORM_ materialised views in the
--    correct dependency order. Called by scheduler after
--    STAGE_ load completes.
-- ============================================================
CREATE OR REPLACE PROCEDURE REFRESH_TRANSFORM_LAYER
AS
    v_start     DATE := SYSDATE;
    v_step      VARCHAR2(100);
BEGIN
    v_step := 'TRANSFORM_GEOGRAPHY';
    DBMS_MVIEW.REFRESH('TRANSFORM_GEOGRAPHY',       method => 'C', atomic_refresh => FALSE);

    v_step := 'TRANSFORM_ACCOUNTS';
    DBMS_MVIEW.REFRESH('TRANSFORM_ACCOUNTS',        method => 'C', atomic_refresh => FALSE);

    v_step := 'TRANSFORM_USERS';
    DBMS_MVIEW.REFRESH('TRANSFORM_USERS',           method => 'C', atomic_refresh => FALSE);

    v_step := 'TRANSFORM_DEALS';
    DBMS_MVIEW.REFRESH('TRANSFORM_DEALS',           method => 'C', atomic_refresh => FALSE);

    v_step := 'TRANSFORM_PRODUCT_EVENTS';
    DBMS_MVIEW.REFRESH('TRANSFORM_PRODUCT_EVENTS',  method => 'C', atomic_refresh => FALSE);

    DBMS_OUTPUT.PUT_LINE('TRANSFORM_ refresh completed in '
        || ROUND((SYSDATE - v_start) * 86400) || ' seconds.');

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(
            -20001,
            'REFRESH_TRANSFORM_LAYER failed at step [' || v_step || ']: ' || SQLERRM
        );
END REFRESH_TRANSFORM_LAYER;
/
