/* ============================================================
   NORDICFLOW CRM — ANALYTICS PIPELINE
   FILE:    01_stage_layer.sql
   LAYER:   STAGE_ (Typed Ingestion / Bronze-Equivalent)
   PURPOSE: Typed copies of all source extracts. These tables
            receive data from SOURCE_ flat file loads or ETL
            tool inserts. Every column is typed but no business
            logic is applied here. TRANSFORM_ reads from this
            layer exclusively.
   AUTHOR:  Data & Analytics Team
   VERSION: 1.0  |  April 2026
   ============================================================ */

-- ============================================================
-- HOUSEKEEPING
-- ============================================================
-- Drop tables in reverse dependency order if rebuilding
-- Only run manually during a full reset; never in scheduled ETL

/*
DROP TABLE STAGE_PRODUCT_EVENTS  PURGE;
DROP TABLE STAGE_DEALS           PURGE;
DROP TABLE STAGE_USERS           PURGE;
DROP TABLE STAGE_ACCOUNTS        PURGE;
DROP TABLE STAGE_GEOGRAPHY       PURGE;
DROP SEQUENCE SEQ_BATCH_ID;
*/

-- ============================================================
-- BATCH CONTROL SEQUENCE
-- Used to tag every row loaded in the same ETL run
-- ============================================================
CREATE SEQUENCE SEQ_BATCH_ID
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- ============================================================
-- STAGE_GEOGRAPHY
-- Source: Geography.xlsx  |  ~8 rows  |  On-change load
-- ============================================================
CREATE TABLE STAGE_GEOGRAPHY (
    -- Source columns ----------------------------------------
    COUNTRY_CODE        VARCHAR2(20),
    COUNTRY_NAME        VARCHAR2(150),
    REGION              VARCHAR2(150),
    MARKET              VARCHAR2(150),
    CURRENCY            VARCHAR2(20),
    SALES_REGION        VARCHAR2(150),

    -- Pipeline metadata -------------------------------------
    BATCH_ID            NUMBER(10)      NOT NULL,
    LOADED_AT           DATE            DEFAULT SYSDATE NOT NULL,
    SOURCE_FILE         VARCHAR2(255)   DEFAULT 'Geography.xlsx'
)
TABLESPACE USERS
PCTFREE 10
NOLOGGING;

COMMENT ON TABLE  STAGE_GEOGRAPHY              IS 'Typed ingestion of Geography.xlsx. One row per source row including duplicates. TRANSFORM_ deduplicates.';
COMMENT ON COLUMN STAGE_GEOGRAPHY.BATCH_ID     IS 'SEQ_BATCH_ID value for the ETL run that inserted this row.';
COMMENT ON COLUMN STAGE_GEOGRAPHY.LOADED_AT    IS 'Wall-clock timestamp at row insert — set by Oracle DEFAULT SYSDATE.';

-- ============================================================
-- STAGE_ACCOUNTS
-- Source: Accounts.csv  |  ~120 rows  |  Daily full-reload
-- ============================================================
CREATE TABLE STAGE_ACCOUNTS (
    -- Source columns ----------------------------------------
    ACCOUNT_ID              VARCHAR2(100),
    ACCOUNT_NAME            VARCHAR2(500),
    COUNTRY_CODE            VARCHAR2(20),
    CITY                    VARCHAR2(150),
    INDUSTRY                VARCHAR2(150),
    EMPLOYEE_BAND           VARCHAR2(50),
    SEGMENT                 VARCHAR2(50),
    CREATED_AT              DATE,
    TRIAL_START_DATE        DATE,
    TRIAL_END_DATE          DATE,
    ACCOUNT_STATUS          VARCHAR2(50),
    ACQUISITION_CHANNEL     VARCHAR2(100),

    -- Pipeline metadata -------------------------------------
    BATCH_ID                NUMBER(10)      NOT NULL,
    LOADED_AT               DATE            DEFAULT SYSDATE NOT NULL,
    SOURCE_FILE             VARCHAR2(255)   DEFAULT 'Accounts.csv'
)
TABLESPACE USERS
PCTFREE 10
NOLOGGING;

COMMENT ON TABLE  STAGE_ACCOUNTS                     IS 'Typed ingestion of Accounts.csv. Full-reload daily. No deduplication at this layer.';
COMMENT ON COLUMN STAGE_ACCOUNTS.ACCOUNT_STATUS      IS 'Raw status value from source — may be mixed case. TRANSFORM_ normalises.';
COMMENT ON COLUMN STAGE_ACCOUNTS.TRIAL_START_DATE    IS '27 rows expected to be NULL in source data — retained as-is at this layer.';

-- ============================================================
-- STAGE_USERS
-- Source: Users.csv  |  ~400 rows  |  Daily full-reload
-- ============================================================
CREATE TABLE STAGE_USERS (
    -- Source columns ----------------------------------------
    USER_ID             VARCHAR2(100),
    ACCOUNT_ID          VARCHAR2(100),
    FULL_NAME           VARCHAR2(500),
    EMAIL               VARCHAR2(500),
    JOB_ROLE            VARCHAR2(100),
    USER_STATUS         VARCHAR2(50),
    CREATED_AT          DATE,
    LAST_SEEN_AT        DATE,
    TIMEZONE            VARCHAR2(100),
    LOCALE              VARCHAR2(20),
    IS_ADMIN            NUMBER(1),

    -- Pipeline metadata -------------------------------------
    BATCH_ID            NUMBER(10)      NOT NULL,
    LOADED_AT           DATE            DEFAULT SYSDATE NOT NULL,
    SOURCE_FILE         VARCHAR2(255)   DEFAULT 'Users.csv'
)
TABLESPACE USERS
PCTFREE 10
NOLOGGING;

COMMENT ON TABLE  STAGE_USERS               IS 'Typed ingestion of Users.csv. Full-reload daily.';
COMMENT ON COLUMN STAGE_USERS.IS_ADMIN      IS 'Boolean flag from source — expected values 0 or 1.';
COMMENT ON COLUMN STAGE_USERS.LAST_SEEN_AT  IS '15 rows expected to be NULL — user has never been seen in product.';

-- ============================================================
-- STAGE_DEALS
-- Source: Deals.csv  |  ~6,443 rows  |  Daily full-reload
-- ============================================================
CREATE TABLE STAGE_DEALS (
    -- Source columns ----------------------------------------
    DEAL_ID                 VARCHAR2(100),
    ACCOUNT_ID              VARCHAR2(100),
    OWNER_USER_ID           VARCHAR2(100),
    PIPELINE_ID             VARCHAR2(100),
    CURRENT_STAGE_ID        VARCHAR2(100),
    STATUS                  VARCHAR2(50),
    CREATED_AT              DATE,
    CLOSED_AT               DATE,
    LAST_STAGE_CHANGED_AT   DATE,
    AMOUNT                  NUMBER(18,4),
    CURRENCY                VARCHAR2(20),
    COUNTRY_CODE            VARCHAR2(20),
    SOURCE_SYSTEM           VARCHAR2(100),

    -- Pipeline metadata -------------------------------------
    BATCH_ID                NUMBER(10)      NOT NULL,
    LOADED_AT               DATE            DEFAULT SYSDATE NOT NULL,
    SOURCE_FILE             VARCHAR2(255)   DEFAULT 'Deals.csv'
)
TABLESPACE USERS
PCTFREE 10
NOLOGGING;

COMMENT ON TABLE  STAGE_DEALS                    IS 'Typed ingestion of Deals.csv. Full-reload daily.';
COMMENT ON COLUMN STAGE_DEALS.STATUS             IS 'Raw status — mixed case in source. Values include open/OPEN/Open, won/WON/Won, lost/LOST/Lost.';
COMMENT ON COLUMN STAGE_DEALS.AMOUNT             IS '227 rows expected to be NULL in source. Retained at this layer; flagged in TRANSFORM_.';
COMMENT ON COLUMN STAGE_DEALS.CLOSED_AT          IS 'NULL for all open deals — expected and correct.';
COMMENT ON COLUMN STAGE_DEALS.CURRENT_STAGE_ID   IS '112 rows expected to be NULL — flagged in TRANSFORM_.';

-- ============================================================
-- STAGE_PRODUCT_EVENTS
-- Source: ProductEvents.csv  |  ~61,747 rows  |  Daily append
-- ============================================================
CREATE TABLE STAGE_PRODUCT_EVENTS (
    -- Source columns ----------------------------------------
    EVENT_ID            VARCHAR2(200),
    EVENT_NAME          VARCHAR2(200),
    USER_ID             VARCHAR2(100),
    ACCOUNT_ID          VARCHAR2(100),
    DEAL_ID             VARCHAR2(100),
    EVENT_TIMESTAMP     DATE,
    INGESTED_AT         DATE,
    EVENT_DATE          DATE,
    PLATFORM            VARCHAR2(50),
    DEVICE_TYPE         VARCHAR2(50),
    APP_VERSION         VARCHAR2(50),
    COUNTRY_CODE        VARCHAR2(20),
    IS_TEST_EVENT       NUMBER(1),
    SOURCE_SYSTEM       VARCHAR2(100),

    -- Pipeline metadata -------------------------------------
    BATCH_ID            NUMBER(10)      NOT NULL,
    LOADED_AT           DATE            DEFAULT SYSDATE NOT NULL,
    SOURCE_FILE         VARCHAR2(255)   DEFAULT 'ProductEvents.csv'
)
TABLESPACE USERS
PCTFREE 10
NOLOGGING;

COMMENT ON TABLE  STAGE_PRODUCT_EVENTS                 IS 'Typed ingestion of ProductEvents.csv. Append-only daily load. Test events retained at this layer and excluded in TRANSFORM_.';
COMMENT ON COLUMN STAGE_PRODUCT_EVENTS.EVENT_ID        IS '625 rows expected to be NULL — surrogate key assigned in TRANSFORM_.';
COMMENT ON COLUMN STAGE_PRODUCT_EVENTS.EVENT_NAME      IS 'Raw event name — mixed case inconsistency (e.g. Login, login, user_login). Canonical mapping applied in TRANSFORM_.';
COMMENT ON COLUMN STAGE_PRODUCT_EVENTS.IS_TEST_EVENT   IS '1 = test event. ~1,195 test events expected. Excluded from all TRANSFORM_ views.';
COMMENT ON COLUMN STAGE_PRODUCT_EVENTS.USER_ID         IS '1,181 rows expected to be NULL — anonymous/automation events. Retained for account-level metrics.';
COMMENT ON COLUMN STAGE_PRODUCT_EVENTS.ACCOUNT_ID      IS '653 rows expected to be NULL — excluded from all account-scoped KPIs in TRANSFORM_.';

-- ============================================================
-- STAGE AUDIT / LOAD LOG TABLE
-- Records one row per ETL run per table for monitoring
-- ============================================================
CREATE TABLE STAGE_LOAD_LOG (
    LOG_ID              NUMBER(10)      GENERATED ALWAYS AS IDENTITY,
    BATCH_ID            NUMBER(10)      NOT NULL,
    TABLE_NAME          VARCHAR2(100)   NOT NULL,
    SOURCE_FILE         VARCHAR2(255),
    ROWS_LOADED         NUMBER(12)      DEFAULT 0,
    LOAD_STARTED_AT     DATE,
    LOAD_COMPLETED_AT   DATE,
    LOAD_STATUS         VARCHAR2(20)    DEFAULT 'PENDING',  -- PENDING / SUCCESS / FAILED
    ERROR_MESSAGE       VARCHAR2(4000),
    LOADED_BY           VARCHAR2(100)   DEFAULT SYS_CONTEXT('USERENV','SESSION_USER'),
    CONSTRAINT PK_STAGE_LOAD_LOG PRIMARY KEY (LOG_ID)
)
TABLESPACE USERS;

COMMENT ON TABLE STAGE_LOAD_LOG IS 'One row per ETL run per target stage table. Used by the Data Quality Monitor Power BI page.';

-- ============================================================
-- INDEXES ON STAGE TABLES
-- Minimal indexing — enough to support TRANSFORM_ join
-- performance without slowing down bulk inserts
-- ============================================================

-- STAGE_ACCOUNTS
CREATE INDEX IDX_STGACC_BATCH     ON STAGE_ACCOUNTS      (BATCH_ID);
CREATE INDEX IDX_STGACC_ACCID     ON STAGE_ACCOUNTS      (ACCOUNT_ID);

-- STAGE_USERS
CREATE INDEX IDX_STGUSR_BATCH     ON STAGE_USERS         (BATCH_ID);
CREATE INDEX IDX_STGUSR_ACCID     ON STAGE_USERS         (ACCOUNT_ID);

-- STAGE_DEALS
CREATE INDEX IDX_STGDEAL_BATCH    ON STAGE_DEALS         (BATCH_ID);
CREATE INDEX IDX_STGDEAL_ACCID    ON STAGE_DEALS         (ACCOUNT_ID);
CREATE INDEX IDX_STGDEAL_STATUS   ON STAGE_DEALS         (STATUS);

-- STAGE_PRODUCT_EVENTS
CREATE INDEX IDX_STGEVT_BATCH     ON STAGE_PRODUCT_EVENTS (BATCH_ID);
CREATE INDEX IDX_STGEVT_ACCID     ON STAGE_PRODUCT_EVENTS (ACCOUNT_ID);
CREATE INDEX IDX_STGEVT_EVTDATE   ON STAGE_PRODUCT_EVENTS (EVENT_DATE);
CREATE INDEX IDX_STGEVT_TEST      ON STAGE_PRODUCT_EVENTS (IS_TEST_EVENT);
CREATE INDEX IDX_STGEVT_EVTNAME   ON STAGE_PRODUCT_EVENTS (EVENT_NAME);
