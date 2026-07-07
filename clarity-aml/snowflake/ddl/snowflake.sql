-- ═══════════════════════════════════════════════════════════════
-- CLARITY AML — Snowflake Setup (Full)
-- ═══════════════════════════════════════════════════════════════

-- ── Database and schemas ───────────────────────────────────────
CREATE DATABASE IF NOT EXISTS CLARITY_AML;
USE DATABASE CLARITY_AML;

CREATE SCHEMA IF NOT EXISTS GOLD;        -- detection output from Databricks
CREATE SCHEMA IF NOT EXISTS DASHBOARD;   -- views for investigator dashboard
CREATE SCHEMA IF NOT EXISTS ACTIONS;     -- investigator actions, SAR filings

-- ── Warehouse ──────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS CLARITY_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE CLARITY_WH;

-- ═══════════════════════════════════════════════════════════════
-- GOLD SCHEMA — written by Databricks NB02
-- ═══════════════════════════════════════════════════════════════
USE SCHEMA GOLD;

-- ── 1. AML Alerts ──────────────────────────────────────────────
CREATE OR REPLACE TABLE AML_ALERTS (
    account_id              VARCHAR,
    alert_date              DATE,
    risk_score              INTEGER,
    risk_tier               VARCHAR,        -- LOW / MEDIUM / HIGH / CRITICAL
    account_flags_summary   VARCHAR,
    is_structuring          BOOLEAN,
    is_layering             BOOLEAN,
    is_circular             BOOLEAN,
    is_fan_in               BOOLEAN,
    sanctions_hit           BOOLEAN,
    statistical_anomaly     BOOLEAN,
    coherence_risk_flag     BOOLEAN,
    young_account_high_value BOOLEAN,
    alert_narrative         VARCHAR,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 2. Account risk scores ─────────────────────────────────────
CREATE OR REPLACE TABLE ACCOUNT_RISK_SCORES (
    account_id                      VARCHAR,
    process_date                    DATE,
    hist_avg_amount                 FLOAT,
    z_score                         FLOAT,
    layering_chain_length           INTEGER,
    sanctions_hit                   BOOLEAN,
    kvk_registered                  BOOLEAN,
    risk_score                      INTEGER,
    risk_tier                       VARCHAR,
    cluster_risk_score              INTEGER,
    cluster_risk_category           VARCHAR,
    cluster_primary_typology        VARCHAR,
    structuring_txn_count           INTEGER,
    structuring_total_amount        FLOAT,
    connection_degree               INTEGER,
    is_circular_account             BOOLEAN,
    has_statistical_anomaly         BOOLEAN,
    coherence_risk_score            FLOAT,
    account_age_days                FLOAT,
    direct_counterparty_risk_ratio  FLOAT,
    counterparty_sanctions_density  FLOAT,
    purpose_code_variety            INTEGER,
    has_operational_payments        BOOLEAN,
    flagged_for_investigation       BOOLEAN,
    created_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 3. Flagged transactions ────────────────────────────────────
CREATE OR REPLACE TABLE FLAGGED_TRANSACTIONS (
    transaction_id          VARCHAR,
    sender_iban             VARCHAR,
    sender_name             VARCHAR,
    receiver_iban           VARCHAR,
    receiver_name           VARCHAR,
    amount_eur              FLOAT,
    purpose_code            VARCHAR,
    value_date              DATE,
    aml_pattern             VARCHAR,
    risk_score              INTEGER,
    flags                   VARCHAR,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 4. Circular clusters ───────────────────────────────────────
CREATE OR REPLACE TABLE CIRCULAR_CLUSTERS (
    account_id              VARCHAR,
    component               VARCHAR,
    detected_date           DATE,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 5. Layering chains ─────────────────────────────────────────
CREATE OR REPLACE TABLE LAYERING_CHAINS (
    account_id              VARCHAR,
    component               VARCHAR,
    chain_length            INTEGER,
    confirmed               BOOLEAN,
    detected_date           DATE,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT 'Gold tables created ✅' AS status;

-- ═══════════════════════════════════════════════════════════════
-- ACTIONS SCHEMA — written by investigator dashboard
-- ═══════════════════════════════════════════════════════════════
USE SCHEMA ACTIONS;

-- ── 6. Investigator actions ────────────────────────────────────
CREATE OR REPLACE TABLE INVESTIGATOR_ACTIONS (
    action_id               VARCHAR DEFAULT UUID_STRING(),
    account_id              VARCHAR,
    action_type             VARCHAR,    -- BLOCK / UNBLOCK / SAR / NOTE / REVIEW
    action_status           VARCHAR,    -- PENDING / APPROVED / REJECTED
    investigator            VARCHAR,
    notes                   VARCHAR,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 7. SAR filings ─────────────────────────────────────────────
CREATE OR REPLACE TABLE SAR_FILINGS (
    sar_id                  VARCHAR DEFAULT UUID_STRING(),
    account_id              VARCHAR,
    filing_date             DATE,
    pattern_type            VARCHAR,
    total_amount_eur        FLOAT,
    narrative               VARCHAR,
    status                  VARCHAR,    -- DRAFT / SUBMITTED / ACKNOWLEDGED
    investigator            VARCHAR,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 8. Case management ─────────────────────────────────────────
CREATE OR REPLACE TABLE CASE_MANAGEMENT (
    case_id                 VARCHAR DEFAULT UUID_STRING(),
    account_id              VARCHAR,
    alert_date              DATE,
    status                  VARCHAR,    -- OPEN / UNDER_REVIEW / CLOSED_TP / CLOSED_FP
    assigned_to             VARCHAR,
    priority                VARCHAR,    -- HIGH / MEDIUM / LOW
    resolution_notes        VARCHAR,
    opened_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    closed_at               TIMESTAMP_NTZ
);

SELECT 'Action tables created ✅' AS status;

-- ═══════════════════════════════════════════════════════════════
-- DASHBOARD SCHEMA — views over Gold + Actions
-- ═══════════════════════════════════════════════════════════════
USE SCHEMA DASHBOARD;

-- ── Alert queue view ───────────────────────────────────────────
-- What the investigator sees on the main dashboard
CREATE OR REPLACE VIEW V_ALERT_QUEUE AS
SELECT
    a.account_id,
    a.alert_date,
    a.risk_score,
    a.risk_tier,
    a.account_flags_summary,
    a.is_structuring,
    a.is_layering,
    a.is_circular,
    a.sanctions_hit,
    a.statistical_anomaly,
    a.alert_narrative,
    -- Case status from actions schema
    c.status        AS case_status,
    c.assigned_to,
    c.priority,
    -- Has any action been taken?
    CASE WHEN ia.account_id IS NOT NULL
         THEN TRUE ELSE FALSE
    END             AS has_action
FROM CLARITY_AML.GOLD.AML_ALERTS a
LEFT JOIN CLARITY_AML.ACTIONS.CASE_MANAGEMENT c
    ON a.account_id = c.account_id
LEFT JOIN CLARITY_AML.ACTIONS.INVESTIGATOR_ACTIONS ia
    ON a.account_id = ia.account_id
ORDER BY a.risk_score DESC;

-- ── Account drilldown view ─────────────────────────────────────
CREATE OR REPLACE VIEW V_ACCOUNT_DETAIL AS
SELECT
    a.account_id,
    a.alert_date,
    a.risk_score,
    a.risk_tier,
    a.account_flags_summary,
    a.is_structuring,
    a.is_layering,
    a.is_circular,
    a.is_fan_in,
    a.sanctions_hit,
    a.statistical_anomaly,
    a.coherence_risk_flag,
    -- Risk score details
    s.cluster_risk_score,
    s.cluster_primary_typology,
    s.structuring_txn_count,
    s.structuring_total_amount,
    s.layering_chain_length,
    s.connection_degree,
    s.has_statistical_anomaly,
    s.z_score,
    s.hist_avg_amount,
    s.account_age_days,
    s.coherence_risk_score,
    s.direct_counterparty_risk_ratio,
    s.counterparty_sanctions_density,
    s.flagged_for_investigation
FROM CLARITY_AML.GOLD.AML_ALERTS a
LEFT JOIN CLARITY_AML.GOLD.ACCOUNT_RISK_SCORES s
    ON a.account_id = s.account_id;

-- ── SAR summary view ───────────────────────────────────────────
CREATE OR REPLACE VIEW V_SAR_SUMMARY AS
SELECT
    s.sar_id,
    s.account_id,
    s.filing_date,
    s.pattern_type,
    s.total_amount_eur,
    s.narrative,
    s.status,
    s.investigator,
    a.risk_score,
    a.account_flags_summary
FROM CLARITY_AML.ACTIONS.SAR_FILINGS s
LEFT JOIN CLARITY_AML.GOLD.AML_ALERTS a
    ON s.account_id = a.account_id
ORDER BY s.filing_date DESC;

-- ── Daily stats view ───────────────────────────────────────────
CREATE OR REPLACE VIEW V_DAILY_STATS AS
SELECT
    alert_date,
    COUNT(*)                                    AS total_alerts,
    COUNT(CASE WHEN risk_tier = 'HIGH'
               THEN 1 END)                      AS high_risk_count,
    COUNT(CASE WHEN risk_tier = 'MEDIUM'
               THEN 1 END)                      AS medium_risk_count,
    COUNT(CASE WHEN sanctions_hit = TRUE
               THEN 1 END)                      AS sanctions_hits,
    COUNT(CASE WHEN is_structuring = TRUE
               THEN 1 END)                      AS structuring_count,
    COUNT(CASE WHEN is_layering = TRUE
               THEN 1 END)                      AS layering_count,
    COUNT(CASE WHEN is_circular = TRUE
               THEN 1 END)                      AS circular_count,
    AVG(risk_score)                             AS avg_risk_score,
    MAX(risk_score)                             AS max_risk_score
FROM CLARITY_AML.GOLD.AML_ALERTS
GROUP BY alert_date
ORDER BY alert_date DESC;

SELECT 'Dashboard views created ✅' AS status;

-- ── Final check ────────────────────────────────────────────────
SELECT CURRENT_ACCOUNT_LOCATOR(), CURRENT_REGION(), CURRENT_ACCOUNT();


USE DATABASE CLARITY_AML;
USE WAREHOUSE CLARITY_WH;

-- ── Row counts across all tables ───────────────────────────────
SELECT 'AML_ALERTS'          AS table_name, COUNT(*) AS row_count FROM GOLD.AML_ALERTS
UNION ALL
SELECT 'ACCOUNT_RISK_SCORES', COUNT(*) FROM GOLD.ACCOUNT_RISK_SCORES
UNION ALL
SELECT 'FLAGGED_TRANSACTIONS', COUNT(*) FROM GOLD.FLAGGED_TRANSACTIONS
UNION ALL
SELECT 'CIRCULAR_CLUSTERS',   COUNT(*) FROM GOLD.CIRCULAR_CLUSTERS
UNION ALL
SELECT 'LAYERING_CHAINS',     COUNT(*) FROM GOLD.LAYERING_CHAINS
UNION ALL
SELECT 'INVESTIGATOR_ACTIONS',COUNT(*) FROM ACTIONS.INVESTIGATOR_ACTIONS
UNION ALL
SELECT 'SAR_FILINGS',         COUNT(*) FROM ACTIONS.SAR_FILINGS
UNION ALL
SELECT 'CASE_MANAGEMENT',     COUNT(*) FROM ACTIONS.CASE_MANAGEMENT
ORDER BY row_count DESC;

DESC TABLE CLARITY_AML.GOLD.AML_ALERTS;