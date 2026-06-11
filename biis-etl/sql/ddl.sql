-- BIIS ETL migration schema (derived from Informatica XML source/target definitions)
-- Idempotent: drops + recreates the test database.
IF DB_ID('biis_test') IS NOT NULL
BEGIN
    ALTER DATABASE biis_test SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE biis_test;
END
GO
CREATE DATABASE biis_test;
GO
USE biis_test;
GO

-- PAY_PERIOD table (from XML/Pay_Calendar source fields + target fields)
CREATE TABLE dbo.PAY_PERIOD (
    PP_NUM         SMALLINT       NOT NULL,
    PP_END_YEAR    SMALLINT       NOT NULL,
    PP_START_DTE   DATETIME2      NULL,
    PP_END_DTE     DATETIME2      NULL,
    LV_NUM         SMALLINT       NULL,
    LV_YEAR        SMALLINT       NULL,
    PAY_DTE        DATETIME2      NULL,
    CURR_PP_FLAG   NVARCHAR(1)    NULL,
    HOLIDAY_1      DATETIME2      NULL,
    HOLIDAY_2      DATETIME2      NULL,
    PRIMARY KEY (PP_NUM, PP_END_YEAR)
);
GO

-- COMP_TIME_DAILY_TBL (from XML/COMPTIME target fields)
CREATE TABLE dbo.COMP_TIME_DAILY_TBL (
    PP_END_YEAR          SMALLINT       NULL,
    PP_NUM               SMALLINT       NULL,
    PP_YEAR_NUM          INT            NULL,
    SSN                  NVARCHAR(9)    NULL,
    NAME                 NVARCHAR(30)   NULL,
    CURRENT_ACCT         NVARCHAR(6)    NULL,
    CURRENT_ORG          NVARCHAR(7)    NULL,
    FLSA_STATUS          NVARCHAR(1)    NULL,
    COMP_TIME_CUR_BAL    DECIMAL(8,2)   NULL,
    COMP_TIME_YEAR_EARNED DECIMAL(4,0)  NULL,
    PP_END_DATE          DATETIME2      NULL,
    DAILY_DATE_EARNED    DATETIME2      NULL,
    COMP_TIME_RATE       DECIMAL(6,2)   NULL,
    COMP_TIME_HOURS      DECIMAL(8,2)   NULL,
    COMP_TIME_UNDEF      DECIMAL(6,0)   NULL
);
GO

-- COUNTER_TBL (from XML/COMPTIME target fields)
CREATE TABLE dbo.COUNTER_TBL (
    RUN_DATE             DATETIME2      NULL,
    PROCESS_NAME         NVARCHAR(100)  NULL,
    COUNTER_DESCRIPTION  NVARCHAR(200)  NULL,
    COUNTER_VALUE        DECIMAL(15,0)  NULL,
    PP_END_YEAR          SMALLINT       NULL,
    PP_NUM               SMALLINT       NULL,
    CYCLE_ID             SMALLINT       NULL
);
GO

-- Migration audit table (new - not from Informatica)
CREATE TABLE dbo.MIGRATION_TEST_LOG (
    test_run_id      NVARCHAR(50)   NOT NULL,
    module           NVARCHAR(50)   NOT NULL,
    test_type        NVARCHAR(20)   NOT NULL,
    test_name        NVARCHAR(200)  NOT NULL,
    status           NVARCHAR(10)   NOT NULL,
    duration_ms      DECIMAL(12,2)  NULL,
    details          NVARCHAR(MAX)  NULL,
    run_timestamp    DATETIME2      DEFAULT GETDATE()
);
GO
