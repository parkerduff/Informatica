"""
Oracle XE schema setup and synthetic data generation.

Creates ALL source, target, lookup, counter, and error tables referenced
by the 6 migrated Informatica workflows. Generates synthetic test data
at two tiers:
  - Functional: 50-1000 rows per table (covers all code paths)
  - Performance: 10K-1M+ rows per table (stress testing)

Usage:
    python -m pyspark_migration.scripts.setup_oracle_schema --tier functional
    python -m pyspark_migration.scripts.setup_oracle_schema --tier performance

Table DDL extracted from Informatica XML Source/Target definitions.
"""

import argparse
import logging
import os
import random
import string
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import oracledb

from pyspark_migration.common.config import OracleConnectionConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ─── DDL Statements ──────────────────────────────────────────────────────────

DDL_STATEMENTS = [
    # PAY_PERIOD - Central reference table (Pay_Calendar, COMPTIME, all jobs)
    """CREATE TABLE PAY_PERIOD (
        PP_NUM          NUMBER(2,0)   NOT NULL,
        PP_END_YEAR     NUMBER(4,0)   NOT NULL,
        PP_START_DTE    DATE,
        PP_END_DTE      DATE,
        LV_NUM          NUMBER(2,0),
        LV_YEAR         NUMBER(4,0),
        PAY_DTE         DATE,
        CURR_PP_FLAG    VARCHAR2(1),
        HOLIDAY_1       DATE,
        HOLIDAY_2       DATE,
        CONSTRAINT pk_pay_period PRIMARY KEY (PP_NUM, PP_END_YEAR)
    )""",

    # COMP_TIME_DAILY_TBL - COMPTIME target
    """CREATE TABLE COMP_TIME_DAILY_TBL (
        PP_END_YEAR           NUMBER(4,0),
        PP_NUM                NUMBER(2,0),
        PP_YEAR_NUM           NUMBER(6,0),
        SSN                   VARCHAR2(9),
        NAME                  VARCHAR2(50),
        CURRENT_ACCT          VARCHAR2(20),
        CURRENT_ORG           VARCHAR2(20),
        FLSA_STATUS           VARCHAR2(5),
        COMP_TIME_CUR_BAL     VARCHAR2(20),
        COMP_TIME_YEAR_EARNED VARCHAR2(20),
        PP_END_DATE           DATE,
        DAILY_DATE_EARNED     DATE,
        COMP_TIME_RATE        VARCHAR2(20),
        COMP_TIME_HOURS       VARCHAR2(20),
        COMP_TIME_UNDEF       VARCHAR2(20)
    )""",

    # COUNTER_TBL - Shared counter tracking
    """CREATE TABLE COUNTER_TBL (
        RUN_DATE              TIMESTAMP,
        PROCESS_NAME          VARCHAR2(100) NOT NULL,
        COUNTER_DESCRIPTION   VARCHAR2(200),
        COUNTER_VALUE         NUMBER(18,0),
        PP_END_YEAR           VARCHAR2(10),
        PP_NUM                VARCHAR2(10),
        CYCLE_ID              VARCHAR2(20)
    )""",

    # ERROR_TBL - Shared error tracking
    """CREATE TABLE ERROR_TBL (
        PROCESS_NAME    VARCHAR2(100) NOT NULL,
        ERROR_MESSAGE   VARCHAR2(500),
        SOURCE_KEY      VARCHAR2(100),
        ERROR_DATE      TIMESTAMP,
        PP_END_YEAR     VARCHAR2(10),
        PP_NUM          VARCHAR2(10),
        CYCLE_ID        VARCHAR2(20),
        ERROR_CODE      VARCHAR2(20)
    )""",

    # PSEUDOSSN_TBL - PseudoSSN management (63 fields simplified)
    """CREATE TABLE PSEUDOSSN_TBL (
        PSEUDOSSN           VARCHAR2(9) NOT NULL,
        TK_NUM              VARCHAR2(20),
        PSEUDOSSN_EFF_DT    DATE,
        PP_END_YEAR         NUMBER(4,0),
        PP_NUM              NUMBER(2,0),
        LOAD_DATE           TIMESTAMP,
        CONSTRAINT pk_pseudossn PRIMARY KEY (PSEUDOSSN)
    )""",

    # PSEUDOSSN_FROM_SDA_TBL - SDA staging
    """CREATE TABLE PSEUDOSSN_FROM_SDA_TBL (
        PSEUDOSSN           VARCHAR2(9) NOT NULL,
        TK_NUM              VARCHAR2(20),
        PSEUDOSSN_EFF_DT    DATE,
        PP_END_YEAR         NUMBER(4,0),
        PP_NUM              NUMBER(2,0),
        LOAD_DATE           TIMESTAMP
    )""",

    # HI_ARCH_PSEUDOSSN_TBL - PseudoSSN archive
    """CREATE TABLE HI_ARCH_PSEUDOSSN_TBL (
        PSEUDOSSN           VARCHAR2(9),
        TK_NUM              VARCHAR2(20),
        PSEUDOSSN_EFF_DT    DATE,
        PP_END_YEAR         NUMBER(4,0),
        PP_NUM              NUMBER(2,0),
        LOAD_DATE           TIMESTAMP
    )""",

    # PSEUDO_RECORD_COUNT - Record count validation
    """CREATE TABLE PSEUDO_RECORD_COUNT (
        RECORD_TYPE     VARCHAR2(10),
        RECORD_COUNT    NUMBER(10,0),
        LOAD_DATE       TIMESTAMP
    )""",

    # HI_PM_FDA_TATRAN_TBL - FDA leave transactions
    """CREATE TABLE HI_PM_FDA_TATRAN_TBL (
        FDA_TK_NO       VARCHAR2(20),
        FDA_EMP_ID      VARCHAR2(9),
        FDA_PP_YEAR     VARCHAR2(4),
        FDA_PP_NUM      VARCHAR2(2),
        FDA_REC_TYPE    VARCHAR2(3),
        FDA_HOURS       VARCHAR2(20),
        BATCH_SEQ       NUMBER(10,0),
        LOAD_DATE       TIMESTAMP
    )""",

    # CPM_CYCLE_TBL - CPM process cycle tracking
    """CREATE TABLE CPM_CYCLE_TBL (
        PROCESS_NAME    VARCHAR2(50) NOT NULL,
        PP_END_YEAR     NUMBER(4,0),
        PP_NUM          NUMBER(2,0),
        CYCLE_ID        VARCHAR2(20),
        RUN_DATE        TIMESTAMP
    )""",

    # CPM_NEWPAY_TBL - Core payroll data (501 fields, simplified)
    """CREATE TABLE CPM_NEWPAY_TBL (
        PP_END_YEAR         NUMBER(4,0),
        PP_NUM              NUMBER(2,0),
        DFAS_PSEUDO_SSN     VARCHAR2(9),
        MP_POOL_DES         VARCHAR2(5),
        BUDGET_ORG_CDE      VARCHAR2(10),
        YTD_GROSS_PAY       NUMBER(15,2),
        YTD_FED_TAX_DED     NUMBER(15,2),
        YTD_FICA_DED        NUMBER(15,2),
        CPP_GROSS_PAY       NUMBER(15,2),
        CPP_BASE_PAY        NUMBER(15,2),
        LOAD_DATE           TIMESTAMP
    )""",

    # CPM_YTD_DETAIL_STG_TBL - YTD staging
    """CREATE TABLE CPM_YTD_DETAIL_STG_TBL (
        PP_END_YEAR     NUMBER(4,0),
        PP_NUM          NUMBER(2,0),
        DYD_SSN_1       VARCHAR2(9),
        YTD_GROSS_PAY   NUMBER(15,2),
        LOAD_DATE       TIMESTAMP
    )""",

    # CPM_PAD_DETAIL_STG_TBL - PAD staging
    """CREATE TABLE CPM_PAD_DETAIL_STG_TBL (
        PP_END_YEAR     NUMBER(4,0),
        PP_NUM          NUMBER(2,0),
        PAD_SOC_SEC_NO  VARCHAR2(9),
        LOAD_DATE       TIMESTAMP
    )""",

    # CPM_MER_DETAIL_STG_TBL - MER staging
    """CREATE TABLE CPM_MER_DETAIL_STG_TBL (
        PP_END_YEAR     NUMBER(4,0),
        PP_NUM          NUMBER(2,0),
        MER_SSN         VARCHAR2(9),
        LOAD_DATE       TIMESTAMP
    )""",

    # PS_GVT_JOB - EHRP source (246 fields, simplified)
    """CREATE TABLE PS_GVT_JOB (
        EMPLID          VARCHAR2(11) NOT NULL,
        EMPL_RCD        NUMBER(3,0) NOT NULL,
        EFFDT           DATE NOT NULL,
        EFFSEQ          NUMBER(3,0) NOT NULL,
        DEPTID          VARCHAR2(10),
        JOBCODE         VARCHAR2(6),
        GVT_NOA_CODE    VARCHAR2(4),
        LOAD_DATE       TIMESTAMP
    )""",

    # NWK_NEW_EHRP_ACTIONS_TBL - EHRP staging
    """CREATE TABLE NWK_NEW_EHRP_ACTIONS_TBL (
        EMPLID          VARCHAR2(11) NOT NULL,
        EMPL_RCD        NUMBER(3,0) NOT NULL,
        EFFDT           DATE NOT NULL,
        EFFSEQ          NUMBER(3,0) NOT NULL
    )""",

    # EHRP_RECS_TRACKING_TBL - EHRP tracking
    """CREATE TABLE EHRP_RECS_TRACKING_TBL (
        EMPLID              VARCHAR2(11),
        EMPL_RCD            NUMBER(3,0),
        EFFDT               DATE,
        EFFSEQ              NUMBER(3,0),
        DEPTID              VARCHAR2(10),
        GVT_NOA_CODE        VARCHAR2(4),
        GVT_WIP_STATUS      VARCHAR2(2),
        BIIS_WIP_STATUS_CHANGED_DT DATE,
        BIIS_EVENT_ID       NUMBER(10,0),
        LOAD_DATE           TIMESTAMP
    )""",

    # NWK_ACTION_PRIMARY_TBL - Action primary records
    """CREATE TABLE NWK_ACTION_PRIMARY_TBL (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        DEPTID          VARCHAR2(10),
        JOBCODE         VARCHAR2(6),
        EVENT_ID        NUMBER(10,0),
        LOAD_DATE       TIMESTAMP
    )""",

    # NWK_ACTION_SECONDARY_TBL - Action secondary records
    """CREATE TABLE NWK_ACTION_SECONDARY_TBL (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        RETND1_STEP_CD  VARCHAR2(20),
        LOAD_DATE       TIMESTAMP
    )""",

    # SEQUENCE_NUM_TBL - Sequence number tracking
    """CREATE TABLE SEQUENCE_NUM_TBL (
        EHRP_YEAR       NUMBER(4,0) NOT NULL,
        SEQ_NUM         NUMBER(10,0),
        CONSTRAINT pk_seq_num PRIMARY KEY (EHRP_YEAR)
    )""",

    # Lookup tables for EHRP2BIIS cross-DB lookups
    """CREATE TABLE PS_GVT_EMPLOYMENT (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        HIRE_DT         DATE,
        REHIRE_DT       DATE
    )""",

    """CREATE TABLE PS_GVT_PERS_NID (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        NATIONAL_ID     VARCHAR2(20)
    )""",

    """CREATE TABLE PS_GVT_AWD_DATA (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        GVT_AWD_AMOUNT  NUMBER(15,2)
    )""",

    """CREATE TABLE PS_GVT_EE_DATA_TRK (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        GVT_TRACK_DATA  VARCHAR2(50)
    )""",

    """CREATE TABLE PS_HE_FILL_POS (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        POSITION_NBR    VARCHAR2(8)
    )""",

    """CREATE TABLE PS_GVT_CITIZENSHIP (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        CITIZENSHIP     VARCHAR2(3)
    )""",

    """CREATE TABLE PS_GVT_PERS_DATA (
        EMPLID          VARCHAR2(11),
        EMPL_RCD        NUMBER(3,0),
        EFFDT           DATE,
        EFFSEQ          NUMBER(3,0),
        NAME            VARCHAR2(50),
        BIRTHDATE       DATE
    )""",

    """CREATE TABLE PS_JPM_JP_ITEMS (
        JPM_PROFILE_ID  VARCHAR2(11),
        JPM_CAT_TYPE    VARCHAR2(4),
        JPM_ITEM_ID     VARCHAR2(20)
    )""",

    # HI_GENERIC_SRC_TBL - Generic source for concatenation triggers
    """CREATE TABLE HI_GENERIC_SRC_TBL (
        GENERIC_FIELD   VARCHAR2(1)
    )""",
]


def create_schema(config: OracleConnectionConfig) -> None:
    """Create all tables, dropping existing ones first."""
    conn = oracledb.connect(
        user=config.username,
        password=config.password,
        dsn=config.thin_url,
    )
    cursor = conn.cursor()

    for ddl in DDL_STATEMENTS:
        table_name = ddl.split("TABLE")[1].strip().split("(")[0].strip()
        try:
            cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
            conn.commit()
            logger.info("Dropped table: %s", table_name)
        except Exception:
            pass

        try:
            cursor.execute(ddl)
            conn.commit()
            logger.info("Created table: %s", table_name)
        except Exception as exc:
            logger.error("Failed to create %s: %s", table_name, str(exc))

    conn.close()
    logger.info("Schema creation complete")


def generate_ssn() -> str:
    """Generate a valid-format SSN (not 000, 666, or 9xx prefix)."""
    while True:
        area = random.randint(1, 899)
        if area in (0, 666):
            continue
        if area >= 900:
            continue
        group = random.randint(1, 99)
        serial = random.randint(1, 9999)
        return f"{area:03d}{group:02d}{serial:04d}"


def generate_flat_files(data_dir: str) -> None:
    """Generate flat file test data for COMPTIME (headerless CSV).

    Creates multiple test files:
      - U0287D01.txt: Correct data (100 rows)
      - U0287D01_empty.txt: Empty file (zero bytes)
      - U0287D01_single.txt: Single row
      - U0287D01_bad_cols.txt: Missing columns
      - U0287D01_extra_cols.txt: Extra columns
    """
    import csv
    os.makedirs(data_dir, exist_ok=True)

    # 15 fields matching COMP_TIME_DAILY_TBL layout (no header)
    def _row(i: int) -> list:
        ssn = f"{random.randint(100,899):03d}{random.randint(10,99)}{random.randint(1000,9999)}"
        pp_end = datetime(2025, 6, 28) + timedelta(days=random.randint(-14, 14))
        return [
            "2025", "13", "202513", ssn,
            f"EMPLOYEE{i:04d}", f"ACCT{i % 10:02d}", f"ORG{i % 5:03d}",
            random.choice(["E", "N"]),  # FLSA
            f"{random.uniform(0, 240):.2f}",  # balance
            "2025",  # year earned
            pp_end.strftime("%Y%m%d"),
            (pp_end - timedelta(days=random.randint(0, 13))).strftime("%Y%m%d"),
            f"{random.uniform(1.0, 1.5):.4f}",  # rate
            f"{random.uniform(0, 16):.2f}",  # hours
            "",  # undef
        ]

    # Correct file (100 rows)
    with open(os.path.join(data_dir, "U0287D01.txt"), "w", newline="") as f:
        w = csv.writer(f)
        # Include edge cases: invalid SSNs, bad dates
        for i in range(100):
            row = _row(i)
            if i == 95:  # invalid SSN (000 prefix)
                row[3] = "000123456"
            if i == 96:  # invalid SSN (666 prefix)
                row[3] = "666998877"
            if i == 97:  # bad date
                row[10] = "99999999"
            if i == 98:  # null-like SSN
                row[3] = ""
            w.writerow(row)

    # Empty file
    open(os.path.join(data_dir, "U0287D01_empty.txt"), "w").close()

    # Single row
    with open(os.path.join(data_dir, "U0287D01_single.txt"), "w", newline="") as f:
        csv.writer(f).writerow(_row(0))

    # Missing columns (only 10 of 15)
    with open(os.path.join(data_dir, "U0287D01_bad_cols.txt"), "w", newline="") as f:
        w = csv.writer(f)
        for i in range(5):
            w.writerow(_row(i)[:10])

    # Extra columns (18 instead of 15)
    with open(os.path.join(data_dir, "U0287D01_extra_cols.txt"), "w", newline="") as f:
        w = csv.writer(f)
        for i in range(5):
            w.writerow(_row(i) + ["EXTRA1", "EXTRA2", "EXTRA3"])

    logger.info("Generated flat file test data in %s", data_dir)


def generate_functional_data(config: OracleConnectionConfig) -> None:
    """Generate functional test data (50-1000 rows per table)."""
    conn = oracledb.connect(
        user=config.username,
        password=config.password,
        dsn=config.thin_url,
    )
    cursor = conn.cursor()

    # ─── PAY_PERIOD (26 pay periods for 2025) ────────────────────────────────
    base_date = datetime(2025, 1, 5)
    for pp in range(1, 27):
        start = base_date + timedelta(days=(pp - 1) * 14)
        end = start + timedelta(days=13)
        pay = end + timedelta(days=7)
        flag = "Y" if pp == 13 else None
        cursor.execute(
            "INSERT INTO PAY_PERIOD (PP_NUM, PP_END_YEAR, PP_START_DTE, "
            "PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
            [pp, 2025, start, end, pp, 2025, pay, flag],
        )
    # Edge case: out-of-range PP_NUM (27) for accuracy validation
    cursor.execute(
        "INSERT INTO PAY_PERIOD (PP_NUM, PP_END_YEAR, PP_START_DTE, "
        "PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG) "
        "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
        [27, 2025, datetime(2025, 12, 28), datetime(2026, 1, 10), 27, 2025,
         datetime(2026, 1, 17), None],
    )
    logger.info("Inserted 27 PAY_PERIOD rows (26 valid + 1 out-of-range)")

    # ─── COMP_TIME_DAILY_TBL synthetic source data ───────────────────────────
    # (Data loaded by COMPTIME job from flat file, but we pre-populate for testing)

    # ─── PSEUDOSSN_TBL (200 records) ─────────────────────────────────────────
    ssns = [generate_ssn() for _ in range(200)]
    for i, ssn in enumerate(ssns):
        tk = f"TK{i:06d}" if random.random() > 0.3 else None
        eff_dt = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 180))
        cursor.execute(
            "INSERT INTO PSEUDOSSN_TBL (PSEUDOSSN, TK_NUM, PSEUDOSSN_EFF_DT, "
            "PP_END_YEAR, PP_NUM, LOAD_DATE) VALUES (:1, :2, :3, :4, :5, :6)",
            [ssn, tk, eff_dt, 2025, 13, datetime.now()],
        )
    logger.info("Inserted 200 PSEUDOSSN_TBL rows")

    # ─── CPM_NEWPAY_TBL (500 records) ────────────────────────────────────────
    for i in range(500):
        ssn = generate_ssn()
        cursor.execute(
            "INSERT INTO CPM_NEWPAY_TBL (PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, "
            "MP_POOL_DES, BUDGET_ORG_CDE, YTD_GROSS_PAY, YTD_FED_TAX_DED, "
            "YTD_FICA_DED, CPP_GROSS_PAY, CPP_BASE_PAY, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
            [
                2025, 13, ssn, " ", f"ORG{i % 10:03d}",
                round(random.uniform(20000, 120000), 2),
                round(random.uniform(2000, 30000), 2),
                round(random.uniform(1000, 9000), 2),
                round(random.uniform(1500, 6000), 2),
                round(random.uniform(1500, 6000), 2),
                datetime.now(),
            ],
        )
    logger.info("Inserted 500 CPM_NEWPAY_TBL rows")

    # ─── CPM staging tables (100 records each) ───────────────────────────────
    for i in range(100):
        ssn = generate_ssn()
        cursor.execute(
            "INSERT INTO CPM_YTD_DETAIL_STG_TBL "
            "(PP_END_YEAR, PP_NUM, DYD_SSN_1, YTD_GROSS_PAY, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5)",
            [2025, 13, ssn, round(random.uniform(20000, 120000), 2), datetime.now()],
        )
        cursor.execute(
            "INSERT INTO CPM_PAD_DETAIL_STG_TBL "
            "(PP_END_YEAR, PP_NUM, PAD_SOC_SEC_NO, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4)",
            [2025, 13, ssn, datetime.now()],
        )
        cursor.execute(
            "INSERT INTO CPM_MER_DETAIL_STG_TBL "
            "(PP_END_YEAR, PP_NUM, MER_SSN, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4)",
            [2025, 13, ssn, datetime.now()],
        )
    logger.info("Inserted 100 rows each in CPM staging tables")

    # ─── HI_PM_FDA_TATRAN_TBL (50 records) ───────────────────────────────────
    # Include rows for Post-SQL DELETE testing:
    #   - rec_type '02' = standard (should be kept)
    #   - rec_type '12' = qualifying (prevents delete)
    #   - rec_type '01' and '99' = should be filtered out
    fda_ssns_with_12 = []  # employees WITH rec_type=12 (should survive delete)
    fda_ssns_without_12 = []  # employees WITHOUT rec_type=12 (should be deleted)
    for i in range(50):
        ssn = generate_ssn()
        if i < 15:
            rec_type = "12"  # 15 employees WITH rec_type=12
            fda_ssns_with_12.append(ssn)
        elif i < 35:
            rec_type = "02"  # 20 employees with only rec_type=02 (NO rec_type=12)
            fda_ssns_without_12.append(ssn)
        elif i < 40:
            rec_type = "01"  # Should be filtered out (not '02')
        else:
            rec_type = "99"  # Should be filtered out
        cursor.execute(
            "INSERT INTO HI_PM_FDA_TATRAN_TBL "
            "(FDA_TK_NO, FDA_EMP_ID, FDA_PP_YEAR, FDA_PP_NUM, "
            "FDA_REC_TYPE, FDA_HOURS, BATCH_SEQ, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
            [
                f"TK{i:04d}", ssn, "2025", "13", rec_type,
                str(round(random.uniform(0, 80), 1)),
                i, datetime.now(),
            ],
        )
    logger.info(
        "Inserted 50 HI_PM_FDA_TATRAN_TBL rows "
        "(%d with rec_type=12, %d without)",
        len(fda_ssns_with_12), len(fda_ssns_without_12),
    )

    # ─── CPM_CYCLE_TBL ───────────────────────────────────────────────────────
    for proc in ["FDA", "NIH", "CDC", "OIG"]:
        cursor.execute(
            "INSERT INTO CPM_CYCLE_TBL "
            "(PROCESS_NAME, PP_END_YEAR, PP_NUM, CYCLE_ID, RUN_DATE) "
            "VALUES (:1, :2, :3, :4, :5)",
            [proc, 2025, 12, "CYC001", datetime.now()],
        )
    logger.info("Inserted CPM_CYCLE_TBL rows")

    # ─── EHRP tables (100 actions) ───────────────────────────────────────────
    empls = [f"E{i:010d}" for i in range(100)]
    for empl in empls:
        eff_dt = datetime(2025, 6, 1) + timedelta(days=random.randint(0, 30))
        cursor.execute(
            "INSERT INTO PS_GVT_JOB "
            "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ, DEPTID, JOBCODE, "
            "GVT_NOA_CODE, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
            [empl, 0, eff_dt, 0, f"D{random.randint(100,999)}",
             f"J{random.randint(100,999)}", f"{random.randint(100,999)}",
             datetime.now()],
        )
        cursor.execute(
            "INSERT INTO NWK_NEW_EHRP_ACTIONS_TBL "
            "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ) "
            "VALUES (:1, :2, :3, :4)",
            [empl, 0, eff_dt, 0],
        )

    # Lookup tables
    for empl in empls[:50]:
        eff_dt = datetime(2025, 6, 1)
        for table, extra_col, extra_val in [
            ("PS_GVT_EMPLOYMENT", "HIRE_DT", datetime(2020, 1, 15)),
            ("PS_GVT_PERS_NID", "NATIONAL_ID", generate_ssn()),
            ("PS_GVT_AWD_DATA", "GVT_AWD_AMOUNT", round(random.uniform(0, 5000), 2)),
            ("PS_GVT_EE_DATA_TRK", "GVT_TRACK_DATA", "ACTIVE"),
            ("PS_HE_FILL_POS", "POSITION_NBR", f"P{random.randint(10000,99999)}"),
            ("PS_GVT_CITIZENSHIP", "CITIZENSHIP", "US"),
            ("PS_GVT_PERS_DATA", "NAME", f"Employee {empl}"),
        ]:
            cursor.execute(
                f"INSERT INTO {table} "
                f"(EMPLID, EMPL_RCD, EFFDT, EFFSEQ, {extra_col}) "
                f"VALUES (:1, :2, :3, :4, :5)",
                [empl, 0, eff_dt, 0, extra_val],
            )

    for empl in empls[:30]:
        cursor.execute(
            "INSERT INTO PS_JPM_JP_ITEMS "
            "(JPM_PROFILE_ID, JPM_CAT_TYPE, JPM_ITEM_ID) "
            "VALUES (:1, :2, :3)",
            [empl, "COMP", f"ITEM{random.randint(1,100)}"],
        )

    cursor.execute(
        "INSERT INTO SEQUENCE_NUM_TBL (EHRP_YEAR, SEQ_NUM) VALUES (:1, :2)",
        [2025, 1000000],
    )

    cursor.execute(
        "INSERT INTO HI_GENERIC_SRC_TBL (GENERIC_FIELD) VALUES (:1)", ["X"]
    )

    logger.info("Inserted EHRP and lookup table data")

    # ─── Edge case data for business rule validation ───────────────────────

    # Duplicate keys in PSEUDOSSN_FROM_SDA_TBL (test uniqueness handling)
    dup_ssn = generate_ssn()
    for seq in range(3):
        cursor.execute(
            "INSERT INTO PSEUDOSSN_FROM_SDA_TBL "
            "(PSEUDOSSN, TK_NUM, PSEUDOSSN_EFF_DT, PP_END_YEAR, PP_NUM, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6)",
            [dup_ssn, f"TK_DUP{seq}",
             datetime(2025, 6, 1) + timedelta(days=seq),
             2025, 13, datetime.now()],
        )
    logger.info("Inserted 3 duplicate-key PSEUDOSSN_FROM_SDA_TBL rows")

    # NULL values in mandatory fields (test completeness checks)
    cursor.execute(
        "INSERT INTO CPM_NEWPAY_TBL (PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, "
        "MP_POOL_DES, BUDGET_ORG_CDE, YTD_GROSS_PAY, YTD_FED_TAX_DED, "
        "YTD_FICA_DED, CPP_GROSS_PAY, CPP_BASE_PAY, LOAD_DATE) "
        "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
        [2025, 13, None, None, None, None, None, None, None, None, datetime.now()],
    )
    logger.info("Inserted NULL-field CPM_NEWPAY_TBL row for completeness check")

    # Out-of-range PP_NUM (27) row for accuracy validation
    cursor.execute(
        "INSERT INTO CPM_NEWPAY_TBL (PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, "
        "MP_POOL_DES, BUDGET_ORG_CDE, YTD_GROSS_PAY, YTD_FED_TAX_DED, "
        "YTD_FICA_DED, CPP_GROSS_PAY, CPP_BASE_PAY, LOAD_DATE) "
        "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
        [2025, 27, generate_ssn(), " ", "ORGBAD",
         50000.00, 10000.00, 3000.00, 2000.00, 2000.00, datetime.now()],
    )
    logger.info("Inserted out-of-range PP_NUM row for accuracy check")

    # EHRP rows with duplicate EFFSEQ for deterministic lookup testing
    dup_empl = "EDUP0000001"
    for seq in range(3):
        eff_dt = datetime(2025, 6, 15)
        cursor.execute(
            "INSERT INTO PS_GVT_JOB "
            "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ, DEPTID, JOBCODE, "
            "GVT_NOA_CODE, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
            [dup_empl, 0, eff_dt, seq, "DDUP", "JDUP", "100", datetime.now()],
        )
    cursor.execute(
        "INSERT INTO NWK_NEW_EHRP_ACTIONS_TBL "
        "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ) VALUES (:1, :2, :3, :4)",
        [dup_empl, 0, datetime(2025, 6, 15), 0],
    )
    logger.info("Inserted duplicate-key EHRP rows for deterministic lookup test")

    conn.commit()
    conn.close()
    logger.info("Functional data generation complete")


def generate_performance_data(config: OracleConnectionConfig) -> None:
    """Generate performance test data (10K-1M+ rows)."""
    conn = oracledb.connect(
        user=config.username,
        password=config.password,
        dsn=config.thin_url,
    )
    cursor = conn.cursor()

    # CPM_NEWPAY_TBL - 100K rows
    logger.info("Generating 100K CPM_NEWPAY_TBL rows...")
    batch = []
    for i in range(100000):
        ssn = generate_ssn()
        batch.append([
            2025, 13, ssn, " ", f"ORG{i % 100:03d}",
            round(random.uniform(20000, 150000), 2),
            round(random.uniform(2000, 40000), 2),
            round(random.uniform(1000, 12000), 2),
            round(random.uniform(1500, 8000), 2),
            round(random.uniform(1500, 8000), 2),
            datetime.now(),
        ])
        if len(batch) >= 5000:
            cursor.executemany(
                "INSERT INTO CPM_NEWPAY_TBL "
                "(PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, MP_POOL_DES, "
                "BUDGET_ORG_CDE, YTD_GROSS_PAY, YTD_FED_TAX_DED, "
                "YTD_FICA_DED, CPP_GROSS_PAY, CPP_BASE_PAY, LOAD_DATE) "
                "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
                batch,
            )
            batch = []
            conn.commit()

    if batch:
        cursor.executemany(
            "INSERT INTO CPM_NEWPAY_TBL "
            "(PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, MP_POOL_DES, "
            "BUDGET_ORG_CDE, YTD_GROSS_PAY, YTD_FED_TAX_DED, "
            "YTD_FICA_DED, CPP_GROSS_PAY, CPP_BASE_PAY, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
            batch,
        )
    conn.commit()
    logger.info("Inserted 100K CPM_NEWPAY_TBL rows")

    # PSEUDOSSN_TBL - 50K rows (sequential SSNs to avoid PK collisions)
    logger.info("Generating 50K PSEUDOSSN_TBL rows...")
    batch = []
    for i in range(50000):
        # Sequential unique 9-digit SSNs: 100010001..649999999
        # Format: AAA-GG-SSSS  (area 100-649, group 01-99, serial 0001-9999)
        area = 100 + (i // (99 * 9999))
        group = 1 + ((i // 9999) % 99)
        serial = 1 + (i % 9999)
        ssn = f"{area:03d}{group:02d}{serial:04d}"
        tk = f"TK{i:06d}" if random.random() > 0.3 else None
        eff_dt = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))
        batch.append([ssn, tk, eff_dt, 2025, 13, datetime.now()])
        if len(batch) >= 5000:
            cursor.executemany(
                "INSERT INTO PSEUDOSSN_TBL "
                "(PSEUDOSSN, TK_NUM, PSEUDOSSN_EFF_DT, PP_END_YEAR, PP_NUM, LOAD_DATE) "
                "VALUES (:1, :2, :3, :4, :5, :6)",
                batch,
            )
            batch = []
            conn.commit()

    if batch:
        cursor.executemany(
            "INSERT INTO PSEUDOSSN_TBL "
            "(PSEUDOSSN, TK_NUM, PSEUDOSSN_EFF_DT, PP_END_YEAR, PP_NUM, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6)",
            batch,
        )
    conn.commit()
    logger.info("Inserted 50K PSEUDOSSN_TBL rows")

    # PS_GVT_JOB + NWK_NEW_EHRP_ACTIONS_TBL - 10K rows
    logger.info("Generating 10K EHRP action rows...")
    batch_job = []
    batch_new = []
    for i in range(10000):
        empl = f"E{i:010d}"
        eff_dt = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 180))
        batch_job.append([
            empl, 0, eff_dt, 0, f"D{random.randint(100,999)}",
            f"J{random.randint(100,999)}", f"{random.randint(100,999)}",
            datetime.now(),
        ])
        batch_new.append([empl, 0, eff_dt, 0])
        if len(batch_job) >= 5000:
            cursor.executemany(
                "INSERT INTO PS_GVT_JOB "
                "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ, DEPTID, JOBCODE, "
                "GVT_NOA_CODE, LOAD_DATE) "
                "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
                batch_job,
            )
            cursor.executemany(
                "INSERT INTO NWK_NEW_EHRP_ACTIONS_TBL "
                "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ) "
                "VALUES (:1, :2, :3, :4)",
                batch_new,
            )
            batch_job = []
            batch_new = []
            conn.commit()

    if batch_job:
        cursor.executemany(
            "INSERT INTO PS_GVT_JOB "
            "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ, DEPTID, JOBCODE, "
            "GVT_NOA_CODE, LOAD_DATE) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
            batch_job,
        )
        cursor.executemany(
            "INSERT INTO NWK_NEW_EHRP_ACTIONS_TBL "
            "(EMPLID, EMPL_RCD, EFFDT, EFFSEQ) "
            "VALUES (:1, :2, :3, :4)",
            batch_new,
        )
    conn.commit()
    logger.info("Inserted 10K EHRP action rows")

    conn.close()
    logger.info("Performance data generation complete")


def setup_cross_db_schema(config: OracleConnectionConfig) -> None:
    """Create a second schema (NATE_USER) for cross-database lookup simulation.

    Simulates the INFO_NATE connection used by EHRP2BIIS_UPDATE for lookups
    against PS_JPM_JP_ITEMS and other tables from a separate database.
    """
    # Connect as SYSTEM to create the second user
    sys_password = os.environ.get("ORACLE_SYS_PASSWORD") or os.environ.get(
        "ORACLE_PASSWORD", ""
    )
    nate_password = os.environ.get("ORACLE_CROSS_DB_PASSWORD") or os.environ.get(
        "ORACLE_PASSWORD", ""
    )
    if not sys_password or not nate_password:
        logger.warning(
            "Cross-DB setup skipped: ORACLE_SYS_PASSWORD/ORACLE_CROSS_DB_PASSWORD not set"
        )
        return

    sys_config = OracleConnectionConfig(
        host=config.host,
        port=config.port,
        service_name=config.service_name,
        username="system",
        password=sys_password,
    )
    conn = oracledb.connect(
        user=sys_config.username,
        password=sys_config.password,
        dsn=sys_config.thin_url,
    )
    cursor = conn.cursor()

    try:
        cursor.execute("DROP USER nate_user CASCADE")
    except Exception:
        pass

    try:
        cursor.execute(
            f"CREATE USER nate_user IDENTIFIED BY {nate_password} "
            "DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS"
        )
        cursor.execute("GRANT CONNECT, RESOURCE TO nate_user")
        cursor.execute("GRANT CREATE SESSION TO nate_user")
        logger.info("Created cross-DB user: nate_user")
    except Exception as exc:
        logger.warning("Could not create nate_user: %s", exc)
        conn.close()
        return

    conn.commit()
    conn.close()

    # Connect as nate_user and create lookup tables
    nate_config = OracleConnectionConfig(
        host=config.host,
        port=config.port,
        service_name=config.service_name,
        username="nate_user",
        password=nate_password,
    )
    conn = oracledb.connect(
        user=nate_config.username,
        password=nate_config.password,
        dsn=nate_config.thin_url,
    )
    cursor = conn.cursor()

    # Create PS_JPM_JP_ITEMS in the cross-DB schema
    cross_db_tables = [
        """CREATE TABLE PS_JPM_JP_ITEMS (
            JPM_PROFILE_ID  VARCHAR2(11),
            JPM_CAT_TYPE    VARCHAR2(4),
            JPM_ITEM_ID     VARCHAR2(20)
        )""",
        """CREATE TABLE PS_GVT_PERS_DATA (
            EMPLID          VARCHAR2(11),
            EMPL_RCD        NUMBER(3,0),
            EFFDT           DATE,
            EFFSEQ          NUMBER(3,0),
            NAME            VARCHAR2(50),
            BIRTHDATE       DATE
        )""",
    ]

    for ddl in cross_db_tables:
        tbl = ddl.split("TABLE")[1].strip().split("(")[0].strip()
        try:
            cursor.execute(f"DROP TABLE {tbl} CASCADE CONSTRAINTS")
        except Exception:
            pass
        cursor.execute(ddl)
        logger.info("Created cross-DB table: nate_user.%s", tbl)

    # Insert matching and non-matching lookup data
    for i in range(40):
        empl = f"E{i:010d}"  # 11 chars: E + 10 digits
        cursor.execute(
            "INSERT INTO PS_JPM_JP_ITEMS (JPM_PROFILE_ID, JPM_CAT_TYPE, JPM_ITEM_ID) "
            "VALUES (:1, :2, :3)",
            [empl, "COMP", f"ITEM{i}"],
        )
        cursor.execute(
            "INSERT INTO PS_GVT_PERS_DATA (EMPLID, EMPL_RCD, EFFDT, EFFSEQ, NAME, BIRTHDATE) "
            "VALUES (:1, :2, :3, :4, :5, :6)",
            [empl, 0, datetime(2025, 6, 1), 0, f"CrossDB Employee {i}",
             datetime(1980, 1, 1) + timedelta(days=i * 100)],
        )

    # Non-matching keys (won't join)
    for i in range(10):
        cursor.execute(
            "INSERT INTO PS_JPM_JP_ITEMS (JPM_PROFILE_ID, JPM_CAT_TYPE, JPM_ITEM_ID) "
            "VALUES (:1, :2, :3)",
            [f"NM{i:09d}", "COMP", f"ITEM_NM{i}"],
        )

    conn.commit()
    conn.close()
    logger.info("Cross-DB schema and data setup complete (nate_user)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Setup Oracle schema and data")
    parser.add_argument(
        "--tier",
        choices=["functional", "performance", "both"],
        default="functional",
        help="Data tier to generate",
    )
    parser.add_argument("--schema-only", action="store_true", help="Only create schema, no data")
    parser.add_argument("--skip-cross-db", action="store_true", help="Skip cross-DB schema setup")
    parser.add_argument(
        "--data-dir",
        default="/tmp/source_files",
        help="Directory for flat file test data",
    )
    args = parser.parse_args()

    config = OracleConnectionConfig()
    logger.info("Connecting to Oracle: %s", config.thin_url)

    create_schema(config)

    if not args.schema_only:
        if args.tier in ("functional", "both"):
            generate_functional_data(config)
            generate_flat_files(args.data_dir)
        if args.tier in ("performance", "both"):
            generate_performance_data(config)

    if not args.skip_cross_db:
        try:
            setup_cross_db_schema(config)
        except Exception as exc:
            logger.warning("Cross-DB setup failed (non-fatal): %s", exc)

    logger.info("Setup complete!")


if __name__ == "__main__":
    main()
