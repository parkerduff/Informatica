"""
Create the BIIS Oracle schema with all tables needed by the 6 ETL jobs.
Populates tables with realistic sample data for performance testing.
"""

import oracledb
import random
import string
from datetime import datetime, timedelta

DSN = "localhost:1521/BIISDB"
USER = "biis_user"
PASSWORD = "BiisUser123"

def get_conn():
    return oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)

def drop_if_exists(cursor, table_name):
    try:
        cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
        print(f"  Dropped existing table: {table_name}")
    except oracledb.DatabaseError:
        pass

def create_tables(cursor):
    """Create all BIIS tables."""
    print("\n=== Creating Tables ===")

    # ---- PAY_PERIOD (Job 1: wf_Pay_Calendar) ----
    drop_if_exists(cursor, "PAY_PERIOD")
    cursor.execute("""
        CREATE TABLE PAY_PERIOD (
            PP_END_YEAR   NUMBER(4),
            PP_NUM        NUMBER(2),
            PP_START_DTE  DATE,
            PP_END_DTE    DATE,
            CURR_PP_FLAG  VARCHAR2(1),
            LV_NUM        NUMBER(2),
            LV_YEAR       NUMBER(4),
            CONSTRAINT pk_pay_period PRIMARY KEY (PP_END_YEAR, PP_NUM)
        )
    """)
    print("  Created: PAY_PERIOD")

    # ---- COMP_TIME_DAILY_TBL (Job 2: wf_COMPTIME) ----
    drop_if_exists(cursor, "COMP_TIME_DAILY_TBL")
    cursor.execute("""
        CREATE TABLE COMP_TIME_DAILY_TBL (
            SSN                VARCHAR2(9),
            NAME               VARCHAR2(30),
            CURRENT_ACCT       VARCHAR2(6),
            CURRENT_ORG        VARCHAR2(7),
            FLSA_STATUS        VARCHAR2(1),
            COMP_TIME_CUR_BAL  NUMBER(8,2),
            COMP_TIME_YEAR_EARNED NUMBER(4),
            PP_END_DATE        DATE,
            DAILY_DATE_EARNED  DATE,
            COMP_TIME_RATE     NUMBER(6,2),
            COMP_TIME_HOURS    NUMBER(8,2),
            COMP_TIME_UNDEF    NUMBER(6),
            PP_END_YEAR        NUMBER(4),
            PP_NUM             NUMBER(2),
            RECORD_TYPE_FLAG   VARCHAR2(10),
            LOAD_DATE          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: COMP_TIME_DAILY_TBL")

    # ---- PSEUDOSSN_TBL (Job 3: m_Pseudossn) ----
    drop_if_exists(cursor, "PSEUDOSSN_TBL")
    cursor.execute("""
        CREATE TABLE PSEUDOSSN_TBL (
            PSEUDO_SSN       VARCHAR2(9),
            HIRE_DATE        DATE,
            EMPLOYEE_NAME    VARCHAR2(50),
            UNIF_ALLOW_AMT   NUMBER(10,2),
            TK_NUM           NUMBER(10),
            LOAD_DATE        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: PSEUDOSSN_TBL")

    # ---- PSEUDOSSN_FROM_SDA_TBL (Job 3 source) ----
    drop_if_exists(cursor, "PSEUDOSSN_FROM_SDA_TBL")
    cursor.execute("""
        CREATE TABLE PSEUDOSSN_FROM_SDA_TBL (
            PSEUDO_SSN       VARCHAR2(9),
            HIRE_DATE        VARCHAR2(10),
            EMPLOYEE_NAME    VARCHAR2(50),
            UNIF_ALLOW_AMT   VARCHAR2(10),
            TK_NUM           NUMBER(10)
        )
    """)
    print("  Created: PSEUDOSSN_FROM_SDA_TBL")

    # ---- CPM_NEWPAY_TBL (Job 4: CPM Extracts) ----
    drop_if_exists(cursor, "CPM_NEWPAY_TBL")
    cursor.execute("""
        CREATE TABLE CPM_NEWPAY_TBL (
            PP_END_YEAR      NUMBER(4),
            PP_NUM           NUMBER(2),
            DFAS_PSEUDO_SSN  VARCHAR2(9),
            LINE_TYPE        VARCHAR2(2),
            AGENCY_CODE      VARCHAR2(10),
            LAST_NAME        VARCHAR2(30),
            FIRST_NAME       VARCHAR2(30),
            SALARY           NUMBER(12,2),
            GRADE            VARCHAR2(5),
            STEP             VARCHAR2(3),
            DUTY_STATION     VARCHAR2(10),
            LOAD_DATE        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: CPM_NEWPAY_TBL")

    # ---- CPM_CYCLE_TBL (Job 4/5) ----
    drop_if_exists(cursor, "CPM_CYCLE_TBL")
    cursor.execute("""
        CREATE TABLE CPM_CYCLE_TBL (
            PROCESS_NAME     VARCHAR2(50),
            PP_END_YEAR      NUMBER(4),
            PP_NUM           NUMBER(2),
            CYCLE_ID         NUMBER(5),
            STATUS           VARCHAR2(20),
            START_TIME       TIMESTAMP,
            END_TIME         TIMESTAMP
        )
    """)
    print("  Created: CPM_CYCLE_TBL")

    # ---- HI_PM_FDA_TATRAN_TBL (Job 5: wf_FDA_Leave) ----
    drop_if_exists(cursor, "HI_PM_FDA_TATRAN_TBL")
    cursor.execute("""
        CREATE TABLE HI_PM_FDA_TATRAN_TBL (
            FDA_EMP_ID       VARCHAR2(20),
            FDA_REC_TYPE     VARCHAR2(2),
            EFFECTIVE_DATE   DATE,
            AMOUNT           NUMBER(12,2),
            PP_END_YEAR      NUMBER(4),
            PP_NUM           NUMBER(2),
            CYCLE_ID         NUMBER(5),
            LOAD_DATE        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: HI_PM_FDA_TATRAN_TBL")

    # ---- ERROR_TBL (Jobs 2,4,5,6) ----
    drop_if_exists(cursor, "ERROR_TBL")
    cursor.execute("""
        CREATE TABLE ERROR_TBL (
            PROCESS_NAME     VARCHAR2(100),
            ERROR_MESSAGE    VARCHAR2(500),
            SOURCE_KEY       VARCHAR2(100),
            ERROR_DATE       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PP_END_YEAR      NUMBER(4),
            PP_NUM           NUMBER(2),
            CYCLE_ID         NUMBER(5)
        )
    """)
    print("  Created: ERROR_TBL")

    # ---- COUNTER_TBL (Jobs 2,4,5,6) ----
    drop_if_exists(cursor, "COUNTER_TBL")
    cursor.execute("""
        CREATE TABLE COUNTER_TBL (
            RUN_DATE            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PROCESS_NAME        VARCHAR2(100),
            COUNTER_DESCRIPTION VARCHAR2(200),
            COUNTER_VALUE       NUMBER(15),
            PP_END_YEAR         NUMBER(4),
            PP_NUM              NUMBER(2),
            CYCLE_ID            NUMBER(5)
        )
    """)
    print("  Created: COUNTER_TBL")

    # ---- PS_GVT_JOB (Job 6: wf_EHRP2BIIS_UPDATE source) ----
    drop_if_exists(cursor, "PS_GVT_JOB")
    cursor.execute("""
        CREATE TABLE PS_GVT_JOB (
            EMPLID           VARCHAR2(11),
            EMPL_RCD         NUMBER(3),
            EFFDT            DATE,
            EFFSEQ           NUMBER(3),
            ACTION           VARCHAR2(3),
            ACTION_REASON    VARCHAR2(3),
            DEPTID           VARCHAR2(10),
            POSITION_NBR     VARCHAR2(8),
            GVT_COMPRATE     NUMBER(12,2),
            JOBCODE          VARCHAR2(6),
            LOCATION         VARCHAR2(10),
            GVT_PAY_PLAN     VARCHAR2(2),
            GRADE            VARCHAR2(5),
            STEP             VARCHAR2(3)
        )
    """)
    print("  Created: PS_GVT_JOB")

    # ---- NWK_NEW_EHRP_ACTIONS_TBL (Job 6 source) ----
    drop_if_exists(cursor, "NWK_NEW_EHRP_ACTIONS_TBL")
    cursor.execute("""
        CREATE TABLE NWK_NEW_EHRP_ACTIONS_TBL (
            EMPLID           VARCHAR2(11),
            EMPL_RCD         NUMBER(3),
            EFFDT            DATE,
            EFFSEQ           NUMBER(3),
            PROCESSED_FLAG   VARCHAR2(1) DEFAULT 'N',
            CREATED_DATE     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: NWK_NEW_EHRP_ACTIONS_TBL")

    # ---- NWK_ACTION_PRIMARY_TBL (Job 6 target) ----
    drop_if_exists(cursor, "NWK_ACTION_PRIMARY_TBL")
    cursor.execute("""
        CREATE TABLE NWK_ACTION_PRIMARY_TBL (
            EMPLID           VARCHAR2(11),
            EMPL_RCD         NUMBER(3),
            EFFDT            DATE,
            EFFSEQ           NUMBER(3),
            ACTION           VARCHAR2(3),
            ACTION_REASON    VARCHAR2(3),
            DEPTID           VARCHAR2(10),
            GVT_COMPRATE     NUMBER(12,2),
            LOAD_DATE        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: NWK_ACTION_PRIMARY_TBL")

    # ---- NWK_ACTION_SECONDARY_TBL (Job 6 target) ----
    drop_if_exists(cursor, "NWK_ACTION_SECONDARY_TBL")
    cursor.execute("""
        CREATE TABLE NWK_ACTION_SECONDARY_TBL (
            EMPLID           VARCHAR2(11),
            EMPL_RCD         NUMBER(3),
            EFFDT            DATE,
            EFFSEQ           NUMBER(3),
            POSITION_NBR     VARCHAR2(8),
            JOBCODE          VARCHAR2(6),
            LOCATION         VARCHAR2(10),
            GVT_PAY_PLAN     VARCHAR2(2),
            GRADE            VARCHAR2(5),
            STEP             VARCHAR2(3),
            LOAD_DATE        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: NWK_ACTION_SECONDARY_TBL")

    # ---- EHRP_RECS_TRACKING_TBL (Job 6 target) ----
    drop_if_exists(cursor, "EHRP_RECS_TRACKING_TBL")
    cursor.execute("""
        CREATE TABLE EHRP_RECS_TRACKING_TBL (
            EMPLID           VARCHAR2(11),
            EMPL_RCD         NUMBER(3),
            EFFDT            DATE,
            EFFSEQ           NUMBER(3),
            PROCESSED_FLAG   VARCHAR2(1),
            PROCESS_DATE     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created: EHRP_RECS_TRACKING_TBL")

    # ---- Lookup tables for Job 6 ----
    lookup_tables = [
        ("PS_GVT_EMPLOYMENT", "EMPLID VARCHAR2(11), EMPL_RCD NUMBER(3), GVT_TENURE VARCHAR2(1), GVT_ANNUIT_IND VARCHAR2(1)"),
        ("PS_GVT_PERS_NID", "EMPLID VARCHAR2(11), NATIONAL_ID VARCHAR2(20), COUNTRY VARCHAR2(3)"),
        ("PS_GVT_AWD_DATA", "EMPLID VARCHAR2(11), EMPL_RCD NUMBER(3), EFFDT DATE, GVT_AWD_TYPE VARCHAR2(3), GVT_AWD_AMT NUMBER(12,2)"),
        ("PS_GVT_EE_DATA_TRK", "EMPLID VARCHAR2(11), EMPL_RCD NUMBER(3), EFFDT DATE, GVT_DATA_TRK_FLD VARCHAR2(20)"),
        ("PS_HE_FILL_POS", "POSITION_NBR VARCHAR2(8), FILL_STATUS VARCHAR2(1), FILL_DATE DATE"),
        ("PS_GVT_CITIZENSHIP", "EMPLID VARCHAR2(11), COUNTRY VARCHAR2(3), CITIZENSHIP_STATUS VARCHAR2(1)"),
        ("PS_GVT_PERS_DATA", "EMPLID VARCHAR2(11), SEX VARCHAR2(1), BIRTHDATE DATE, GVT_VETERAN_STATUS VARCHAR2(2)"),
        ("PS_JPM_JP_ITEMS", "EMPLID VARCHAR2(11), EMPL_RCD NUMBER(3), JPM_ITEM VARCHAR2(20), JPM_ITEM_STATUS VARCHAR2(1)"),
    ]
    for tbl_name, cols in lookup_tables:
        drop_if_exists(cursor, tbl_name)
        cursor.execute(f"CREATE TABLE {tbl_name} ({cols})")
        print(f"  Created: {tbl_name}")


def load_sample_data(cursor):
    """Load realistic sample data into all tables."""
    print("\n=== Loading Sample Data ===")

    # ---- PAY_PERIOD: 26 pay periods for 2025 ----
    base_date = datetime(2025, 1, 4)
    for pp in range(1, 27):
        start = base_date + timedelta(days=(pp - 1) * 14)
        end = start + timedelta(days=13)
        flag = 'Y' if pp == 5 else None
        cursor.execute(
            "INSERT INTO PAY_PERIOD (PP_END_YEAR, PP_NUM, PP_START_DTE, PP_END_DTE, CURR_PP_FLAG, LV_NUM, LV_YEAR) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7)",
            [2025, pp, start, end, flag, pp, 2025]
        )
    print(f"  PAY_PERIOD: 26 rows loaded")

    # ---- PSEUDOSSN_FROM_SDA_TBL: 500 source records ----
    for i in range(500):
        ssn = f"{100000000 + i}"
        # Mix of valid and invalid dates
        if i < 400:
            hire_date = f"{(i%12)+1:02d}{(i%28)+1:02d}2024"
        else:
            hire_date = "INVALID!!"
        sign = "+" if i % 3 != 0 else "-"
        amt = f"{(i*10+50):05d}{sign}"
        name = f"EMPLOYEE_{i:04d}"
        cursor.execute(
            "INSERT INTO PSEUDOSSN_FROM_SDA_TBL (PSEUDO_SSN, HIRE_DATE, EMPLOYEE_NAME, UNIF_ALLOW_AMT, TK_NUM) "
            "VALUES (:1, :2, :3, :4, :5)",
            [ssn, hire_date, name, amt, i + 1]
        )
    print(f"  PSEUDOSSN_FROM_SDA_TBL: 500 rows loaded")

    # ---- CPM_NEWPAY_TBL: 1000 records across 3 agencies ----
    agencies = ["NIH", "CDC", "OIG"]
    grades = ["GS-07", "GS-09", "GS-11", "GS-12", "GS-13", "GS-14", "GS-15"]
    for i in range(1000):
        agency = agencies[i % 3]
        cursor.execute(
            "INSERT INTO CPM_NEWPAY_TBL (PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, LINE_TYPE, AGENCY_CODE, "
            "LAST_NAME, FIRST_NAME, SALARY, GRADE, STEP, DUTY_STATION) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
            [2025, 5, f"SSN{i:06d}", "01", agency,
             f"LAST_{i:04d}", f"FIRST_{i:04d}",
             round(40000 + i * 75.50, 2),
             random.choice(grades), str(random.randint(1, 10)),
             f"DC{i%100:03d}"]
        )
    print(f"  CPM_NEWPAY_TBL: 1000 rows loaded")

    # ---- CPM_CYCLE_TBL ----
    cursor.execute(
        "INSERT INTO CPM_CYCLE_TBL (PROCESS_NAME, PP_END_YEAR, PP_NUM, CYCLE_ID, STATUS, START_TIME) "
        "VALUES ('FDA', 2025, 5, 1, 'ACTIVE', CURRENT_TIMESTAMP)"
    )
    print(f"  CPM_CYCLE_TBL: 1 row loaded")

    # ---- HI_PM_FDA_TATRAN_TBL: 800 FDA records ----
    for i in range(800):
        emp_id = f"FDA_EMP_{i:04d}"
        # Mix of rec types: 02 (leave), 12 (master), 01 (other)
        if i < 500:
            rec_type = "02"
        elif i < 700:
            rec_type = "12"
        else:
            rec_type = "01"
        eff_date = datetime(2025, 3, 1) + timedelta(days=i % 28)
        cursor.execute(
            "INSERT INTO HI_PM_FDA_TATRAN_TBL (FDA_EMP_ID, FDA_REC_TYPE, EFFECTIVE_DATE, AMOUNT, "
            "PP_END_YEAR, PP_NUM, CYCLE_ID) VALUES (:1, :2, :3, :4, :5, :6, :7)",
            [emp_id, rec_type, eff_date, round(100 + i * 10.5, 2), 2025, 5, 1]
        )
    print(f"  HI_PM_FDA_TATRAN_TBL: 800 rows loaded")

    # ---- PS_GVT_JOB + NWK_NEW_EHRP_ACTIONS_TBL: 300 records ----
    actions = ["HIR", "PRO", "PAY", "SEP", "RET", "DEM"]
    for i in range(300):
        emplid = f"EMP{i:07d}"
        empl_rcd = i % 3
        effdt = datetime(2025, 3, 1) + timedelta(days=i % 60)
        effseq = i % 5
        action = random.choice(actions)
        deptid = f"DEPT_{i%20:03d}"
        position = f"POS{i:05d}"
        comprate = round(40000 + random.uniform(0, 80000), 2)

        cursor.execute(
            "INSERT INTO PS_GVT_JOB (EMPLID, EMPL_RCD, EFFDT, EFFSEQ, ACTION, ACTION_REASON, "
            "DEPTID, POSITION_NBR, GVT_COMPRATE, JOBCODE, LOCATION, GVT_PAY_PLAN, GRADE, STEP) "
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)",
            [emplid, empl_rcd, effdt, effseq, action, "01",
             deptid, position, comprate, f"JC{i%99:04d}",
             f"LOC{i%50:03d}", "GS", random.choice(grades), str(random.randint(1, 10))]
        )

        # Only 200 have pending actions (unprocessed)
        if i < 200:
            cursor.execute(
                "INSERT INTO NWK_NEW_EHRP_ACTIONS_TBL (EMPLID, EMPL_RCD, EFFDT, EFFSEQ, PROCESSED_FLAG) "
                "VALUES (:1, :2, :3, :4, 'N')",
                [emplid, empl_rcd, effdt, effseq]
            )
    print(f"  PS_GVT_JOB: 300 rows loaded")
    print(f"  NWK_NEW_EHRP_ACTIONS_TBL: 200 rows loaded")

    # ---- Lookup tables for Job 6 ----
    for i in range(300):
        emplid = f"EMP{i:07d}"
        empl_rcd = i % 3
        effdt = datetime(2025, 3, 1) + timedelta(days=i % 60)

        cursor.execute("INSERT INTO PS_GVT_EMPLOYMENT (EMPLID, EMPL_RCD, GVT_TENURE, GVT_ANNUIT_IND) VALUES (:1, :2, :3, :4)",
                       [emplid, empl_rcd, str(random.randint(0, 3)), random.choice(["Y", "N"])])
        cursor.execute("INSERT INTO PS_GVT_PERS_NID (EMPLID, NATIONAL_ID, COUNTRY) VALUES (:1, :2, :3)",
                       [emplid, f"NID{i:08d}", "USA"])
        cursor.execute("INSERT INTO PS_GVT_AWD_DATA (EMPLID, EMPL_RCD, EFFDT, GVT_AWD_TYPE, GVT_AWD_AMT) VALUES (:1, :2, :3, :4, :5)",
                       [emplid, empl_rcd, effdt, random.choice(["QSI", "SCA", "PCA"]), round(random.uniform(100, 5000), 2)])
        cursor.execute("INSERT INTO PS_GVT_EE_DATA_TRK (EMPLID, EMPL_RCD, EFFDT, GVT_DATA_TRK_FLD) VALUES (:1, :2, :3, :4)",
                       [emplid, empl_rcd, effdt, f"TRK_{i%10}"])
        cursor.execute("INSERT INTO PS_GVT_CITIZENSHIP (EMPLID, COUNTRY, CITIZENSHIP_STATUS) VALUES (:1, :2, :3)",
                       [emplid, "USA", random.choice(["C", "N", "R"])])
        cursor.execute("INSERT INTO PS_GVT_PERS_DATA (EMPLID, SEX, BIRTHDATE, GVT_VETERAN_STATUS) VALUES (:1, :2, :3, :4)",
                       [emplid, random.choice(["M", "F"]), datetime(1960 + random.randint(0, 40), random.randint(1, 12), random.randint(1, 28)),
                        random.choice(["VE", "NV", "DV"])])
        cursor.execute("INSERT INTO PS_JPM_JP_ITEMS (EMPLID, EMPL_RCD, JPM_ITEM, JPM_ITEM_STATUS) VALUES (:1, :2, :3, :4)",
                       [emplid, empl_rcd, f"JPM_{i%15:03d}", random.choice(["A", "I"])])

    for i in range(300):
        position = f"POS{i:05d}"
        cursor.execute("INSERT INTO PS_HE_FILL_POS (POSITION_NBR, FILL_STATUS, FILL_DATE) VALUES (:1, :2, :3)",
                       [position, random.choice(["F", "V"]), datetime(2024, 6, 1) + timedelta(days=random.randint(0, 365))])

    print(f"  Lookup tables: 300 rows each (8 tables)")


def verify_data(cursor):
    """Verify all tables have data."""
    print("\n=== Verification ===")
    tables = [
        "PAY_PERIOD", "COMP_TIME_DAILY_TBL", "PSEUDOSSN_TBL",
        "PSEUDOSSN_FROM_SDA_TBL", "CPM_NEWPAY_TBL", "CPM_CYCLE_TBL",
        "HI_PM_FDA_TATRAN_TBL", "ERROR_TBL", "COUNTER_TBL",
        "PS_GVT_JOB", "NWK_NEW_EHRP_ACTIONS_TBL",
        "NWK_ACTION_PRIMARY_TBL", "NWK_ACTION_SECONDARY_TBL",
        "EHRP_RECS_TRACKING_TBL",
        "PS_GVT_EMPLOYMENT", "PS_GVT_PERS_NID", "PS_GVT_AWD_DATA",
        "PS_GVT_EE_DATA_TRK", "PS_HE_FILL_POS", "PS_GVT_CITIZENSHIP",
        "PS_GVT_PERS_DATA", "PS_JPM_JP_ITEMS",
    ]
    total = 0
    for tbl in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
        count = cursor.fetchone()[0]
        total += count
        print(f"  {tbl}: {count} rows")
    print(f"\n  TOTAL: {total} rows across {len(tables)} tables")


def main():
    print("=" * 60)
    print("BIIS Oracle Schema Setup")
    print(f"Database: {DSN}")
    print(f"User: {USER}")
    print("=" * 60)

    conn = get_conn()
    cursor = conn.cursor()

    try:
        create_tables(cursor)
        conn.commit()

        load_sample_data(cursor)
        conn.commit()

        verify_data(cursor)
    finally:
        cursor.close()
        conn.close()

    print("\n=== Schema setup complete ===")


if __name__ == "__main__":
    main()
