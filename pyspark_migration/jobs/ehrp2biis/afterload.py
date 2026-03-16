"""
EHRP2BIIS Afterload

Replaces ehrp2biis_afterload.sql (289 lines) and actstage_load shell script.
Implements each step sequentially via db_utils.execute_sql().

Steps:
1. Retained step cleanup (lines 9-17)
2. Sequence number update (lines 44-52)
3. Execute 5 formatting procedures (lines 59-74)
4. WIP status change detection (lines 107-142)
5. Historical table inserts (lines 154-185)
6. Cancelled action delete-and-reinsert (lines 207-282)
7. Truncate staging (lines 286-287)
"""

import argparse
import glob
import logging
import os

from pyspark_migration.common.db_utils import (
    compile_procedure,
    execute_procedure,
    execute_procedure_with_args,
    execute_sql,
    truncate_table,
)
from pyspark_migration.common.notification import (
    send_failure_email,
    send_success_email,
)
from pyspark_migration.config.settings import FILE_PATHS, SCHEMAS

logger = logging.getLogger(__name__)


def step_retained_step_cleanup(connection_name="ORA_BIIS"):
    """
    Step 1: Clean up retained step codes.

    Replaces ehrp2biis_afterload.sql lines 9-17:
        UPDATE nwk_action_secondary_tbl SET retnd1_step_cd = NULL
        WHERE event_id IN (SELECT event_id FROM nwk_action_primary_tbl
                           WHERE load_date = trunc(sysdate) AND event_id < 9000000000)
        AND retnd1_step_cd = '0.0000000000000'

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 1: Retained step cleanup")
    sql = """
        UPDATE NKNIGHT.NWK_ACTION_SECONDARY_TBL a
        SET a.RETND1_STEP_CD = NULL
        WHERE a.EVENT_ID IN (
            SELECT b.EVENT_ID
            FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL b
            WHERE b.LOAD_DATE = TRUNC(SYSDATE)
              AND b.EVENT_ID < 9000000000
        )
        AND a.RETND1_STEP_CD = '0.0000000000000'
    """
    rows = execute_sql(connection_name, sql)
    logger.info("Retained step cleanup: %d rows updated", rows)


def step_sequence_number_update(connection_name="ORA_BIIS"):
    """
    Step 2: Compile and execute sequence number update procedure.

    Replaces ehrp2biis_afterload.sql lines 44-52:
        ALTER PROCEDURE update_sequence_number_tbl_p COMPILE;
        EXECUTE update_sequence_number_tbl_p;

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 2: Sequence number update")
    compile_procedure(connection_name, "UPDATE_SEQUENCE_NUMBER_TBL_P")
    execute_procedure(connection_name, "UPDATE_SEQUENCE_NUMBER_TBL_P")
    logger.info("Sequence number update complete")


def step_formatting_procedures(connection_name="ORA_BIIS"):
    """
    Step 3: Execute 5 formatting stored procedures.

    Replaces ehrp2biis_afterload.sql lines 59-74:
        EXEC HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P
        EXEC HISTDBA.UPDATE_ERP2BIIS_NO900S01_p
        EXEC HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01
        EXEC HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P
        EXEC HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 3: Executing formatting procedures")

    procedures = [
        "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P",
        "HISTDBA.UPDATE_ERP2BIIS_NO900S01_P",
        "HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01",
        "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P",
        "HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P",
    ]

    for proc in procedures:
        logger.info("Executing procedure: %s", proc)
        execute_procedure(connection_name, proc)
        logger.info("Procedure %s completed", proc)

    logger.info("All formatting procedures complete")


def step_wip_status_change(connection_name="ORA_BIIS"):
    """
    Step 4: WIP status change detection.

    Replaces ehrp2biis_afterload.sql lines 107-142:
    - Set PROCESS_TABLE.P_STARTDT = NULL
    - Find earliest effdt from changed WIP status records
    - If no changes, set P_STARTDT = trunc(sysdate) + 10000
    - Compile and execute chk_ehrp2biis_wip_status_p

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 4: WIP status change detection")

    # Reset P_STARTDT
    execute_sql(
        connection_name,
        "UPDATE PROCESS_TABLE SET P_STARTDT = NULL",
    )

    # Set P_STARTDT to earliest effdt where WIP status differs
    wip_sql = """
        UPDATE PROCESS_TABLE SET P_STARTDT = (
            SELECT EFFDT FROM (
                SELECT a.BIIS_EVENT_ID, a.EMPLID, a.EMPL_RCD,
                       a.EFFDT, a.EFFSEQ, b.DEPTID,
                       a.GVT_WIP_STATUS, b.GVT_WIP_STATUS AS NEW_STATUS
                FROM NKNIGHT.EHRP_RECS_TRACKING_TBL a,
                     EHRP.PS_GVT_JOB b
                WHERE a.EMPLID = b.EMPLID
                  AND a.EMPL_RCD = b.EMPL_RCD
                  AND a.EFFDT = b.EFFDT
                  AND a.EFFSEQ = b.EFFSEQ
                  AND a.GVT_WIP_STATUS <> b.GVT_WIP_STATUS
                  AND a.CHANGED_WIP_STATUS IS NULL
                ORDER BY a.EFFDT, a.BIIS_EVENT_ID
            ) WHERE ROWNUM < 2
        )
    """
    execute_sql(connection_name, wip_sql)

    # If no changes found, set far-future date
    execute_sql(
        connection_name,
        "UPDATE PROCESS_TABLE SET P_STARTDT = TRUNC(SYSDATE) + 10000 "
        "WHERE P_STARTDT IS NULL",
    )

    # Compile and execute WIP status check procedure
    compile_procedure(connection_name, "CHK_EHRP2BIIS_WIP_STATUS_P")
    execute_procedure(connection_name, "CHK_EHRP2BIIS_WIP_STATUS_P")

    logger.info("WIP status change detection complete")


def step_historical_inserts(connection_name="ORA_BIIS"):
    """
    Step 5: Insert today's records into historical tables.

    Replaces ehrp2biis_afterload.sql lines 154-185:
        INSERT INTO action_primary_all SELECT * FROM nwk_action_primary_tbl WHERE load_date = today
        INSERT INTO action_secondary_all (joined via event_id to today's primary)
        INSERT INTO action_remarks_all (joined via event_id to today's primary)

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 5: Historical table inserts")

    # Insert primary records
    execute_sql(
        connection_name,
        "INSERT INTO ACTION_PRIMARY_ALL "
        "SELECT * FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL "
        "WHERE LOAD_DATE = TRUNC(SYSDATE)",
    )

    # Insert secondary records (joined via event_id)
    execute_sql(
        connection_name,
        "INSERT INTO ACTION_SECONDARY_ALL "
        "SELECT c.* FROM NKNIGHT.NWK_ACTION_SECONDARY_TBL c "
        "WHERE c.EVENT_ID IN ("
        "  SELECT a.EVENT_ID FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL a "
        "  WHERE a.LOAD_DATE = TRUNC(SYSDATE)"
        ")",
    )

    # Insert remarks records (joined via event_id)
    execute_sql(
        connection_name,
        "INSERT INTO ACTION_REMARKS_ALL "
        "SELECT c.* FROM NKNIGHT.NWK_ACTION_REMARKS_TBL c "
        "WHERE c.EVENT_ID IN ("
        "  SELECT a.EVENT_ID FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL a "
        "  WHERE a.LOAD_DATE = TRUNC(SYSDATE)"
        ")",
    )

    # Gather run counts
    execute_procedure_with_args(
        connection_name,
        "HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P",
        args=[None],
    )

    logger.info("Historical table inserts complete")


def step_cancelled_actions(connection_name="ORA_BIIS"):
    """
    Step 6: Delete and reinsert cancelled actions.

    Replaces ehrp2biis_afterload.sql lines 207-282:
    For records where biis_wip_status_changed_dt = today:
    - DELETE from action_secondary_all, action_remarks_all, action_primary_all
    - INSERT fresh copies from nwk_action_* tables

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 6: Cancelled action delete-and-reinsert")

    # Common subquery for cancelled event IDs
    cancelled_subquery = (
        "SELECT B.BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL B "
        "WHERE B.BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)"
    )

    # Delete old versions from historical tables (order matters for FK)
    execute_sql(
        connection_name,
        f"DELETE FROM ACTION_SECONDARY_ALL "
        f"WHERE EVENT_ID IN ({cancelled_subquery})",
    )

    execute_sql(
        connection_name,
        f"DELETE FROM ACTION_REMARKS_ALL "
        f"WHERE EVENT_ID IN ({cancelled_subquery})",
    )

    execute_sql(
        connection_name,
        f"DELETE FROM ACTION_PRIMARY_ALL "
        f"WHERE EVENT_ID IN ({cancelled_subquery})",
    )

    # Reinsert current versions
    execute_sql(
        connection_name,
        f"INSERT INTO ACTION_PRIMARY_ALL "
        f"SELECT * FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL "
        f"WHERE EVENT_ID IN ({cancelled_subquery})",
    )

    execute_sql(
        connection_name,
        f"INSERT INTO ACTION_SECONDARY_ALL "
        f"SELECT * FROM NKNIGHT.NWK_ACTION_SECONDARY_TBL "
        f"WHERE EVENT_ID IN ({cancelled_subquery})",
    )

    execute_sql(
        connection_name,
        f"INSERT INTO ACTION_REMARKS_ALL "
        f"SELECT * FROM NKNIGHT.NWK_ACTION_REMARKS_TBL "
        f"WHERE EVENT_ID IN ({cancelled_subquery})",
    )

    logger.info("Cancelled action processing complete")


def step_truncate_staging(connection_name="ORA_BIIS"):
    """
    Step 7: Truncate staging table.

    Replaces ehrp2biis_afterload.sql lines 286-287:
        TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 7: Truncating staging table")
    truncate_table(connection_name, "NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL")
    logger.info("Staging table truncated")


def run_action_stage_load(connection_name="ORA_BIIS"):
    """
    Execute the action_stage_load SQL.

    Replaces actstage_load shell script which runs:
        @ $homedir/action_stage_load

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Executing action_stage_load")
    try:
        execute_sql(
            connection_name,
            "BEGIN NKNIGHT.ACTION_STAGE_LOAD_P; END;",
        )
        send_success_email(
            "Action Staging Records load",
            "Action Staging Load SQL Script Ran Successfully",
        )
        logger.info("Action stage load completed successfully")
    except Exception:
        logger.exception("Action stage load failed")
        send_failure_email(
            "Action Staging Records load script",
            "Action stage load did not complete successfully",
        )
        raise


def run(environment=None):
    """
    Execute the full EHRP2BIIS afterload workflow.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting EHRP2BIIS Afterload")
    logger.info("=" * 60)

    try:
        step_retained_step_cleanup()
        step_sequence_number_update()
        step_formatting_procedures()
        step_wip_status_change()
        step_historical_inserts()
        step_cancelled_actions()
        step_truncate_staging()

        # Also run action stage load
        run_action_stage_load()

        logger.info("EHRP2BIIS Afterload completed successfully")

    except Exception:
        logger.exception("EHRP2BIIS Afterload failed")
        send_failure_email("EHRP2BIIS Afterload", "Afterload failed - check logs")
        raise


def main():
    """CLI entry point for EHRP2BIIS afterload."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS EHRP2BIIS Afterload")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
