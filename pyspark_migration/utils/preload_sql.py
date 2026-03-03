"""
Pre-load and post-load SQL steps.

Replaces the KSH shell scripts:
  - ehrp2biis_preload   (calls step01 via SQL*Plus)
  - actstage_load       (calls action_stage_load via SQL*Plus)
  - ehrp2biis_afterload.sql (post-ETL stored procedures and DML)

All operations use cx_Oracle through the helpers in ``utils.db``.
"""

import logging

from pyspark_migration.utils.db import (
    get_tgt_cx_connection,
    execute_sql,
    call_procedure,
)
from pyspark_migration.utils.email import (
    send_success_email,
    send_failure_email,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ehrp2biis_preload  (replaces the KSH script)
# ---------------------------------------------------------------------------

def run_preload_sql() -> None:
    """Execute the EHRP2BIIS pre-load stored procedure (step01).

    The original KSH script:
      1. Reads credentials from $HOME/.use1 and $HOME/.pw1
      2. Calls SQL*Plus to execute ``step01``
      3. Sends email on success/failure

    This function reproduces the same behaviour using cx_Oracle.
    """
    conn = get_tgt_cx_connection()
    try:
        logger.info("Starting EHRP2BIIS preload (step01)")
        call_procedure(conn, "step01")
        send_success_email("EHRP2BIIS Preload script")
        logger.info("EHRP2BIIS preload completed successfully")
    except Exception as exc:
        logger.error("EHRP2BIIS preload failed: %s", exc)
        send_failure_email("EHRP2BIIS Preload script", str(exc))
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# actstage_load  (replaces the KSH script)
# ---------------------------------------------------------------------------

def run_action_stage_load() -> None:
    """Execute the Action Staging Records load procedure.

    The original KSH script calls ``action_stage_load`` via SQL*Plus
    and checks for 'SUCCESS!!!' in the spool output.
    """
    conn = get_tgt_cx_connection()
    try:
        logger.info("Starting Action Staging Records load")
        call_procedure(conn, "action_stage_load")
        send_success_email("Action Staging Records load")
        logger.info("Action Staging Records load completed successfully")
    except Exception as exc:
        logger.error("Action Staging Records load failed: %s", exc)
        send_failure_email("Action Staging Records load", str(exc))
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# ehrp2biis_afterload  (replaces ehrp2biis_afterload.sql)
# ---------------------------------------------------------------------------

def run_afterload_sql() -> None:
    """Execute all post-load SQL steps from ehrp2biis_afterload.sql.

    Steps (matching the original SQL script numbering):
      Step 04 - Null out retained step codes with value '0.0000000000000'
      Step 05 - Update sequence numbers, run formatting procedures,
                sync to historical tables, handle cancelled actions,
                truncate staging table.
    """
    conn = get_tgt_cx_connection()
    try:
        logger.info("Starting EHRP2BIIS afterload processing")

        # Step 04 — Update retained step codes
        execute_sql(conn, """
            UPDATE nknight.nwk_action_secondary_tbl a
            SET a.retnd1_step_cd = NULL
            WHERE a.event_id IN (
                SELECT b.event_id
                FROM nknight.nwk_action_primary_tbl b
                WHERE b.load_date = TRUNC(SYSDATE)
                  AND b.event_id < 9000000000
            )
            AND a.retnd1_step_cd = '0.0000000000000'
        """)

        # Step 05 — Compile and execute sequence number update
        execute_sql(conn, "ALTER PROCEDURE update_sequence_number_tbl_p COMPILE")
        call_procedure(conn, "update_sequence_number_tbl_p")

        # Run four formatting procedures
        call_procedure(conn, "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P")
        call_procedure(conn, "HISTDBA.UPDATE_ERP2BIIS_NO900S01_p")
        call_procedure(conn, "HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01")
        call_procedure(conn, "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P")

        # Handle cancelled/updated WIP status records
        call_procedure(conn, "HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P")

        # Update PROCESS_TABLE — set P_STARTDT to NULL, then recalculate
        execute_sql(conn, "UPDATE PROCESS_TABLE SET P_STARTDT = NULL")

        execute_sql(conn, """
            UPDATE PROCESS_TABLE
            SET P_STARTDT = (
                SELECT effdt FROM (
                    SELECT a.biis_event_id, a.emplid, a.empl_rcd, a.effdt,
                           a.effseq, b.deptid,
                           a.gvt_wip_status, b.gvt_wip_status
                    FROM nknight.ehrp_recs_tracking_tbl a, ehrp.ps_gvt_job b
                    WHERE a.emplid = b.emplid
                      AND a.empl_rcd = b.empl_rcd
                      AND a.effdt = b.effdt
                      AND a.effseq = b.effseq
                      AND a.gvt_wip_status <> b.gvt_wip_status
                      AND a.changed_wip_status IS NULL
                    ORDER BY 4, 1
                ) WHERE ROWNUM < 2
            )
        """)

        # If P_STARTDT is still NULL, set to far future
        execute_sql(conn, """
            UPDATE PROCESS_TABLE
            SET P_STARTDT = TRUNC(SYSDATE) + 10000
            WHERE P_STARTDT IS NULL
        """)

        # Compile and run WIP status check procedure
        execute_sql(conn, "ALTER PROCEDURE chk_ehrp2biis_wip_status_p COMPILE")
        call_procedure(conn, "chk_ehrp2biis_wip_status_p")

        # Sync today's load into historical tables
        execute_sql(conn, """
            INSERT INTO action_primary_all
            SELECT * FROM nknight.nwk_action_primary_tbl
            WHERE load_date = TRUNC(SYSDATE)
        """)

        execute_sql(conn, """
            INSERT INTO action_secondary_all
            SELECT * FROM nknight.nwk_action_secondary_tbl
            WHERE event_id IN (
                SELECT a.event_id FROM nknight.nwk_action_primary_tbl a
                WHERE a.load_date = TRUNC(SYSDATE)
            )
        """)

        execute_sql(conn, """
            INSERT INTO action_remarks_all
            SELECT * FROM nknight.nwk_action_remarks_tbl
            WHERE event_id IN (
                SELECT a.event_id FROM nknight.nwk_action_primary_tbl a
                WHERE a.load_date = TRUNC(SYSDATE)
            )
        """)

        # Gather run counts
        call_procedure(conn, "HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P", [None])

        # Handle cancelled actions — delete old, re-insert current
        _handle_cancelled_actions(conn)

        # Truncate staging table before next load
        execute_sql(conn, "TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl")

        logger.info("EHRP2BIIS afterload completed successfully")

    except Exception as exc:
        logger.error("EHRP2BIIS afterload failed: %s", exc)
        raise
    finally:
        conn.close()


def _handle_cancelled_actions(conn) -> None:
    """Delete and re-insert records for actions whose WIP status changed today."""

    # Delete from secondary, remarks, then primary (order matters for FK)
    execute_sql(conn, """
        DELETE FROM action_secondary_all
        WHERE event_id IN (
            SELECT b.biis_event_id FROM nknight.ehrp_recs_tracking_tbl b
            WHERE b.biis_wip_status_changed_dt = TRUNC(SYSDATE)
        )
    """)

    execute_sql(conn, """
        DELETE FROM action_remarks_all
        WHERE event_id IN (
            SELECT b.biis_event_id FROM nknight.ehrp_recs_tracking_tbl b
            WHERE b.biis_wip_status_changed_dt = TRUNC(SYSDATE)
        )
    """)

    execute_sql(conn, """
        DELETE FROM action_primary_all
        WHERE event_id IN (
            SELECT b.biis_event_id FROM nknight.ehrp_recs_tracking_tbl b
            WHERE b.biis_wip_status_changed_dt = TRUNC(SYSDATE)
        )
    """)

    # Re-insert current versions
    execute_sql(conn, """
        INSERT INTO action_primary_all
        SELECT * FROM nknight.nwk_action_primary_tbl
        WHERE event_id IN (
            SELECT b.biis_event_id FROM nknight.ehrp_recs_tracking_tbl b
            WHERE b.biis_wip_status_changed_dt = TRUNC(SYSDATE)
        )
    """)

    execute_sql(conn, """
        INSERT INTO action_secondary_all
        SELECT * FROM nknight.nwk_action_secondary_tbl
        WHERE event_id IN (
            SELECT b.biis_event_id FROM nknight.ehrp_recs_tracking_tbl b
            WHERE b.biis_wip_status_changed_dt = TRUNC(SYSDATE)
        )
    """)

    execute_sql(conn, """
        INSERT INTO action_remarks_all
        SELECT * FROM nknight.nwk_action_remarks_tbl
        WHERE event_id IN (
            SELECT b.biis_event_id FROM nknight.ehrp_recs_tracking_tbl b
            WHERE b.biis_wip_status_changed_dt = TRUNC(SYSDATE)
        )
    """)
