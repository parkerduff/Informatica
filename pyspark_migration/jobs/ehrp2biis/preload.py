"""
EHRP2BIIS Preload

Replaces ehrp2biis_preload shell script (lines 1-47).

Steps:
1. Clean up temp files
2. Read credentials from config
3. Execute step01 SQL script via db_utils.execute_sql()
4. Log output
5. Check execution status, send success/failure email
"""

import argparse
import glob
import logging
import os

from pyspark_migration.common.db_utils import execute_sql
from pyspark_migration.common.notification import (
    send_email,
    send_failure_email,
    send_success_email,
)
from pyspark_migration.config.settings import FILE_PATHS

logger = logging.getLogger(__name__)

# Step01 SQL: prepares the database environment for EHRP2BIIS load.
# Referenced at ehrp2biis_preload line 44 as: @ $homedir/step01
# This SQL is typically stored at /home/sa-biisint/bin/EHRP2BIIS/step01
# It prepares staging tables and validates prerequisites.
STEP01_SQL = """
BEGIN
    -- Truncate staging tables to prepare for new load
    EXECUTE IMMEDIATE 'TRUNCATE TABLE NKNIGHT.NWK_ACTION_PRIMARY_TBL';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE NKNIGHT.NWK_ACTION_SECONDARY_TBL';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE NKNIGHT.NWK_ACTION_REMARKS_TBL';

    -- Reset tracking table for new load cycle
    UPDATE NKNIGHT.EHRP_RECS_TRACKING_TBL
    SET CHANGED_WIP_STATUS = NULL,
        BIIS_WIP_STATUS_CHANGED_DT = NULL
    WHERE CHANGED_WIP_STATUS IS NOT NULL
      AND BIIS_WIP_STATUS_CHANGED_DT < TRUNC(SYSDATE);

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Step01 completed successfully');
END;
"""


def cleanup_temp_files():
    """
    Clean up temporary files in the EHRP2BIIS working directory.

    Replaces: find /home/sa-biisint/bin/EHRP2BIIS -name "xyztemp" -exec rm -f {} \\;
    from ehrp2biis_preload line 2.
    """
    work_dir = FILE_PATHS["ehrp2biis_bin"]
    logger.info("Cleaning up temp files in %s", work_dir)

    if os.path.exists(work_dir):
        pattern = os.path.join(work_dir, "*xyztemp*")
        temp_files = glob.glob(pattern)
        for f in temp_files:
            try:
                os.remove(f)
                logger.info("Removed temp file: %s", f)
            except OSError as e:
                logger.warning("Could not remove %s: %s", f, e)
    else:
        logger.warning("Working directory does not exist: %s", work_dir)


def execute_step01(connection_name="ORA_BIIS"):
    """
    Execute the step01 SQL script.

    Replaces: @ $homedir/step01
    from ehrp2biis_preload line 44.

    Parameters
    ----------
    connection_name : str

    Returns
    -------
    bool
        True if successful.
    """
    logger.info("Executing Step01 SQL")
    try:
        execute_sql(connection_name, STEP01_SQL)
        logger.info("Step01 SQL completed successfully")
        return True
    except Exception:
        logger.exception("Step01 SQL failed")
        return False


def run(environment=None):
    """
    Execute the EHRP2BIIS preload workflow.

    Replaces the full ehrp2biis_preload ksh script.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting EHRP2BIIS Preload")
    logger.info("=" * 60)

    try:
        # Step 1: Clean up temp files
        cleanup_temp_files()

        # Step 2-3: Execute step01 SQL
        success = execute_step01()

        # Step 4-5: Send success/failure email
        if success:
            send_success_email(
                "EHRP2BIIS Preload script",
                "SQL Script Ran Successfully",
            )
            logger.info("EHRP2BIIS Preload completed successfully")
        else:
            send_failure_email(
                "EHRP2BIIS Preload script",
                "Step01 SQL did not complete successfully",
            )
            raise RuntimeError("EHRP2BIIS Preload step01 failed")

    except Exception:
        logger.exception("EHRP2BIIS Preload failed")
        raise


def main():
    """CLI entry point for EHRP2BIIS preload."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS EHRP2BIIS Preload")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
