"""
Database connection management for PySpark migration.

Replaces Informatica connection objects:
- INFO_TARGET (target DB, Oracle ORA_BIIS)
- $Source (source DB, Oracle ORA_BIISPRD_SRC)  
- INFO_NATE (separate Oracle DB for lkp_PS_JPM_JP_ITEMS in wf_EHRP2BIIS_UPDATE)

Also replaces credential file reads from:
- /home/sa-biisint/.use, .pw (primary credentials)
- /home/sa-biisint/.use1, .pw1 (secondary credentials)
"""

import logging
from contextlib import contextmanager
from typing import Optional

import oracledb
from pyspark.sql import SparkSession, DataFrame

from pyspark_migration.common.config import OracleConnectionConfig, MigrationConfig

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages Oracle database connections for all ETL jobs.
    
    Replaces Informatica's connection management including:
    - Relational Reader connections (CONNECTIONSUBTYPE="Oracle")
    - Relational Writer connections (CONNECTIONNAME="INFO_TARGET")
    - Lookup connections (used by all lookup transformations)
    """

    def __init__(self, config: MigrationConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self._target_conn: Optional[oracledb.Connection] = None
        self._source_conn: Optional[oracledb.Connection] = None
        self._nate_conn: Optional[oracledb.Connection] = None

    @contextmanager
    def target_connection(self):
        """Get Oracle connection to INFO_TARGET (ORA_BIIS).
        
        Used by all jobs for writing to target tables:
        - PAY_PERIOD, COMP_TIME_DAILY_TBL, PSEUDOSSN_TBL,
        - HI_PM_FDA_TATRAN_TBL, COUNTER_TBL, ERROR_TBL,
        - NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL, EHRP_RECS_TRACKING_TBL
        """
        conn = None
        try:
            conn = oracledb.connect(
                user=self.config.info_target.username,
                password=self.config.info_target.password,
                dsn=self.config.info_target.dsn
            )
            logger.info("Connected to INFO_TARGET (ORA_BIIS)")
            yield conn
        except oracledb.Error as e:
            logger.error(f"Failed to connect to INFO_TARGET: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Closed INFO_TARGET connection")

    @contextmanager
    def source_connection(self):
        """Get Oracle connection to $Source (ORA_BIISPRD_SRC).
        
        Used by wf_EHRP2BIIS_UPDATE for reading source tables:
        - PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
        """
        conn = None
        try:
            conn = oracledb.connect(
                user=self.config.source_db.username,
                password=self.config.source_db.password,
                dsn=self.config.source_db.dsn
            )
            logger.info("Connected to $Source (ORA_BIISPRD_SRC)")
            yield conn
        except oracledb.Error as e:
            logger.error(f"Failed to connect to $Source: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Closed $Source connection")

    @contextmanager
    def nate_connection(self):
        """Get Oracle connection to INFO_NATE.
        
        Used ONLY by lkp_PS_JPM_JP_ITEMS in wf_EHRP2BIIS_UPDATE.
        (XML/EHRP2BIIS_UPDATE lines 2752-2754: Connection Information = INFO_NATE)
        """
        conn = None
        try:
            conn = oracledb.connect(
                user=self.config.info_nate.username,
                password=self.config.info_nate.password,
                dsn=self.config.info_nate.dsn
            )
            logger.info("Connected to INFO_NATE")
            yield conn
        except oracledb.Error as e:
            logger.error(f"Failed to connect to INFO_NATE: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Closed INFO_NATE connection")

    def read_jdbc(self, table: str, connection: str = "target",
                  predicate: Optional[str] = None) -> DataFrame:
        """Read a table via JDBC into a Spark DataFrame.
        
        Replaces Informatica Source Qualifier (Relational Reader).
        
        Args:
            table: Table name or subquery in parentheses
            connection: "target" (INFO_TARGET), "source" ($Source), or "nate" (INFO_NATE)
            predicate: Optional WHERE clause
        """
        conn_config = self._get_connection_config(connection)
        jdbc_url = conn_config.jdbc_url
        props = {
            "user": conn_config.username,
            "password": conn_config.password,
            "driver": "oracle.jdbc.OracleDriver"
        }

        if predicate:
            query = f"(SELECT * FROM {table} WHERE {predicate}) t"
            df = self.spark.read.jdbc(url=jdbc_url, table=query, properties=props)
        else:
            df = self.spark.read.jdbc(url=jdbc_url, table=table, properties=props)

        logger.info(f"Read from {table} via {connection}")
        return df

    def write_jdbc(self, df: DataFrame, table: str, mode: str = "append",
                   connection: str = "target") -> None:
        """Write a DataFrame to an Oracle table via JDBC.
        
        Replaces Informatica Target Definition (Relational Writer).
        Supports mode="append" (DD_INSERT) and mode="overwrite".
        
        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode ("append", "overwrite")
            connection: "target" (INFO_TARGET), "source", or "nate"
        """
        conn_config = self._get_connection_config(connection)
        jdbc_url = conn_config.jdbc_url
        props = {
            "user": conn_config.username,
            "password": conn_config.password,
            "driver": "oracle.jdbc.OracleDriver",
            "batchsize": "10000"  # Replaces Informatica Commit Interval = 10000
        }

        df.write.jdbc(url=jdbc_url, table=table, mode=mode, properties=props)
        logger.info(f"Wrote DataFrame to {table} via {connection} (mode={mode})")

    def execute_sql(self, sql: str, connection: str = "target",
                    params: Optional[list] = None) -> None:
        """Execute a SQL statement directly via Oracle connection.
        
        Replaces Informatica Update Strategy (DD_UPDATE, DD_DELETE)
        and Post-session SQL commands.
        
        Args:
            sql: SQL statement to execute
            connection: "target", "source", or "nate"
            params: Optional bind parameters
        """
        conn_config = self._get_connection_config(connection)
        conn = oracledb.connect(
            user=conn_config.username,
            password=conn_config.password,
            dsn=conn_config.dsn
        )
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            rows_affected = cursor.rowcount
            logger.info(f"Executed SQL on {connection}: {rows_affected} rows affected")
            cursor.close()
        except oracledb.Error as e:
            conn.rollback()
            logger.error(f"SQL execution failed on {connection}: {e}")
            raise
        finally:
            conn.close()

    def execute_sql_file(self, filepath: str, connection: str = "target") -> str:
        """Execute a SQL file via Oracle connection.
        
        Replaces ehrp2biis_preload KSH script's SQL*Plus execution:
        sqlplus -s '/nolog' << connect $loin2/$ps2 ... @ $homedir/step01
        
        Args:
            filepath: Path to SQL file
            connection: "target", "source", or "nate"
            
        Returns:
            Output log string for error detection
        """
        with open(filepath, "r") as f:
            sql_content = f.read()

        conn_config = self._get_connection_config(connection)
        conn = oracledb.connect(
            user=conn_config.username,
            password=conn_config.password,
            dsn=conn_config.dsn
        )
        output_lines = []
        has_errors = False
        first_error = None
        try:
            cursor = conn.cursor()
            # Split and execute individual statements
            for statement in sql_content.split(";"):
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    try:
                        cursor.execute(statement)
                        output_lines.append(f"OK: {statement[:80]}...")
                    except oracledb.Error as e:
                        output_lines.append(f"ERROR: {e} - {statement[:80]}...")
                        has_errors = True
                        if first_error is None:
                            first_error = e
            if has_errors:
                conn.rollback()
                cursor.close()
                raise oracledb.Error(
                    f"SQL file execution had errors, rolled back: {first_error}"
                )
            conn.commit()
            cursor.close()
        except oracledb.Error as e:
            conn.rollback()
            output_lines.append(f"ERROR: {e}")
            raise
        finally:
            conn.close()

        return "\n".join(output_lines)

    def _get_connection_config(self, connection: str) -> OracleConnectionConfig:
        """Get connection config by name."""
        if connection == "target":
            return self.config.info_target
        elif connection == "source":
            return self.config.source_db
        elif connection == "nate":
            return self.config.info_nate
        else:
            raise ValueError(f"Unknown connection: {connection}")
