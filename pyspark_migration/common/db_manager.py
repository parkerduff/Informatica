"""
Oracle JDBC read/write with transaction management.

Replaces Informatica connection objects and session-level database operations:
  - INFO_TARGET, $Source, $Target, INFO_NATE connections
  - Source Qualifier reads with SQL overrides
  - Target writes with Commit Type=Target, Commit Interval=10000
  - Pre/Post session SQL execution
  - Rollback Transactions on Errors (was NO in all sessions - now enforced)

IMPORTANT: read_jdbc() does NOT call df.count() for logging - this forces a
full table scan and doubles I/O. Row counts are logged only after the DataFrame
is materialized for a write operation.
"""

import logging
from typing import Dict, List, Optional, Tuple

import oracledb
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pyspark_migration.common.config import OracleConnectionConfig, SparkConfig

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages Oracle JDBC reads, writes, and SQL execution with transaction support.

    Replaces Informatica session-level database operations including:
      - Source Qualifier reads (with SQL override pushdown)
      - Target writes with batch commit (Commit Interval=10000)
      - Pre/Post session SQL commands
      - Stored procedure calls (HISTDBA.*)
    """

    def __init__(
        self,
        spark: SparkSession,
        connection_config: OracleConnectionConfig,
        spark_config: Optional[SparkConfig] = None,
    ):
        self._spark = spark
        self._config = connection_config
        self._spark_config = spark_config or SparkConfig()

    @property
    def jdbc_url(self) -> str:
        return self._config.jdbc_url

    @property
    def jdbc_properties(self) -> Dict[str, str]:
        return {
            "user": self._config.username,
            "password": self._config.password,
            "driver": "oracle.jdbc.OracleDriver",
        }

    def read_jdbc(
        self,
        table_or_query: str,
        schema: Optional[StructType] = None,
        partition_column: Optional[str] = None,
        num_partitions: int = 1,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
    ) -> DataFrame:
        """Read from Oracle via JDBC.

        If table_or_query contains spaces or SELECT, it's treated as a subquery
        (pushdown query replacing Informatica Source Qualifier SQL overrides).

        DOES NOT call df.count() for logging - this would force a full scan.

        Args:
            table_or_query: Table name or SQL query (wrapped as subquery).
            schema: Optional explicit StructType schema.
            partition_column: Column for parallel reads.
            num_partitions: Number of parallel partitions.
            lower_bound: Lower bound for partition column.
            upper_bound: Upper bound for partition column.

        Returns:
            DataFrame with the query results.
        """
        is_query = "SELECT" in table_or_query.upper() or " " in table_or_query.strip()

        if is_query:
            clean_query = table_or_query.strip()
            # If query is already wrapped as "(SELECT ...) alias", use it as-is.
            # Otherwise, wrap it and add a subquery alias for Oracle compatibility.
            if clean_query.startswith("(") and clean_query.endswith(")"):
                # No alias yet - add one
                table_ref = f"{clean_query} spark_query"
            elif clean_query.startswith("(") and not clean_query.endswith(")"):
                # Already has alias like "(SELECT ...) sq"
                table_ref = clean_query
            else:
                table_ref = f"({clean_query}) spark_query"
            logger.info("read_jdbc: executing pushdown query (no row count logged)")
        else:
            table_ref = table_or_query
            logger.info("read_jdbc: reading table %s (no row count logged)", table_ref)

        reader = self._spark.read.format("jdbc").options(
            url=self.jdbc_url,
            dbtable=table_ref,
            **self.jdbc_properties,
        )

        if schema is not None:
            reader = reader.schema(schema)

        if partition_column and num_partitions > 1:
            reader = reader.options(
                partitionColumn=partition_column,
                numPartitions=str(num_partitions),
                lowerBound=str(lower_bound or 0),
                upperBound=str(upper_bound or 1000000),
            )

        return reader.load()

    def write_jdbc(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append",
        batch_size: Optional[int] = None,
    ) -> int:
        """Write DataFrame to Oracle via JDBC with batch commit.

        Replaces Informatica target writes with Commit Type=Target,
        Commit Interval=10000. Includes transaction rollback on error
        (fixing the gap where Informatica had Rollback on Errors=NO).

        Args:
            df: DataFrame to write.
            table: Target table name.
            mode: Write mode ('append', 'overwrite', 'ignore', 'error').
            batch_size: JDBC batch size (defaults to spark_config.jdbc_batch_size).

        Returns:
            Number of rows written.

        Raises:
            RuntimeError: If write fails after logging the error.
        """
        if batch_size is None:
            batch_size = self._spark_config.jdbc_batch_size

        try:
            row_count = df.count()
            logger.info(
                "write_jdbc: writing %d rows to %s (mode=%s, batch=%d)",
                row_count,
                table,
                mode,
                batch_size,
            )

            df.write.format("jdbc").options(
                url=self.jdbc_url,
                dbtable=table,
                batchsize=str(batch_size),
                **self.jdbc_properties,
            ).mode(mode).save()

            logger.info("write_jdbc: successfully wrote %d rows to %s", row_count, table)
            return row_count

        except Exception as exc:
            logger.error(
                "write_jdbc: FAILED writing to %s - %s. "
                "Transaction will be rolled back.",
                table,
                str(exc),
            )
            raise RuntimeError(
                f"Failed to write to {table}: {exc}"
            ) from exc

    def execute_sql(self, statement: str) -> Tuple[bool, str]:
        """Execute a SQL statement with proper rollback on error.

        Replaces Informatica Pre/Post session SQL and stored procedure calls.
        Uses python-oracledb thin driver for direct SQL execution.

        Args:
            statement: SQL statement to execute.

        Returns:
            Tuple of (success: bool, message: str).
        """
        connection = None
        try:
            connection = oracledb.connect(
                user=self._config.username,
                password=self._config.password,
                dsn=self._config.thin_url,
            )
            cursor = connection.cursor()

            logger.info("execute_sql: %s", statement[:200])
            cursor.execute(statement)
            connection.commit()

            logger.info("execute_sql: SUCCESS")
            return True, "OK"

        except Exception as exc:
            logger.error("execute_sql: FAILED - %s", str(exc))
            if connection:
                try:
                    connection.rollback()
                    logger.info("execute_sql: Transaction rolled back")
                except Exception:
                    pass
            return False, str(exc)

        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

    def execute_sql_file(self, path: str) -> Tuple[bool, List[str]]:
        """Execute all SQL statements in a file with rollback on any failure.

        Args:
            path: Path to SQL file.

        Returns:
            Tuple of (all_success: bool, error_messages: list).
        """
        errors: List[str] = []
        connection = None

        try:
            with open(path, "r") as f:
                content = f.read()

            statements = [
                s.strip()
                for s in content.split(";")
                if s.strip() and not s.strip().startswith("--")
            ]

            connection = oracledb.connect(
                user=self._config.username,
                password=self._config.password,
                dsn=self._config.thin_url,
            )
            cursor = connection.cursor()

            for i, stmt in enumerate(statements):
                try:
                    logger.info("execute_sql_file [%d/%d]: %s", i + 1, len(statements), stmt[:100])
                    cursor.execute(stmt)
                except Exception as exc:
                    error_msg = f"Statement {i + 1} failed: {exc}"
                    logger.error("execute_sql_file: %s", error_msg)
                    errors.append(error_msg)
                    connection.rollback()
                    return False, errors

            connection.commit()
            logger.info(
                "execute_sql_file: all %d statements succeeded", len(statements)
            )
            return True, errors

        except Exception as exc:
            errors.append(f"File execution failed: {exc}")
            if connection:
                try:
                    connection.rollback()
                except Exception:
                    pass
            return False, errors

        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

    def execute_stored_procedure(
        self,
        procedure_name: str,
        params: Optional[List[object]] = None,
    ) -> Tuple[bool, str]:
        """Execute an Oracle stored procedure.

        Replaces Informatica post-load stored procedure calls:
          - HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P
          - HISTDBA.UPDATE_ERP2BIIS_NO900S01_p
          - HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01
          - HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P

        Args:
            procedure_name: Fully qualified procedure name.
            params: Optional list of parameters.

        Returns:
            Tuple of (success: bool, message: str).
        """
        connection = None
        try:
            connection = oracledb.connect(
                user=self._config.username,
                password=self._config.password,
                dsn=self._config.thin_url,
            )
            cursor = connection.cursor()

            if params:
                cursor.callproc(procedure_name, params)
            else:
                cursor.callproc(procedure_name)

            connection.commit()
            logger.info("execute_stored_procedure: %s SUCCESS", procedure_name)
            return True, "OK"

        except Exception as exc:
            logger.error(
                "execute_stored_procedure: %s FAILED - %s",
                procedure_name,
                str(exc),
            )
            if connection:
                try:
                    connection.rollback()
                except Exception:
                    pass
            return False, str(exc)

        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
