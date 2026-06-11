# JDBC drivers

Place the SQL Server JDBC driver here for the production (`ENV=prod`) path, e.g.:

    mssql-jdbc-12.4.2.jre11.jar

The default `test` environment uses the bundled SQLite backend and needs no JDBC jar.
Driver jars are intentionally **not** committed (binaries); download from
https://learn.microsoft.com/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server
