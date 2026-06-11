-- T-SQL stub for Oracle stored procedure gather_ehrp2biis_runcounts_p.
-- The original PL/SQL source is NOT in the Informatica repo; it lives in the
-- Oracle database. This stub accepts the same parameters as the Oracle version
-- (inferred from ehrp2biis_afterload.sql usage) and emits an informational
-- "STUB" message via RAISERROR severity 10 (does not abort the batch, so the
-- afterload pipeline still completes) until the Oracle source is extracted.
IF OBJECT_ID('dbo.gather_ehrp2biis_runcounts_p', 'P') IS NOT NULL DROP PROCEDURE dbo.gather_ehrp2biis_runcounts_p;
GO
CREATE PROCEDURE dbo.gather_ehrp2biis_runcounts_p
    @p_run_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    RAISERROR('STUB: Extract Oracle source and implement dbo.gather_ehrp2biis_runcounts_p.', 10, 1) WITH NOWAIT;
END;
GO
