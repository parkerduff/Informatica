-- T-SQL stub for Oracle stored procedure update_erp2biis_900sonly01_p.
-- The original PL/SQL source is NOT in the Informatica repo; it lives in the
-- Oracle database. This stub accepts the same parameters as the Oracle version
-- (inferred from ehrp2biis_afterload.sql usage) and emits an informational
-- "STUB" message via RAISERROR severity 10 (does not abort the batch, so the
-- afterload pipeline still completes) until the Oracle source is extracted.
IF OBJECT_ID('dbo.update_erp2biis_900sonly01_p', 'P') IS NOT NULL DROP PROCEDURE dbo.update_erp2biis_900sonly01_p;
GO
CREATE PROCEDURE dbo.update_erp2biis_900sonly01_p
AS
BEGIN
    SET NOCOUNT ON;
    RAISERROR('STUB: Extract Oracle source and implement dbo.update_erp2biis_900sonly01_p.', 10, 1) WITH NOWAIT;
END;
GO
