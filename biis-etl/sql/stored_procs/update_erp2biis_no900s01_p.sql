-- T-SQL stub for Oracle procedure UPDATE_ERP2BIIS_NO900S01_P.
-- The original PL/SQL source lives only in the Oracle database and is not in
-- this repository. Extract it and replace the body below.
IF OBJECT_ID('dbo.update_erp2biis_no900s01_p', 'P') IS NOT NULL DROP PROCEDURE [dbo].[update_erp2biis_no900s01_p];
GO
CREATE PROCEDURE [dbo].[update_erp2biis_no900s01_p]
    @run_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    PRINT 'WARNING - STUB: Extract Oracle source for UPDATE_ERP2BIIS_NO900S01_P and implement.';
    -- RAISERROR is intentionally informational (severity 10) so the migrated
    -- pipeline can run end-to-end before the Oracle source is ported.
    RAISERROR('STUB: Extract Oracle source and implement.', 10, 1) WITH NOWAIT;
END;
GO
