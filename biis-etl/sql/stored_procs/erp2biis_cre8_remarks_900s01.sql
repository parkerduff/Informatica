-- T-SQL stub for Oracle procedure ERP2BIIS_CRE8_REMARKS_900S01.
-- The original PL/SQL source lives only in the Oracle database and is not in
-- this repository. Extract it and replace the body below.
IF OBJECT_ID('dbo.erp2biis_cre8_remarks_900s01', 'P') IS NOT NULL DROP PROCEDURE [dbo].[erp2biis_cre8_remarks_900s01];
GO
CREATE PROCEDURE [dbo].[erp2biis_cre8_remarks_900s01]
    @run_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    PRINT 'WARNING - STUB: Extract Oracle source for ERP2BIIS_CRE8_REMARKS_900S01 and implement.';
    -- RAISERROR is intentionally informational (severity 10) so the migrated
    -- pipeline can run end-to-end before the Oracle source is ported.
    RAISERROR('STUB: Extract Oracle source and implement.', 10, 1) WITH NOWAIT;
END;
GO
