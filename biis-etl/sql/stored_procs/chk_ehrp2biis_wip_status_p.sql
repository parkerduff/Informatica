-- T-SQL stub for Oracle procedure CHK_EHRP2BIIS_WIP_STATUS_P.
-- The original PL/SQL source lives only in the Oracle database and is not in
-- this repository. Extract it and replace the body below.
IF OBJECT_ID('dbo.chk_ehrp2biis_wip_status_p', 'P') IS NOT NULL DROP PROCEDURE [dbo].[chk_ehrp2biis_wip_status_p];
GO
CREATE PROCEDURE [dbo].[chk_ehrp2biis_wip_status_p]
    @run_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    PRINT 'WARNING - STUB: Extract Oracle source for CHK_EHRP2BIIS_WIP_STATUS_P and implement.';
    -- RAISERROR is intentionally informational (severity 10) so the migrated
    -- pipeline can run end-to-end before the Oracle source is ported.
    RAISERROR('STUB: Extract Oracle source and implement.', 10, 1) WITH NOWAIT;
END;
GO
