from pyspark_etl.utils.oracle_jdbc import _split_sql_statements


def test_strips_sqlplus_directives_and_splits():
    script = """
    SET ECHO ON
    SPOOL /tmp/out.log
    PROMPT Running...
    UPDATE foo SET bar = 1 WHERE baz = 2;
    INSERT INTO foo VALUES (1, 2);
    EXIT
    """
    stmts = _split_sql_statements(script)
    assert stmts == [
        "UPDATE foo SET bar = 1 WHERE baz = 2",
        "INSERT INTO foo VALUES (1, 2)",
    ]


def test_plsql_block_kept_intact():
    script = "BEGIN\n  my_proc;\n  other_proc;\nEND;\n/\n"
    stmts = _split_sql_statements(script)
    assert len(stmts) == 1
    assert stmts[0].startswith("BEGIN")
    assert stmts[0].rstrip().endswith("END;")
    assert "my_proc" in stmts[0] and "other_proc" in stmts[0]


def test_exec_shorthand_becomes_anonymous_block():
    script = (
        "EXEC HISTDBA.MY_PROC;\n"
        "EXEC HISTDBA.OTHER_PROC(NULL);\n"
        "COMMIT;\n"
    )
    stmts = _split_sql_statements(script)
    assert stmts == [
        "BEGIN HISTDBA.MY_PROC; END;",
        "BEGIN HISTDBA.OTHER_PROC(NULL); END;",
        "COMMIT",
    ]
