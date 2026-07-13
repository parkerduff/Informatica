# ---------------------------------------------------------------------------
# Glue jobs -- one per converted PowerCenter folder, typed by classification.
#   pyspark      -> glueetl   (Glue PySpark)
#   python_shell -> pythonshell (Glue Python Shell)
# ---------------------------------------------------------------------------

resource "aws_glue_job" "job" {
  for_each = var.jobs

  name         = "informatica-${lower(each.key)}"
  role_arn     = aws_iam_role.glue.arn
  glue_version = var.glue_version
  tags         = local.tags

  command {
    name            = each.value.engine == "pyspark" ? "glueetl" : "pythonshell"
    python_version  = "3"
    script_location = "s3://${var.code_bucket}/jobs/${each.key}/job.py"
  }

  default_arguments = {
    "--folder"                           = each.key
    "--in-dir"                           = "s3://${var.data_bucket}/data/int/in/${each.key}"
    "--out-dir"                          = "s3://${var.data_bucket}/data/out"
    "--extra-py-files"                   = "s3://${var.code_bucket}/lib/migration_lib.zip"
    "--additional-python-modules"        = "faker"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }

  # PySpark feeds get real workers; python-shell feeds a fractional DPU.
  number_of_workers = each.value.engine == "pyspark" ? 5 : null
  worker_type       = each.value.engine == "pyspark" ? "G.1X" : null
  max_capacity      = each.value.engine == "python_shell" ? 1 : null

  max_retries = 1
  timeout     = 120
  execution_property { max_concurrent_runs = 3 }
}

# Push-down SQL runner (invokes migration/sql/pushdown.py against the warehouse).
resource "aws_glue_job" "pushdown_sql" {
  name         = "informatica-pushdown-sql"
  role_arn     = aws_iam_role.glue.arn
  glue_version = var.glue_version
  tags         = local.tags

  command {
    name            = "pythonshell"
    python_version  = "3"
    script_location = "s3://${var.code_bucket}/sql/pushdown_runner.py"
  }
  default_arguments = {
    "--extra-py-files" = "s3://${var.code_bucket}/lib/migration_lib.zip"
  }
  max_capacity = 1
  max_retries  = 1
  timeout      = 60
}
