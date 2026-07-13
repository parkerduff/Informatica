variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Resource name prefix"
  type        = string
  default     = "informatica-biis"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "code_bucket" {
  description = "S3 bucket holding the Glue job scripts + libraries"
  type        = string
  default     = "informatica-biis-code"
}

variable "data_bucket" {
  description = "S3 bucket for int/in, archive, and target output prefixes"
  type        = string
  default     = "informatica-biis-data"
}

variable "glue_version" {
  type    = string
  default = "4.0"
}

variable "alert_emails" {
  description = "mailx -> SNS subscription targets"
  type        = list(string)
  default     = []
}

variable "pay_period_schedule" {
  description = "EventBridge cron for the biweekly pay-period run (UTC)"
  type        = string
  # 06:00 UTC every other Thursday is approximated with a weekly rule + a
  # pay-period gate inside the state machine; adjust to the agency calendar.
  default = "cron(0 6 ? * THU *)"
}

# The eight converted jobs and their Glue job type.
variable "jobs" {
  type = map(object({ engine = string }))
  default = {
    Pseudossn        = { engine = "pyspark" }
    EHRP2BIIS_UPDATE = { engine = "pyspark" }
    CPM_NIH          = { engine = "pyspark" }
    CPM_OIG          = { engine = "pyspark" }
    CPM_CDC          = { engine = "pyspark" }
    FDA_Leave        = { engine = "pyspark" }
    Pay_Calendar     = { engine = "python_shell" }
    COMPTIME         = { engine = "python_shell" }
  }
}
