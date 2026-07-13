output "glue_jobs" {
  description = "Converted Glue job names"
  value       = { for k, j in aws_glue_job.job : k => j.name }
}

output "master_state_machine_arn" {
  value = aws_sfn_state_machine.master.arn
}

output "family_state_machine_arns" {
  value = { for k, m in aws_sfn_state_machine.family : k => m.arn }
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "data_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "code_bucket" {
  value = aws_s3_bucket.code.bucket
}
