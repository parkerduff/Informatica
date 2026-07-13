# ---------------------------------------------------------------------------
# Step Functions state machines (generated ASL under orchestration/statemachines)
# ---------------------------------------------------------------------------

locals {
  asl_dir = "${path.module}/../orchestration/statemachines"
  families = {
    agency_feeds = "agency_feeds.asl.json"
    pseudossn    = "pseudossn.asl.json"
    ehrp         = "ehrp.asl.json"
    reference    = "reference.asl.json"
  }
}

resource "aws_sfn_state_machine" "family" {
  for_each   = local.families
  name       = "${local.name}-${each.key}"
  role_arn   = aws_iam_role.sfn.arn
  definition = file("${local.asl_dir}/${each.value}")
  tags       = local.tags
}

resource "aws_sfn_state_machine" "master" {
  name       = "${local.name}-master"
  role_arn   = aws_iam_role.sfn.arn
  definition = file("${local.asl_dir}/master.asl.json")
  tags       = local.tags
}

# ---------------------------------------------------------------------------
# EventBridge: ksh + cron -> scheduled biweekly pay-period trigger
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "pay_period" {
  name                = "${local.name}-pay-period"
  description         = "Biweekly pay-period trigger for the EHRP->BIIS master pipeline"
  schedule_expression = var.pay_period_schedule
  tags                = local.tags
}

resource "aws_iam_role" "events" {
  name = "${local.name}-events-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "events" {
  name = "${local.name}-events-policy"
  role = aws_iam_role.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.master.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "pay_period" {
  rule     = aws_cloudwatch_event_rule.pay_period.name
  arn      = aws_sfn_state_machine.master.arn
  role_arn = aws_iam_role.events.arn
  input = jsonencode({
    input_prefix    = "s3://${var.data_bucket}/data/int/in"
    output_prefix   = "s3://${var.data_bucket}/data/out"
    alert_topic_arn = aws_sns_topic.alerts.arn
  })
}
