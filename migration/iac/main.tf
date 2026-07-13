terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  name = "${var.project}-${var.environment}"
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
    Migration   = "informatica-powercenter-to-glue"
  }
}

# ---------------------------------------------------------------------------
# S3: code, input (data/int/in/<FOLDER>), archive (data/archive/<FOLDER>), output
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "code" {
  bucket = var.code_bucket
  tags   = local.tags
}

resource "aws_s3_bucket" "data" {
  bucket = var.data_bucket
  tags   = local.tags
}

# archive prefix lifecycle == legacy Maintenance Scripts/archive_files + remove_file
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "archive-retention"
    status = "Enabled"
    filter { prefix = "data/archive/" }
    transition {
      days          = 30
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
  }
}

# ---------------------------------------------------------------------------
# SNS: mailx alerts -> topic
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
  tags = local.tags
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# ---------------------------------------------------------------------------
# IAM roles
# ---------------------------------------------------------------------------
resource "aws_iam_role" "glue" {
  name = "${local.name}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.name}-glue-s3"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*",
      aws_s3_bucket.code.arn, "${aws_s3_bucket.code.arn}/*"]
    }]
  })
}

resource "aws_iam_role" "sfn" {
  name = "${local.name}-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "sfn" {
  name = "${local.name}-sfn-policy"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], Resource = "*" },
      { Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
      { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = "*" },
      { Effect = "Allow", Action = ["states:StartExecution"], Resource = "*" }
    ]
  })
}
