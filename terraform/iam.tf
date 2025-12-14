# IAM Roles and Policies for Hyperion
# Three roles: ETL runner, Spark runner, Web app

# ============================================
# ETL RUNNER ROLE
# Used by: Kafka consumers, seed scripts
# Access: S3 read/write, RDS connect, Secrets Manager
# ============================================

resource "aws_iam_role" "etl_runner" {
  name = "${local.name_prefix}-etl-runner"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "ecs-tasks.amazonaws.com",
            "ec2.amazonaws.com"
          ]
        }
      }
    ]
  })

  tags = {
    Name = "${local.name_prefix}-etl-runner"
  }
}

resource "aws_iam_policy" "etl_s3" {
  name        = "${local.name_prefix}-etl-s3"
  description = "S3 access for ETL processes"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRawBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*"
        ]
      },
      {
        Sid    = "ReadWriteStagingBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.staging.arn,
          "${aws_s3_bucket.staging.arn}/*"
        ]
      },
      {
        Sid    = "WriteProcessedBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "etl_secrets" {
  name        = "${local.name_prefix}-etl-secrets"
  description = "Secrets Manager access for ETL processes"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadDbSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.db_password.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "etl_s3" {
  role       = aws_iam_role.etl_runner.name
  policy_arn = aws_iam_policy.etl_s3.arn
}

resource "aws_iam_role_policy_attachment" "etl_secrets" {
  role       = aws_iam_role.etl_runner.name
  policy_arn = aws_iam_policy.etl_secrets.arn
}

# CloudWatch Logs for ETL
resource "aws_iam_role_policy_attachment" "etl_logs" {
  role       = aws_iam_role.etl_runner.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ============================================
# SPARK RUNNER ROLE
# Used by: EMR, Glue, or EC2-based Spark jobs
# Access: S3 read (all buckets), Glue catalog
# ============================================

resource "aws_iam_role" "spark_runner" {
  name = "${local.name_prefix}-spark-runner"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "elasticmapreduce.amazonaws.com",
            "glue.amazonaws.com",
            "ec2.amazonaws.com"
          ]
        }
      }
    ]
  })

  tags = {
    Name = "${local.name_prefix}-spark-runner"
  }
}

resource "aws_iam_policy" "spark_s3" {
  name        = "${local.name_prefix}-spark-s3"
  description = "S3 access for Spark jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadAllBuckets"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.staging.arn,
          "${aws_s3_bucket.staging.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*"
        ]
      },
      {
        Sid    = "WriteProcessedBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.processed.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "spark_glue" {
  name        = "${local.name_prefix}-spark-glue"
  description = "Glue Data Catalog access for Spark/Hive"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          "arn:aws:glue:${var.aws_region}:*:database/hyperion*",
          "arn:aws:glue:${var.aws_region}:*:table/hyperion*/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_s3" {
  role       = aws_iam_role.spark_runner.name
  policy_arn = aws_iam_policy.spark_s3.arn
}

resource "aws_iam_role_policy_attachment" "spark_glue" {
  role       = aws_iam_role.spark_runner.name
  policy_arn = aws_iam_policy.spark_glue.arn
}

resource "aws_iam_role_policy_attachment" "spark_logs" {
  role       = aws_iam_role.spark_runner.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ============================================
# WEB APP ROLE
# Used by: React/Node backend (ECS, Lambda, etc.)
# Access: RDS read, limited S3 read
# ============================================

resource "aws_iam_role" "web_app" {
  name = "${local.name_prefix}-web-app"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "ecs-tasks.amazonaws.com",
            "ec2.amazonaws.com"
          ]
        }
      }
    ]
  })

  tags = {
    Name = "${local.name_prefix}-web-app"
  }
}

resource "aws_iam_policy" "web_app_s3" {
  name        = "${local.name_prefix}-web-app-s3"
  description = "Limited S3 read access for web app"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadProcessedExports"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/exports/*",
          "${aws_s3_bucket.processed.arn}/analytics/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "web_app_secrets" {
  name        = "${local.name_prefix}-web-app-secrets"
  description = "Secrets Manager read access for web app"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadDbSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.db_password.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "web_app_s3" {
  role       = aws_iam_role.web_app.name
  policy_arn = aws_iam_policy.web_app_s3.arn
}

resource "aws_iam_role_policy_attachment" "web_app_secrets" {
  role       = aws_iam_role.web_app.name
  policy_arn = aws_iam_policy.web_app_secrets.arn
}

resource "aws_iam_role_policy_attachment" "web_app_logs" {
  role       = aws_iam_role.web_app.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ============================================
# INSTANCE PROFILES (for EC2-based deployments)
# ============================================

resource "aws_iam_instance_profile" "etl_runner" {
  name = "${local.name_prefix}-etl-runner"
  role = aws_iam_role.etl_runner.name
}

resource "aws_iam_instance_profile" "spark_runner" {
  name = "${local.name_prefix}-spark-runner"
  role = aws_iam_role.spark_runner.name
}

resource "aws_iam_instance_profile" "web_app" {
  name = "${local.name_prefix}-web-app"
  role = aws_iam_role.web_app.name
}
