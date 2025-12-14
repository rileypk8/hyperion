# Outputs for Hyperion infrastructure
# These values are needed by other components (ETL, Spark, Web app)

# ============================================
# S3 BUCKETS
# ============================================

output "s3_bucket_raw" {
  description = "Raw data bucket name"
  value       = aws_s3_bucket.raw.id
}

output "s3_bucket_raw_arn" {
  description = "Raw data bucket ARN"
  value       = aws_s3_bucket.raw.arn
}

output "s3_bucket_staging" {
  description = "Staging data bucket name"
  value       = aws_s3_bucket.staging.id
}

output "s3_bucket_staging_arn" {
  description = "Staging data bucket ARN"
  value       = aws_s3_bucket.staging.arn
}

output "s3_bucket_processed" {
  description = "Processed data bucket name"
  value       = aws_s3_bucket.processed.id
}

output "s3_bucket_processed_arn" {
  description = "Processed data bucket ARN"
  value       = aws_s3_bucket.processed.arn
}

# ============================================
# RDS
# ============================================

output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = aws_db_instance.main.endpoint
}

output "rds_address" {
  description = "RDS instance address (hostname only)"
  value       = aws_db_instance.main.address
}

output "rds_port" {
  description = "RDS instance port"
  value       = aws_db_instance.main.port
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.main.db_name
}

output "rds_username" {
  description = "RDS master username"
  value       = aws_db_instance.main.username
  sensitive   = true
}

output "rds_secret_arn" {
  description = "Secrets Manager ARN containing RDS credentials"
  value       = aws_secretsmanager_secret.db_password.arn
}

output "rds_secret_name" {
  description = "Secrets Manager secret name for RDS credentials"
  value       = aws_secretsmanager_secret.db_password.name
}

# ============================================
# VPC / NETWORKING
# ============================================

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (for RDS, Lambda, etc.)"
  value       = aws_subnet.private[*].id
}

output "rds_security_group_id" {
  description = "Security group ID for RDS access"
  value       = aws_security_group.rds.id
}

output "etl_security_group_id" {
  description = "Security group ID for ETL processes"
  value       = aws_security_group.etl.id
}

# ============================================
# IAM ROLES
# ============================================

output "etl_runner_role_arn" {
  description = "IAM role ARN for ETL processes"
  value       = aws_iam_role.etl_runner.arn
}

output "etl_runner_instance_profile" {
  description = "Instance profile name for ETL EC2 instances"
  value       = aws_iam_instance_profile.etl_runner.name
}

output "spark_runner_role_arn" {
  description = "IAM role ARN for Spark jobs"
  value       = aws_iam_role.spark_runner.arn
}

output "spark_runner_instance_profile" {
  description = "Instance profile name for Spark EC2 instances"
  value       = aws_iam_instance_profile.spark_runner.name
}

output "web_app_role_arn" {
  description = "IAM role ARN for web application"
  value       = aws_iam_role.web_app.arn
}

output "web_app_instance_profile" {
  description = "Instance profile name for web app EC2 instances"
  value       = aws_iam_instance_profile.web_app.name
}

# ============================================
# CONNECTION STRINGS (for convenience)
# ============================================

output "postgres_connection_string" {
  description = "PostgreSQL connection string (password from Secrets Manager)"
  value       = "postgresql://${aws_db_instance.main.username}:<password>@${aws_db_instance.main.endpoint}/${aws_db_instance.main.db_name}"
  sensitive   = true
}

# ============================================
# REGION INFO
# ============================================

output "aws_region" {
  description = "AWS region where resources are deployed"
  value       = var.aws_region
}
