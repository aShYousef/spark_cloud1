# Terraform configuration for AWS infrastructure
# This is a sample configuration - modify according to your needs

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  default     = "spark-data-processor"
}

variable "environment" {
  description = "Environment (dev/staging/prod)"
  default     = "dev"
}

# S3 bucket for data storage
resource "aws_s3_bucket" "data_bucket" {
  bucket = "${var.project_name}-${var.environment}-data"

  tags = {
    Name        = "${var.project_name}-data"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "data_bucket_versioning" {
  bucket = aws_s3_bucket.data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_bucket_encryption" {
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM role for the application
resource "aws_iam_role" "app_role" {
  name = "${var.project_name}-${var.environment}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "app_s3_policy" {
  name = "${var.project_name}-s3-policy"
  role = aws_iam_role.app_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Output values
output "s3_bucket_name" {
  value       = aws_s3_bucket.data_bucket.id
  description = "S3 bucket name for data storage"
}

output "s3_bucket_arn" {
  value       = aws_s3_bucket.data_bucket.arn
  description = "S3 bucket ARN"
}

output "iam_role_arn" {
  value       = aws_iam_role.app_role.arn
  description = "IAM role ARN for the application"
}
