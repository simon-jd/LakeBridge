terraform {
  required_version = ">= 1.9, < 2.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.94.0" }
  }
}

provider "aws" {
  region = var.region
  default_tags { tags = { Project = "LakeBridge", Environment = "experiment" } }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  type        = string
  description = "Globally unique lowercase S3 bucket prefix"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{3,40}$", var.name))
    error_message = "Use 4–41 lowercase letters, digits or hyphens, starting with a letter."
  }
}

resource "aws_s3_bucket" "raw" {
  bucket        = "${var.name}-raw"
  force_destroy = false
}

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.name}-warehouse"
  force_destroy = false
}

locals {
  buckets = { raw = aws_s3_bucket.raw.id, warehouse = aws_s3_bucket.warehouse.id }
}

resource "aws_s3_bucket_public_access_block" "private" {
  for_each                = local.buckets
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "versions" {
  for_each = local.buckets
  bucket   = each.value
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  for_each = local.buckets
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

data "aws_iam_policy_document" "writer" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn, aws_s3_bucket.warehouse.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.warehouse.arn}/*"]
  }
}

resource "aws_iam_policy" "writer" {
  name   = "${var.name}-lake-writer"
  policy = data.aws_iam_policy_document.writer.json
}

output "raw_bucket" { value = aws_s3_bucket.raw.id }
output "warehouse_uri" { value = "s3://${aws_s3_bucket.warehouse.id}/warehouse" }
output "writer_policy_arn" { value = aws_iam_policy.writer.arn }

