terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Raw data bucket
resource "aws_s3_bucket" "raw_data" {
  bucket = "soccer-data-platform-raw-demo"
}

# Processed data bucket
resource "aws_s3_bucket" "processed_data" {
  bucket = "soccer-data-platform-processed-demo"
}

# Analytics bucket
resource "aws_s3_bucket" "analytics_data" {
  bucket = "soccer-data-platform-analytics-demo"
}