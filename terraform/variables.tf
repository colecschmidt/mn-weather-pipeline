variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-2"
}

variable "bucket_name" {
  description = "S3 bucket name for the raw weather data lake"
  type        = string
  default     = "mn-weather-pipeline-raw"
}

variable "raw_retention_days" {
  description = "Days before raw objects transition to S3 Glacier Instant Retrieval"
  type        = number
  default     = 90
}
