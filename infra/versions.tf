terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Optional: set TF_VAR_aws_profile or aws_profile in terraform.tfvars.
  # Leave empty to use the default AWS credential chain (env vars, shared config).
  profile = var.aws_profile != "" ? var.aws_profile : null
}
