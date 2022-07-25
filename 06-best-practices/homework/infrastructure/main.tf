# Make sure to create state bucket beforehand
# terraform {
#   required_version = ">= 1.0"
#   backend "s3" {
#     bucket  = "tf-state-mlops-zoomcamp"
#     key     = "mlops-zoomcamp-stg.tfstate"
#     region  = "eu-west-1"
#     encrypt = true
#   }
# }

provider "aws" {

  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-east-1"


  # only required for non virtual hosted-style endpoint use case.
  # https://registry.terraform.io/providers/hashicorp/aws/latest/docs#s3_force_path_style
  s3_use_path_style         = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
	endpoints {
    s3             = "http://localhost:4566"
    apigateway     = "http://localhost:4566"
    apigatewayv2   = "http://localhost:4566"
    cloudformation = "http://localhost:4566"
    cloudwatch     = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    ec2            = "http://localhost:4566"
    es             = "http://localhost:4566"
    elasticache    = "http://localhost:4566"
    firehose       = "http://localhost:4566"
    iam            = "http://localhost:4566"
    kinesis        = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    rds            = "http://localhost:4566"
    redshift       = "http://localhost:4566"
    route53        = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
    ses            = "http://localhost:4566"
    sns            = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    ssm            = "http://localhost:4566"
    stepfunctions  = "http://localhost:4566"
    sts            = "http://localhost:4566"
	}
}

# provider "aws" {
#   region = var.aws_region
# }

data "aws_caller_identity" "current_identity" {}

locals {
  account_id = data.aws_caller_identity.current_identity.account_id
}

# ride_events
module "source_kinesis_stream" {
  source = "./modules/kinesis"
  retention_period = 48
  shard_count = 2
  stream_name = "${var.source_stream_name}-${var.project_id}"
  tags = var.project_id
}

# ride_predictions
module "output_kinesis_stream" {
  source = "./modules/kinesis"
  retention_period = 48
  shard_count = 2
  stream_name = "${var.output_stream_name}-${var.project_id}"
  tags = var.project_id
}

# model bucket
module "s3_bucket" {
  source = "./modules/s3"
  bucket_name = "${var.model_bucket}-${var.project_id}"
}

#image registry
module "ecr_image" {
   source = "./modules/ecr"
   ecr_repo_name = "${var.ecr_repo_name}_${var.project_id}"
   account_id = local.account_id
   lambda_function_local_path = var.lambda_function_local_path
   docker_image_local_path = var.docker_image_local_path
}

module "lambda_function" {
  source = "./modules/lambda"
  image_uri = module.ecr_image.image_uri
  lambda_function_name = "${var.lambda_function_name}_${var.project_id}"
  model_bucket = module.s3_bucket.name
  output_stream_name = "${var.output_stream_name}-${var.project_id}"
  output_stream_arn = module.output_kinesis_stream.stream_arn
  source_stream_name = "${var.source_stream_name}-${var.project_id}"
  source_stream_arn = module.source_kinesis_stream.stream_arn
}
