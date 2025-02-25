resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = var.ingested_data_bucket_prefix
  tags = {
    Name        = "data bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "timestamp_bucket" {
  bucket_prefix = var.timestamp_bucket_prefix
  tags = {
    Name        = "timestamp of last lambda call bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix  = var.code_bucket_prefix
  tags = {
    Name        = "lambda code bucket"
    Environment = "Dev"
  }
}

#TODO: Upload the ingester_lambda code to the code_bucket
resource "aws_s3_object" "ingester_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "lambda/ingester_lambda.zip"
  source = "${path.module}/../ingester_function.zip" 
}

#TODO: Upload the ingester_lambda layer to the code_bucket if exists