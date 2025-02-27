resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = var.ingested_data_bucket_prefix
  tags = {
    Name        = "ingested data bucket"
  }
}

resource "aws_s3_bucket" "timestamp_bucket" {
  bucket_prefix = var.timestamp_bucket_prefix
  tags = {
    Name        = "timestamp of last lambda call bucket"
  }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix  = var.code_bucket_prefix
  tags = {
    Name        = "lambda code bucket"
  }
}

# Upload the ingester_lambda code to the code_bucket
resource "aws_s3_object" "ingester_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "lambdas/ingester_function.zip"
  # source = "${path.module}/../ingester_function.zip"
  # source = "${path.module}/../packages/ingester_function.zip"
  source = data.archive_file.ingester_lambda_zip.output_path
}

# Upload the ingester_lambda layer to the code_bucket if exists
resource "aws_s3_object" "ingester_layer_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/ingester_layer.zip"
  source = data.archive_file.ingester_layer_code_zip.output_path
  # source = "${path.module}/../ingester_lambda_layer.zip"
}