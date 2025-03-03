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

resource "aws_s3_bucket" "processed_bucket" {
  bucket_prefix = var.processed_data_bucket_prefix
  tags = {
    Name        = "processed data bucket"
  }
}

variable "processed_data_bucket_prefix" { #MOVE TO .vars FILE <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    type = string
    default = "de-project-processed-data-"
}

# Upload the ingester_lambda code to the code_bucket
resource "aws_s3_object" "ingestion_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "ingestion/function.zip"
  source = data.archive_file.ingestion_lambda.output_path
  etag = filemd5(data.archive_file.ingestion_lambda.output_path)
}

# Upload the ingester_lambda layer to the code_bucket if exists
resource "aws_s3_object" "ingestion_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/ingestion_layer.zip"
  source = data.archive_file.ingestion_layer_code.output_path
  etag = filemd5(data.archive_file.ingestion_layer_code.output_path)
  depends_on = [ data.archive_file.ingestion_layer_code ]
}