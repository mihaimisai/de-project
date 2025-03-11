# buckets
resource "aws_s3_bucket" "timestamp_bucket" {
  bucket_prefix = var.timestamp_bucket_prefix
  force_destroy = true
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix  = var.code_bucket_prefix
  force_destroy = true
}

resource "aws_s3_bucket" "ingested_data_bucket" {
  bucket_prefix = var.ingested_data_bucket_prefix
  force_destroy = true
}

resource "aws_s3_bucket" "transformed_data_bucket" {
  bucket_prefix = var.transformed_data_bucket_prefix
  force_destroy = true
}


# objects
resource "aws_s3_object" "lambda_code" {
  for_each = toset(["ingest", "transform", "load"])
  bucket   = aws_s3_bucket.code_bucket.bucket
  key      = "${each.key}/function.zip"
  source   = "${path.module}/../packages/${each.key}/function.zip"
  etag     = filemd5("${path.module}/../packages/${each.key}/function.zip")
}


resource "aws_s3_object" "pandas_pyarrow_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/pandas_pyarrow_layer.zip"
  source = data.archive_file.pandas_pyarrow_layer_code.output_path
  etag = filemd5(data.archive_file.pandas_pyarrow_layer_code.output_path)
  depends_on = [ data.archive_file.pandas_pyarrow_layer_code ]
}

resource "aws_s3_object" "pg8000_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/pg8000_layer.zip"
  source = data.archive_file.pg8000_layer_code.output_path
  etag = filemd5(data.archive_file.pg8000_layer_code.output_path)
  depends_on = [ data.archive_file.pg8000_layer_code ]
}