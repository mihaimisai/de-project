# buckets
resource "aws_s3_bucket" "timestamp_bucket" {
  bucket_prefix = var.timestamp_bucket_prefix
  force_destroy = true
  # tags = {
  #   Name        = "timestamp of last lambda call bucket"
  # }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix  = var.code_bucket_prefix
  force_destroy = true
  # tags = {
  #   Name        = "lambda code bucket"
  # }
}

resource "aws_s3_bucket" "ingested_data_bucket" {
  bucket_prefix = var.ingested_data_bucket_prefix
  force_destroy = true
  # tags = {
  #   Name        = "ingested data bucket"
  # }
}

resource "aws_s3_bucket" "transformed_data_bucket" {
  bucket_prefix = var.transformed_data_bucket_prefix
  force_destroy = true
  # tags = {
  #   Name        = "transformed data bucket"
  # }
}


# objects
resource "aws_s3_object" "lambda_code" {
  for_each = toset(["ingest", "transform", "load"])
  bucket   = aws_s3_bucket.code_bucket.bucket
  key      = "${each.key}/function.zip"
  source   = "${path.module}/../packages/${each.key}/function.zip"
  etag     = filemd5("${path.module}/../packages/${each.key}/function.zip")
}


# once lambda code complete - can be combined if all lambdas have same dependencies

# Upload the ingester_lambda layer to the code_bucket if exists
resource "aws_s3_object" "ingest_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/ingest_layer.zip"
  source = data.archive_file.ingest_layer_code.output_path
  etag = filemd5(data.archive_file.ingest_layer_code.output_path)
  depends_on = [ data.archive_file.ingest_layer_code ]
}

# Upload the transformation_lambda layer to the code_bucket if exists
resource "aws_s3_object" "transform_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/transform_layer.zip"
  source = data.archive_file.transform_layer_code.output_path
  etag = filemd5(data.archive_file.transform_layer_code.output_path)
  depends_on = [ data.archive_file.transform_layer_code ]
}

resource "aws_s3_object" "load_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "layers/load_layer.zip"
  source = data.archive_file.load_layer_code.output_path
  etag = filemd5(data.archive_file.load_layer_code.output_path)
  depends_on = [ data.archive_file.load_layer_code ]
}

# for all lambdas using one layer
# resource "aws_s3_object" "layer" {
#   bucket = aws_s3_bucket.code_bucket.bucket
#   key = "layers/layer.zip"
#   source = data.archive_file.layer_code.output_path
#   etag = filemd5(data.archive_file.layer_code.output_path)
#   depends_on = [ data.archive_file.layer_code ]
# }