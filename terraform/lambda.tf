data "archive_file" "ingestion_lambda" {
  type             = "zip"
  output_path = "${path.module}/../packages/ingestion/function.zip"
  source_dir      = "${path.module}/../src/ingestion"
}

resource "aws_lambda_function" "ingested_lambda_function" {
  function_name = var.ingestion_lambda
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "ingestion/function.zip"
  role = aws_iam_role.lambda_1_role.arn
  # adjust handler to match
  handler = "${var.ingestion_lambda}.ingestion_handler.ingestion_handler"
  runtime = var.python_runtime
  timeout = var.default_timeout
  layers = [aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {
      ingested_data_bucket = aws_s3_bucket.data_bucket.bucket
      timestamp_bucket = aws_s3_bucket.timestamp_bucket.bucket
    }
  }
  depends_on = [aws_s3_object.ingestion_lambda_code, aws_s3_object.ingestion_layer]
}