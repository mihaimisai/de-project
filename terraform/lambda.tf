data "archive_file" "ingestion_lambda" {
  type             = "zip"
  output_path = "${path.module}/../packages/ingestion/function.zip"
  source_dir      = "${path.module}/../src/ingestion"
  # dir instead of file 
}

resource "aws_lambda_function" "ingested_lambda_function" {
  function_name = var.lambda_1_name
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "${var.lambda_1_name}/function.zip"
  role = aws_iam_role.lambda_1_role.arn
  # adjust handler to match
  handler = "ingestion_handler.ingestion_handler"
  runtime = var.python_runtime
  layers = [aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {
      ingested_data_bucket = aws_s3_bucket.data_bucket.bucket
      timestamp_bucket = aws_s3_bucket.timestamp_bucket.bucket
    }
  }
  depends_on = [aws_s3_object.ingestion_lambda_code, aws_s3_object.ingestion_layer_code]
}