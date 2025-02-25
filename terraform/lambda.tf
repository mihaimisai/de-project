data "archive_file" "ingester_lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir      = "${path.module}/../lambda_function_ingestion" 
  output_path      = "${path.module}/../ingester_function.zip"
}

data "archive_file" "ingester_lambda_layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../package_for_lambda_ingestion" 
  output_path = "${path.module}/../ingester_lambda_layer.zip"
}

resource "aws_lambda_layer_version" "ingester_lambda_layer" {
  layer_name          = "ingester_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.ingester_layer_code.key
}
resource "aws_lambda_function" "ingested_lambda_function" {
  function_name = "${var.lambda_1_name}"
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "lambda/ingester_lambda.zip"
  role = aws_iam_role.lambda_1_role.arn
  handler = "lambda_function_ingestion.lambda_function.process_all_tables"
  runtime = var.python_runtime

  layers = [aws_lambda_layer_version.ingester_lambda_layer.arn]
}