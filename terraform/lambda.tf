data "archive_file" "ingester_lambda_zip" {
  type             = "zip"
  # output_file_mode = "0666"
  source_dir      = "${path.module}/../src/ingestion" 
  # output_path      = "${path.module}/../ingester_function.zip"
  output_path = "${path.module}/../packages/ingestion/ingestion_function.zip"
}

# data "archive_file" "ingester_lambda_layer" {
#   type = "zip"
#   output_file_mode = "0666"
#   source_dir = "${path.module}/../package_for_lambda_ingestion" 
#   output_path = "${path.module}/../ingester_lambda_layer.zip"
# }

# resource "aws_lambda_layer_version" "ingester_lambda_layer" {
#   layer_name          = "ingester_lambda_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = aws_s3_bucket.code_bucket.bucket
#   s3_key              = aws_s3_object.ingester_layer_code.key
# }
resource "aws_lambda_function" "ingested_lambda_function" {
  function_name = var.lambda_1_name
  source_code_hash = data.archive_file.ingester_lambda_zip.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "lambdas/ingester_function.zip"
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
  depends_on = [aws_s3_object.lingester_lambda_code, aws_s3_object.ingester_layer_code]

}