data "archive_file" "ingester_lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/remove-this-file.py" # modify path
  output_path      = "${path.module}/../ingester_function.zip"
}

resource "aws_lambda_function" "ingested_lambda_function" {
  function_name = "${var.lambda_1_name}"
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "lambda/ingester_lambda.zip"
  role = aws_iam_role.lambda_role.arn
  handler = "" # modify function entry point
  runtime = var.python_runtime

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.ingested_data_bucket.bucket
    }
  }
}