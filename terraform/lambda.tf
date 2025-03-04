
##### LAMBDA ONE #####

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
  handler = "function.ingestion_handler_fn.ingestion_handler"
  runtime = var.python_runtime
  timeout = var.default_timeout
  layers = [aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {
      ingested_data_bucket = aws_s3_bucket.data_bucket.bucket
      timestamp_bucket = aws_s3_bucket.timestamp_bucket.bucket
      DB_HOST = "${var.db_host}"
      DB_PORT = "${var.db_port}"
      DB = "${var.db_db}"
      DB_USER = "${var.db_user}"
      DB_PASSWORD = "${var.db_password}"
    }
  }
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.ingestion_layer]
}

##### LAMBDA TWO #####


data "archive_file" "transformation_lambda" {
  type             = "zip"
  output_path = "${path.module}/../packages/transformation/function.zip"
  source_dir      = "${path.module}/../src/transform" 
}

resource "aws_lambda_function" "transformation_lambda_function" {
  function_name = var.transformation_lambda
  source_code_hash = data.archive_file.transformation_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "transformation/function.zip"
  role = aws_iam_role.lambda_2_role.arn
  # adjust handler to match
  handler = "function.transformation_handler_fn.transformation_handler"
  runtime = var.python_runtime
  timeout = var.default_timeout
  layers = [aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {
      ingested_data_bucket = aws_s3_bucket.data_bucket.bucket
      processed_data_bucket = aws_s3_bucket.processed_bucket.bucket
    }
  }
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.transformation_layer]
}

##### LAMBDA THREE #####


data "archive_file" "load_lambda" {
  type             = "zip"
  output_path = "${path.module}/../packages/load/function.zip"
  source_dir      = "${path.module}/../src/load" 
}

resource "aws_lambda_function" "load_lambda_function" {
  function_name = var.load_lambda
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "load/function.zip"
  role = aws_iam_role.lambda_3_role.arn
  # fix role name
  handler = "function.load_handler_fn.load_handler"
  runtime = var.python_runtime
  timeout = var.default_timeout
  layers = [aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {
      processed_data_bucket = aws_s3_bucket.processed_bucket.bucket
    }
  }
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.load_layer]
}