
##### INGEST LAMBDA #####

data "archive_file" "ingest_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  output_path = "${path.module}/../packages/ingest/function.zip"
  source_dir      = "${path.module}/../src/ingest"
}

resource "aws_lambda_function" "ingest_lambda_function" {
  function_name = var.ingest_lambda
  source_code_hash = data.archive_file.ingest_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "ingest/function.zip"
  role = aws_iam_role.ingest_lambda_role.arn
  handler = "function.ingest_handler_fn.ingest_handler"
  runtime = var.python_runtime
  timeout = var.default_timeout
  # layers = [aws_lambda_layer_version.dependencies.arn]
  layers = [aws_lambda_layer_version.pandas_pyarrow.arn, aws_lambda_layer_version.pg8000.arn]
  environment {
    variables = {
      ingested_data_bucket = aws_s3_bucket.ingested_data_bucket.bucket
      timestamp_bucket = aws_s3_bucket.timestamp_bucket.bucket
      DB_HOST = "${var.db_host}"
      DB_PORT = "${var.db_port}"
      DB = "${var.db_db}"
      DB_USER = "${var.db_user}"
      DB_PASSWORD = "${var.db_password}"
    }
  }
  # depends_on = [aws_s3_object.lambda_code, aws_s3_object.ingest_layer]
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.pandas_pyarrow_layer, aws_s3_object.pg8000_layer]

}

##### TRANSFORM LAMBDA #####


data "archive_file" "transform_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  output_path = "${path.module}/../packages/transform/function.zip"
  source_dir      = "${path.module}/../src/transform" 
}

resource "aws_lambda_function" "transform_lambda_function" {
  function_name = var.transform_lambda
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "transform/function.zip"
  role = aws_iam_role.transform_lambda_role.arn
  handler = "function.transform_handler_fn.transform_handler"
  runtime = var.python_runtime
  timeout = var.default_timeout
  memory_size = 256
  # layers = [aws_lambda_layer_version.dependencies.arn]
  layers = [aws_lambda_layer_version.pandas_pyarrow.arn]
  environment {
    variables = {
      ingested_data_bucket = aws_s3_bucket.ingested_data_bucket.bucket
      transformed_data_bucket = aws_s3_bucket.transformed_data_bucket.bucket
    }
  }
  # depends_on = [aws_s3_object.lambda_code, aws_s3_object.transform_layer]
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.pandas_pyarrow_layer]

}

##### LOAD LAMBDA #####


data "archive_file" "load_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  output_path = "${path.module}/../packages/load/function.zip"
  source_dir      = "${path.module}/../src/load" 
}

resource "aws_lambda_function" "load_lambda_function" {
  function_name = var.load_lambda
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "load/function.zip"
  role = aws_iam_role.load_lambda_role.arn
  handler = "function.load_handler_fn.load_handler"
  runtime = var.python_runtime
  timeout = 600
  memory_size = 256
  # layers = [aws_lambda_layer_version.dependencies.arn]
  layers = [aws_lambda_layer_version.pandas_pyarrow.arn, aws_lambda_layer_version.pg8000.arn]
  environment {
    variables = {
      transformed_data_bucket = aws_s3_bucket.transformed_data_bucket.bucket

      DB_HOST_DW = "${var.db_host_dw}"
      DB_PORT_DW = "${var.db_port_dw}"
      DB_DW  = "${var.db_db_dw}"
      DB_USER_DW  = "${var.db_user_dw}"
      DB_PASSWORD_DW  = "${var.db_password_dw}"
    }
  }
  # depends_on = [aws_s3_object.lambda_code, aws_s3_object.load_layer]
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.pandas_pyarrow_layer, aws_s3_object.pg8000_layer]
}