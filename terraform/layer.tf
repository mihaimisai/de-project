data "archive_file" "pandas_pyarrow_layer_code" {
  type = "zip"
  output_file_mode = "0666"
  output_path = "${path.module}/../packages/layers/pandas_pyarrow_layer.zip"
  source_dir = "${path.module}/../dependencies/pandas_pyarrow"
}

data "archive_file" "pg8000_layer_code" {
  type = "zip"
  output_file_mode = "0666"
  output_path = "${path.module}/../packages/layers/pg8000_layer.zip"
  source_dir = "${path.module}/../dependencies/pg8000"
}

resource "aws_lambda_layer_version" "pandas_pyarrow" {
  layer_name          = "pandas_pyarrow_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.pandas_pyarrow_layer.key
  source_code_hash = data.archive_file.pandas_pyarrow_layer_code.output_base64sha256
}

resource "aws_lambda_layer_version" "pg8000" {
  layer_name          = "pg8000_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.pg8000_layer.key
  source_code_hash = data.archive_file.pg8000_layer_code.output_base64sha256
}