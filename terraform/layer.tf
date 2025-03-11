
##### LAMBDA ONE #####

data "archive_file" "ingest_layer_code" {
  type = "zip"
  output_file_mode = "0666"
  output_path = "${path.module}/../packages/layers/ingest_layer.zip"
  source_dir = "${path.module}/../dependencies" 

}

# refactor when we know if all lambdas use same dependencies
# - s3 key currently only referencing ingestion layer
resource "aws_lambda_layer_version" "dependencies" {
  layer_name          = "ingest_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.ingest_layer.key
  source_code_hash = data.archive_file.ingest_layer_code.output_base64sha256
}


##### LAMBDA TWO #####

data "archive_file" "transform_layer_code" {
  type = "zip"
  output_path = "${path.module}/../packages/layers/transform_layer.zip"
  source_dir = "${path.module}/../dependencies"

}

##### LAMBDA THREE #####

data "archive_file" "load_layer_code" {
  type = "zip"
  output_path = "${path.module}/../packages/layers/load_layer.zip"
  source_dir = "${path.module}/../dependencies"

}
#Auto-create dependencies folder based on pip install requirements.txt
# resource "null_resource" "create_dependencies" {
#   provisioner "local-exec" {
#     command = "pip install -r ${path.module}/../requirements_aws.txt -t ${path.module}/../dependencies/python"
#   }

#   triggers = {
#     dependencies = filemd5("${path.module}/../requirements_aws.txt")
#   }
# }


# data "archive_file" "pandas_pyarrow_layer_code" {
#   type = "zip"
#   output_file_mode = "0666"
#   output_path = "${path.module}/../packages/layers/pandas_pyarrow_layer.zip"
#   source_dir = "${path.module}/../dependencies/pandas_pyarrow"
# }

# data "archive_file" "pg8000_layer_code" {
#   type = "zip"
#   output_file_mode = "0666"
#   output_path = "${path.module}/../packages/layers/pg8000_layer.zip"
#   source_dir = "${path.module}/../dependencies/pg8000"
# }

# resource "aws_lambda_layer_version" "pandas_pyarrow" {
#   layer_name          = "pandas_pyarrow_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = aws_s3_bucket.code_bucket.bucket
#   s3_key              = aws_s3_object.pandas_pyarrow_layer.key
#   source_code_hash = data.archive_file.pandas_pyarrow_layer_code.output_base64sha256
# }

# resource "aws_lambda_layer_version" "pg8000" {
#   layer_name          = "pg8000_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = aws_s3_bucket.code_bucket.bucket
#   s3_key              = aws_s3_object.pg8000_layer.key
#   source_code_hash = data.archive_file.pg8000_layer_code.output_base64sha256
# }