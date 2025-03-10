
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

data "archive_file" "transforma_layer_code" {
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

# for single layer used by all lambdas
# data "archive_file" "layer_code" {
#   type = "zip"
# #   output_file_mode = "0666"
#   output_path = "${path.module}/../packages/layers/layer.zip"
#   source_dir = "${path.module}/../dependencies" 
#   depends_on = [null_resource...]
# }