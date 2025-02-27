resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../requirements.txt -t ${path.module}/../dependencies/python"
  }

  triggers = {
    dependencies = filemd5("${path.module}/../requirements.txt")
  }
}

data "archive_file" "ingester_layer_code" {
  type = "zip"
#   output_file_mode = "0666"
  source_dir = "${path.module}/../dependencies" 
  output_path = "${path.module}/../packages/layers/ingester_layer.zip"
}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name          = "ingester_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.ingester_layer_code.key
}