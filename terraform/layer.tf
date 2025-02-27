resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../lambda_requirements.txt -t ${path.module}/../dependencies/python"
  }

  triggers = {
    dependencies = filemd5("${path.module}/../lambda_requirements.txt")
  }
}

data "archive_file" "ingestion_layer_code" {
  type = "zip"
#   output_file_mode = "0666"
  output_path = "${path.module}/../packages/layers/ingestion_layer.zip"
  source_dir = "${path.module}/../dependencies" 

}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name          = "ingestion_lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_object.ingestion_layer.bucket
  s3_key              = aws_s3_object.ingestion_layer.key
  source_code_hash = data.archive_file.ingestion_layer_code.output_base64sha256
}