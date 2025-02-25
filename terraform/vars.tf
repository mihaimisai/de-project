variable "state_bucket" { 
    type = string
    default = "de-project-terraform-state"
}
variable "ingested_data_bucket_prefix" {
    type = string
    default = "de-project-ingested-data-"
}
variable "code_bucket_prefix" {
    type = string
    default = "de-project-lambda-code-"
}

variable "timestamp_bucket_prefix" {
  type = string
    default = "de-project-lambda-timestamp-"
}
variable "lambda_1_name" {
    type = string
    default = "data_ingester_lambda"
}

variable "lambda_2_name" {
    type = string
    default = "data_processer_lambda"
}
variable "lambda_3_name" {
    type = string
    default = "data_storer_lambda"
}
variable "python_runtime" {
    type = string
    default = "python3.12"
}
variable "alert_email" {
    type = string
    default = ""# add email

}