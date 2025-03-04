variable "db_user" { 
  type = string 
  sensitive = true
  }

variable "db_password" { 
  type = string 
  sensitive = true
  }

variable "db_host" { 
  type = string 
  sensitive = true
  }

variable "db_db" { 
  type = string 
  sensitive = true
  }

variable "db_port" { 
  type = number 
  sensitive = true
  }

variable "alert_email" {
    type = string
    sensitive = true 
}

variable "log_email_pass" {
  type = string
  sensitive = true
}

variable "ingested_data_bucket_prefix" {
    type = string
    default = "de-project-ingested-data-"
}

variable "processed_data_bucket_prefix" {
    type = string
    default = "de-project-processed-data-"
}

variable "code_bucket_prefix" {
    type = string
    default = "de-project-lambda-code-"
}

variable "timestamp_bucket_prefix" {
  type = string
    default = "de-project-lambda-timestamp-"
}

variable "ingestion_lambda" {
    type = string
    default = "ingestion_lambda"
}

variable "transformation_lambda" {
    type = string
    default = "transformation_lambda"
}

variable "load_lambda" {
    type = string
    default = "load_lambda"
}

variable "python_runtime" {
    type = string
    default = "python3.12"
}

variable "default_timeout" {
  type    = number
  default = 120
}
variable "state_machine_name" {
    type = string
    default = "sfn_ingest_to_transform"
}