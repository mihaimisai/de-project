# totesys database credentials
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


# s3 buckets
variable "ingested_data_bucket_prefix" {
    type = string
    default = "de-project-ingested-data-"
}

variable "transformed_data_bucket_prefix" {
    type = string
    default = "de-project-transformed-data-"
}

variable "code_bucket_prefix" {
    type = string
    default = "de-project-lambda-code-"
}

variable "timestamp_bucket_prefix" {
  type = string
    default = "de-project-lambda-timestamp-"
}


# lambdas
variable "ingest_lambda" {
    type = string
    default = "ingest-lambda"
}

variable "transform_lambda" {
    type = string
    default = "transform-lambda"
}

variable "load_lambda" {
    type = string
    default = "load-lambda"
}

variable "python_runtime" {
    type = string
    default = "python3.12"
}

variable "default_timeout" {
  type    = number
  default = 120
}


# sfn
variable "state_machine_name" {
    type = string
    default = "sfn-ingest-transform-load"
}


# data warehouse credentials
variable "db_host_dw" { 
  type = string 
  sensitive = true
  }

variable "db_db_dw" { 
  type = string 
  sensitive = true
  }

variable "db_port_dw" { 
  type = number 
  sensitive = true
  }

variable "db_user_dw" { 
  type = string 
  sensitive = true
  }

variable "db_password_dw" { 
  type = string 
  sensitive = true
  }