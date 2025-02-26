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

# variable "log_email" {
#   type = string
# }

variable "alert_email" {
    type = string
    sensitive = true
    # default = "NotificationStreamBanshee@Gmail.com"   
}

variable "log_email_pass" {
  type = string
  sensitive = true
}


# variable "state_bucket" { 
#     type = string
#     default = "de-project-terraform-state"
# }
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

# variable "lambda_2_name" {
#     type = string
#     default = "data_processer_lambda"
# }

# variable "lambda_3_name" {
#     type = string
#     default = "data_storer_lambda"
# }

variable "python_runtime" {
    type = string
    default = "python3.12"
}