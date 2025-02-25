# resource "aws_secretsmanager_secret" "db_secret" {
#   name = "db_credentials"
# }

# resource "aws_secretsmanager_secret_version" "db_secret_version" {
#   secret_id     = aws_secretsmanager_secret.db_secret.id
#   secret_string = jsonencode({
#     db_user    = var.db_user
#     db_password = var.db_password
#     db_host    = var.db_host
#     db_db      = var.db_db
#     db_port    = var.db_port
#   })
  
# }

# resource "aws_secretsmanager_secret" "email_secret" {
#   name = "log_email_credentials"
# }

# resource "aws_secretsmanager_secret_version" "email_secret_version" {
#   secret_id     = aws_secretsmanager_secret.email_secret.id
#   secret_string = jsonencode({
#     log_email    = var.log_email
#     log_email_pass = var.log_email_pass
#   })
# }
