resource "aws_sns_topic" "lambda_errors_topic" {
  name = "lambda_errors_topic"
}

resource "aws_sns_topic_subscription" "email_notification" {
  topic_arn = aws_sns_topic.lambda_errors_topic.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_metric_filter" "lambda_error_filter" {
  for_each = toset(["ingestion", "transformation", "load"])
  name           = "${each.key}_lambda_error_filter"
  log_group_name = "/aws/lambda/${each.key}_lambda"
  pattern = "ERROR"

 metric_transformation {
    name      = "${each.key}LambdaErrorCount"
    namespace = "${each.key}_Errors"
    value     = 1
    default_value = 0
  }
  depends_on = [
    aws_lambda_function.ingested_lambda_function,
    aws_lambda_function.transformation_lambda_function,
    aws_lambda_function.load_lambda_function,
  ]
}

resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  for_each = toset(["ingestion", "transformation", "load"])
  alarm_name          = "${each.key}LambdaErrorNotifier"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "${each.key}LambdaErrorCount"
  namespace          = "${each.key}_Errors"
  period             = 300
  statistic          = "Sum"
  threshold          = 1
  alarm_description  = "Triggers when ${each.key} Lambda logs contain errors"
  alarm_actions      = [aws_sns_topic.lambda_errors_topic.arn]
}



# resource "aws_sns_topic" "lambda_errors_topic" {
#   name = "lambda_errors_topic"
# }

# resource "aws_sns_topic_subscription" "email_notification" {
#   topic_arn = aws_sns_topic.lambda_errors_topic.arn
#   protocol  = "email"
#   endpoint  = var.alert_email
# }

# resource "aws_cloudwatch_log_metric_filter" "lambda_error_filter" {
#   name           = "lambda_error_filter"
#   log_group_name = "/aws/lambda/${var.ingestion_lambda}"
#   pattern = "ERROR"

#  metric_transformation {
#     name      = "LambdaErrorCount"
#     namespace = "${var.ingestion_lambda}_Errors"
#     value     = 1
#     default_value = 0
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
#   alarm_name          = "LambdaErrorNotifier"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = 1
#   metric_name         = "LambdaErrorCount"
#   namespace          = "${var.ingestion_lambda}_Errors"
#   period             = 300
#   statistic          = "Sum"
#   threshold          = 1
#   alarm_description  = "Triggers when Lambda logs contain errors"
#   alarm_actions      = [aws_sns_topic.lambda_errors_topic.arn]
# }