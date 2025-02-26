resource "aws_sns_topic" "lambda_errors_topic" {
  name = "lambda-errors-topic"
}

resource "aws_sns_topic_subscription" "email_notification" {
  topic_arn = aws_sns_topic.lambda_errors_topic.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_group" "aws_cw_log_access" {
  name = "/aws/lambda/lambda_handler"
}

resource "aws_cloudwatch_log_metric_filter" "lambda_error_filter" {
    name           = "lambda-error-filter"
    log_group_name = aws_cloudwatch_log_group.aws_cw_log_access.name

    pattern = "ERROR"

 metric_transformation {
    name      = "LambdaErrorCount"
    namespace = "LambdaErrors"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  alarm_name          = "LambdaErrorNotifier"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "LambdaErrorCount"
  namespace          = "LambdaErrors"
  period             = 300
  statistic          = "Sum"
  threshold          = 1
  alarm_description  = "Triggers when Lambda logs contain errors"
  alarm_actions      = [aws_sns_topic.lambda_errors_topic.arn]
}

