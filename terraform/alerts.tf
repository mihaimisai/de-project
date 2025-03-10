resource "aws_sns_topic" "alert_topic" {
  name = "de_project_alert_topic"
}

resource "aws_sns_topic_subscription" "alert_subscription" {
  topic_arn = aws_sns_topic.alert_topic.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_metric_filter" "lambda_error_filter" {
  for_each = toset(["ingest", "transform", "load"])
  name           = "${each.key}_lambda_error_filter"
  log_group_name = "/aws/lambda/${each.key}_lambda"
  pattern = "ERROR"

 metric_transformation {
    name      = "${each.key}LambdaErrorCount"
    namespace = "DE_Project_Lambda_Errors"
    value     = 1
    default_value = 0
  }
  
  depends_on = [
    aws_lambda_function.ingest_lambda_function,
    aws_lambda_function.transform_lambda_function,
    aws_lambda_function.load_lambda_function,
  ]
}

resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  for_each = toset(["ingest", "transform", "load"])
  alarm_name          = "${each.key}LambdaErrorNotifier"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "${each.key}LambdaErrorCount"
  namespace          = "DE_Project_Lambda_Errors"
  period             = 300
  statistic          = "Sum"
  threshold          = 1
  alarm_description  = "Triggers when ${each.key} Lambda logs contain errors"
  alarm_actions      = [aws_sns_topic.alert_topic.arn]
}
