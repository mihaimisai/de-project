#set up a scheduler that will trigger the Lambda every 20 minutes
resource "aws_cloudwatch_event_rule" "every_20_mins_scheduler" {
  name                = "every_20_mins_scheduler"
  description         = "Triggers every 20 minutes"
  schedule_expression = "rate(20 minutes)"
}
#set up trigger as schedule target 
resource "aws_cloudwatch_event_target" "trigger_lambda_every_20_mins" {
  rule      = aws_cloudwatch_event_rule.every_20_mins_scheduler.name
  target_id = var.lambda_1_name
  arn       = aws_lambda_function.ingested_lambda_function.arn
}
#set up permissions to invoke lambda
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingested_lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_20_mins_scheduler.arn
}