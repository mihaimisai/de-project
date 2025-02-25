
#set up a scheduler that will trigger the Lambda every 20 minutes
resource "aws_cloudwatch_event_rule" "every_20_minutes" {
  name                = "quote_getter_schedule"
  description         = "Triggers every 20 minutes"
  schedule_expression = "rate(20 minutes)"
}

#set up trigger as schedual target 
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.every_20_minutes.name
  target_id = "LambdaTarget" # to be confirmed
  arn       = aws_lambda_function.lambda_1.arn # to be confirmed
}
#set up permissions to invoke lambda
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_1.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_5_minutes.arn
}