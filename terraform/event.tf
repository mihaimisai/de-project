#set up a scheduler that will trigger the Lambda every 20 minutes
resource "aws_cloudwatch_event_rule" "every_20_mins_scheduler" {
  name                = "every_20_mins_scheduler"
  description         = "Triggers every 20 minutes"
  schedule_expression = "rate(20 minutes)"
}
#set up trigger as schedule target 
resource "aws_cloudwatch_event_target" "trigger_state_machine_every_20_mins" {
  rule      = aws_cloudwatch_event_rule.every_20_mins_scheduler.name
  target_id = var.state_machine_name
  arn       = aws_sfn_state_machine.sfn_state_machine_ingest_to_transform.arn
  role_arn = aws_iam_role.state_machine_eventbridge_iam_role.arn
  input = jsonencode({})
}