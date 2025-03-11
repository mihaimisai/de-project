resource "aws_cloudwatch_event_rule" "every_20_mins_scheduler" {
  name                = "every-20-mins-scheduler"
  description         = "triggers every 20 minutes"
  schedule_expression = "rate(20 minutes)"
}

resource "aws_cloudwatch_event_target" "trigger_state_machine_every_20_mins" {
  rule      = aws_cloudwatch_event_rule.every_20_mins_scheduler.name
  target_id = var.state_machine_name
  arn       = aws_sfn_state_machine.sfn_ingest_transform_load.arn
  role_arn = aws_iam_role.scheduler_iam_role.arn
  input = jsonencode({})
}

# --------------------
# Eventbridge IAM Role
# --------------------

resource "aws_iam_role" "scheduler_iam_role" {
  name_prefix        = "role-scheduler-eventbridge-"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ----------------------------------------
# Eventbridge Scheduler IAM Policy for sfn
# ----------------------------------------

# Define
data "aws_iam_policy_document" "scheduler_sfn_policy_doc" {
    statement {
            effect = "Allow"
            actions = ["states:StartExecution"]
            resources = [
                "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.state_machine_name}"
            ]
        }
}

# Create
resource "aws_iam_policy" "scheduler_sfn_policy" {
  name_prefix = "scheduler-${var.state_machine_name}-"
  policy      = data.aws_iam_policy_document.scheduler_sfn_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "scheduler_sfn_policy_attachment" {
  policy_arn = aws_iam_policy.scheduler_sfn_policy.arn
  role       = aws_iam_role.scheduler_iam_role.name
}