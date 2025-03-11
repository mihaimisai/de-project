
resource "aws_sfn_state_machine" "sfn_ingest_transform_load" {
  name = var.state_machine_name
  definition = templatefile("${path.module}/state-machine-definition.json",
    { aws_region        = "${data.aws_region.current.name}",
      aws_account_num   = "${data.aws_caller_identity.current.account_id}",
      ingest_function_name     = "${var.ingest_lambda}",
      transform_function_name = "${var.transform_lambda}",
      load_function_name = "${var.load_lambda}"
    })
  role_arn = aws_iam_role.state_machine_iam_role.arn
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.log_group_for_sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

resource "aws_cloudwatch_log_group" "log_group_for_sfn" {
  name = "/aws/sfn/ingest_transform_load"
}

# ----------------------
# State Machine IAM Role
# ----------------------

# create
resource "aws_iam_role" "state_machine_iam_role" {
  name_prefix        = "role-${var.state_machine_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ------------------------
# State Machine IAM Policy
# ------------------------

# Define
data "aws_iam_policy_document" "sfn_policy_doc" {
  statement {
    effect = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingest_lambda}:*",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda}:*",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda}:*"
    ]
  }

  statement  {
    effect = "Allow"
    actions = [
        "xray:PutTraceSegments",
        "xray:PutTelemetryRecords",
        "xray:GetSamplingRules",
        "xray:GetSamplingTargets"
    ]
    resources = ["*"]
  }

  statement {
    actions = [
        "logs:CreateLogDelivery",
        "logs:CreateLogStream",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutLogEvents",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups"
        ]
    resources = ["*"]
    effect = "Allow"
    }
}

# Create
resource "aws_iam_policy" "sfn_policy" {
  name_prefix = "lambda-policy-${var.state_machine_name}"
  policy      = data.aws_iam_policy_document.sfn_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "sfn_policy_attachment" {
  policy_arn = aws_iam_policy.sfn_policy.arn
  role       = aws_iam_role.state_machine_iam_role.name
}