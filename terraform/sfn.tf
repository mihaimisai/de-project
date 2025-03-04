
resource "aws_sfn_state_machine" "sfn_state_machine_ingest_to_transform" {
  name = var.state_machine_name
  definition = templatefile("${path.module}/state-machine-definition.json",
    { aws_region        = "${data.aws_region.current.name}",
      aws_account_num   = "${data.aws_caller_identity.current.account_id}",
      ingestion_function_name     = "${var.ingestion_lambda}"
      transformation_function_name = "${var.transformation_lambda}"
    })
  role_arn = aws_iam_role.state_machine_iam_role.arn
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.log_group_for_sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

###########

resource "aws_iam_role" "state_machine_iam_role" {
  name_prefix        = "role-${var.state_machine_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

data "aws_iam_policy_document" "state_machine_policy_doc" {
    #"Version": "2012-10-17",
    statement {
            effect = "Allow"
            actions = ["lambda:InvokeFunction"]
            resources = [
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingestion_lambda}:*",
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transformation_lambda}:*"
            ]
        }
    statement {
            effect = "Allow"
            actions = ["lambda:InvokeFunction"]
            resources = [
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingestion_lambda}",
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transformation_lambda}"
            ]
        }
    statement  {
            effect = "Allow",
            actions = [
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
                "xray:GetSamplingRules",
                "xray:GetSamplingTargets"
            ],
            resources = [
                "*"
            ]
        }
}


resource "aws_iam_policy" "state_machine_policy" {
  name_prefix = "s3-policy-${var.state_machine_name}"
  policy      = data.aws_iam_policy_document.state_machine_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "state_machine_policy_attachment" {
  policy_arn = aws_iam_policy.state_machine_policy.arn
  role       = aws_iam_role.state_machine_iam_role.name
}

######
resource "aws_iam_role" "state_machine_eventbridge_iam_role" {
  name_prefix        = "role-scheduler-eventbridge-"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

data "aws_iam_policy_document" "eventbridge_policy_doc" {
    statement {
            effect = "Allow"
            actions = ["states:StartExecution"]
            resources = [
                "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.state_machine_name}"
            ]
        }
}

resource "aws_iam_policy" "eventbridge_policy" {
  name_prefix = "scheduler-eventbridge-policy-"
  policy      = data.aws_iam_policy_document.eventbridge_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "eventbridge_policy_attachment" {
  policy_arn = aws_iam_policy.eventbridge_policy.arn
  role       = aws_iam_role.state_machine_eventbridge_iam_role.name
}

