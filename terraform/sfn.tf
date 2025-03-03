
resource "aws_sfn_state_machine" "sfn_state_machine_ingest_to_transform" {
  name_prefix = var.state_machine_name
  definition = templatefile("${path.module}/state-machine-definition.json",
    { aws_region        = "${data.aws_region.current.name}",
      aws_account_num   = "${data.aws_caller_identity.current.account_id}",
      ingestion_function_name     = "${var.ingestion_lambda}"
      transformation_function_name = "${var.transformation_lambda}"
    })
  role_arn = aws_iam_role.step_function_role.arn
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
}


resource "aws_iam_policy" "state_machine_policy" {
  name_prefix = "s3-policy-${var.state_machine_name}"
  policy      = data.aws_iam_policy_document.state_machine_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "state_machine_policy_attachment" {
  policy_arn = aws_iam_policy.state_machine_policy.arn
  role       = aws_iam_role.state_machine_iam_role.name
}
