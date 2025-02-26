# ---------------
# Lambda IAM Role
# ---------------

# Define
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-${var.lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}


# ------------------------------
# Lambda IAM Policy for S3 Write
# ------------------------------

# Define



data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {

    actions = [
      "s3:PutObject",
      "s3:PutObjectACL"
    ]
    resources = [
      "${aws_s3_bucket.data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.lambda_name}-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  #TODO: attach the s3 write policy to the lambda role
  policy_arn = aws_iam_policy.s3_write_policy.arn
  role       = aws_iam_role.lambda_role.name
}


# # ------------------------------
# # Lambda IAM Policy for CloudWatch
# # ------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  statement {
    #TODO: this statement should give permission to create Log Groups in your account
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
    effect    = "Allow"
  }

  statement {
    #TODO: this statement should give permission to create Log Streams and put Log Events in the lambda's own Log Group
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:*:*:log-group:/aws/lambda/${var.lambda_name}:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "cw_policy" {
  #TODO: use the policy document defined above
  name_prefix = "cw-policy-${var.lambda_name}"
  policy      = data.aws_iam_policy_document.cw_document.json
}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  policy_arn = aws_iam_policy.cw_policy.arn
  role       = aws_iam_role.lambda_role.name
}

