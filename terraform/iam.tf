# ---------------
# Lambda IAM Role
# ---------------

# Define
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com", "states.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create
resource "aws_iam_role" "lambda_1_role" {
  name_prefix        = "role-${var.ingestion_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}


# ------------------------------
# Lambda IAM Policy for S3 Read and Write
# ------------------------------

# Define
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {

    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.timestamp_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.timestamp_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*",
      "${aws_s3_bucket.code_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:ListBucket"]
    resources = [
      "${aws_s3_bucket.timestamp_bucket.arn}"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "s3_read_write_policy" {
  name_prefix = "s3-policy-${var.ingestion_lambda}-read-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_read_write_policy_attachment" {
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
  role       = aws_iam_role.lambda_1_role.name
}


# # ------------------------------
# # Lambda IAM Policy for CloudWatch
# # ------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  statement {
    actions = [
      "logs:CreateLogGroup",
    
    ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
    effect    = "Allow"
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_lambda}:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-${var.ingestion_lambda}"
  policy      = data.aws_iam_policy_document.cw_document.json
}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  policy_arn = aws_iam_policy.cw_policy.arn
  role       = aws_iam_role.lambda_1_role.name
}
####################################################################################################
# ---------------
# Lambda 2 IAM Role
# ---------------

# Create
resource "aws_iam_role" "lambda_2_role" {
  name_prefix        = "role-${var.transformation_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ------------------------------
# Lambda IAM Policy for S3 Read and Write
# ------------------------------

# Define
data "aws_iam_policy_document" "s3_data_policy_doc_lambda_two" {
  statement {

    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.processed_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.processed_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*",
      "${aws_s3_bucket.code_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:ListBucket"]
    resources = [
      "${aws_s3_bucket.processed_bucket.arn}"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "s3_read_write_policy_lambda_two" {
  name_prefix = "s3-policy-${var.transformation_lambda}-read-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc_lambda_two.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_read_write_policy_attachment_lambda_two" {
  policy_arn = aws_iam_policy.s3_read_write_policy_lambda_two.arn
  role       = aws_iam_role.lambda_2_role.name
}


# # ------------------------------
# # Lambda IAM Policy for CloudWatch
# # ------------------------------

# Define
data "aws_iam_policy_document" "cw_document_lambda_two" {
  statement {
    actions = [
      "logs:CreateLogGroup",
    
    ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
    effect    = "Allow"
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transformation_lambda}:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "cw_policy_lambda_two" {
  name_prefix = "cw-policy-${var.transformation_lambda}"
  policy      = data.aws_iam_policy_document.cw_document_lambda_two.json
}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment_lambda_two" {
  policy_arn = aws_iam_policy.cw_policy_lambda_two.arn
  role       = aws_iam_role.lambda_2_role.name
}