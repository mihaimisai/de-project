# ----------------
# IAM Trust Policy
# ----------------

# Define
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = [
        "lambda.amazonaws.com", 
        "states.amazonaws.com", 
        "events.amazonaws.com"
        ]
    }

    actions = ["sts:AssumeRole"]
  }
}

# ----------------
# Lambda IAM Roles
# ----------------

# Create
resource "aws_iam_role" "ingest_lambda_role" {
  name_prefix        = "role-${var.ingest_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

resource "aws_iam_role" "transform_lambda_role" {
  name_prefix        = "role-${var.transform_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "role-${var.load_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}


# ---------------------------------
# Ingest Lambda IAM Policies for S3
# ---------------------------------

# Define
data "aws_iam_policy_document" "ingest_s3_policy_doc" {
  statement {

    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.timestamp_bucket.arn}/*",
      "${aws_s3_bucket.ingested_data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.timestamp_bucket.arn}/*",
      "${aws_s3_bucket.ingested_data_bucket.arn}/*",
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
resource "aws_iam_policy" "ingest_s3_policy" {
  name_prefix = "s3-policy-${var.ingest_lambda}-read-write"
  policy      = data.aws_iam_policy_document.ingest_s3_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "ingest_s3_policy_attachment" {
  policy_arn = aws_iam_policy.ingest_s3_policy.arn
  role       = aws_iam_role.ingest_lambda_role.name
}

# ----------------------------------
# Transform Lambda IAM Policy for S3
# ----------------------------------

# Define
data "aws_iam_policy_document" "transform_s3_policy_doc" {
  statement {

    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.transformed_data_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.ingested_data_bucket.arn}/*",
      "${aws_s3_bucket.code_bucket.arn}/*"
    ]
    effect = "Allow"
  }  
  statement {

    actions = ["s3:ListBucket"]
    resources = [
      "${aws_s3_bucket.ingested_data_bucket.arn}",
      "${aws_s3_bucket.transformed_data_bucket.arn}"
    ]
  effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "transform_s3_policy" {
  name_prefix = "s3-policy-${var.transform_lambda}-read-write"
  policy      = data.aws_iam_policy_document.transform_s3_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "transform_s3_policy_attachment" {
  policy_arn = aws_iam_policy.transform_s3_policy.arn
  role       = aws_iam_role.transform_lambda_role.name
}


# -----------------------------
# Load Lambda IAM Policy for S3
# -----------------------------

# Define
data "aws_iam_policy_document" "load_s3_policy_doc" {
  statement {

    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.transformed_data_bucket.arn}/*",
      "${aws_s3_bucket.code_bucket.arn}/*"
    ]
    effect = "Allow"
  }
  statement {

    actions = ["s3:ListBucket"]
    resources = [
      "${aws_s3_bucket.transformed_data_bucket.arn}"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "load_s3_policy" {
  name_prefix = "s3-policy-${var.load_lambda}-read"
  policy      = data.aws_iam_policy_document.load_s3_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "load_s3_policy_attachment" {
  policy_arn = aws_iam_policy.load_s3_policy.arn
  role       = aws_iam_role.load_lambda_role.name
}

# --------------------------------
# Lambda IAM Policy for CloudWatch
# --------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  for_each = toset(["ingest", "transform", "load"])
  statement {
    actions = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      ]
    effect    = "Allow"
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${each.key}_lambda:*"
    ]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "ingest_cw_policy" {
  name_prefix = "cw-policy-${var.ingest_lambda}"
  policy      = data.aws_iam_policy_document.cw_document["ingest"].json
}
# Attach
resource "aws_iam_role_policy_attachment" "cw_policy_attachment" {
  policy_arn = aws_iam_policy.ingest_cw_policy.arn
  role       = aws_iam_role.ingest_lambda_role.name
}

# Create
resource "aws_iam_policy" "transform_cw_policy" {
  name_prefix = "cw-policy-${var.transform_lambda}"
  policy      = data.aws_iam_policy_document.cw_document["transform"].json
}
# Attach
resource "aws_iam_role_policy_attachment" "transform_cw_policy_attachment" {
  policy_arn = aws_iam_policy.transform_cw_policy.arn
  role       = aws_iam_role.transform_lambda_role.name
}

# Create
resource "aws_iam_policy" "load_cw_policy" {
  name_prefix = "cw-policy-${var.load_lambda}"
  policy      = data.aws_iam_policy_document.cw_document["load"].json
}
# Attach
resource "aws_iam_role_policy_attachment" "load_cw_policy_attachment" {
  policy_arn = aws_iam_policy.load_cw_policy.arn
  role       = aws_iam_role.load_lambda_role.name
}