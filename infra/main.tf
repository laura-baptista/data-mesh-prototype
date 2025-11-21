provider "aws" {
  region = "us-east-1"
  profile = "data-mesh"
}

# -------------------------------
# 1. Buckets por domínio
# -------------------------------
resource "aws_s3_bucket" "clients" {
  bucket = "data-mesh-clients"
  force_destroy = true
}

resource "aws_s3_bucket" "sales" {
  bucket = "data-mesh-sales"
  force_destroy = true
}

resource "aws_s3_bucket" "shared" {
  bucket = "data-mesh-shared"
  force_destroy = true
}

# -------------------------------
# 2. IAM User para o projeto
# -------------------------------
resource "aws_iam_user" "mesh_user" {
  name = "data-mesh-prototype-user"
}

# -------------------------------
# 3. Policy mínima
# -------------------------------
data "aws_iam_policy_document" "s3_policy" {
  statement {
    effect = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      aws_s3_bucket.clients.arn,
      aws_s3_bucket.sales.arn,
      aws_s3_bucket.shared.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.clients.arn}/*",
      "${aws_s3_bucket.sales.arn}/*",
      "${aws_s3_bucket.shared.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "mesh_policy" {
  name   = "data-mesh-s3-policy"
  policy = data.aws_iam_policy_document.s3_policy.json
}

resource "aws_iam_user_policy_attachment" "attach_policy" {
  user       = aws_iam_user.mesh_user.name
  policy_arn = aws_iam_policy.mesh_policy.arn
}

# -------------------------------
# 4. Access keys para uso local
# -------------------------------
resource "aws_iam_access_key" "mesh_user_key" {
  user = aws_iam_user.mesh_user.name
}

output "aws_access_key_id" {
  value     = aws_iam_access_key.mesh_user_key.id
  sensitive = true
}

output "aws_secret_access_key" {
  value     = aws_iam_access_key.mesh_user_key.secret
  sensitive = true
}
