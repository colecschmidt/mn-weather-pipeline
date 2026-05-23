resource "aws_iam_user" "poller" {
  name = "mn-weather-pipeline-poller"
}

resource "aws_iam_user_policy" "poller_s3" {
  name = "mn-weather-pipeline-s3-write"
  user = aws_iam_user.poller.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.raw.arn}/raw/*"
      }
    ]
  })
}

resource "aws_iam_access_key" "poller" {
  user = aws_iam_user.poller.name
}
