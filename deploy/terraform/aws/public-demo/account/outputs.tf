output "zone_id" {
  description = "Route53 hosted zone ID of the demo subdomain; pass it to ../environment as route53_zone_id."
  value       = aws_route53_zone.demo.zone_id
}

output "zone_name" {
  description = "The demo subdomain."
  value       = aws_route53_zone.demo.name
}

output "name_servers" {
  description = "NS records to add for zone_name in the parent domain's DNS, which delegates the subdomain to this account. Nothing resolves until they are added."
  value       = aws_route53_zone.demo.name_servers
}

output "budget_topic_arn" {
  description = "SNS topic that receives the budget alerts."
  value       = aws_sns_topic.budget.arn
}

output "ecr_repository_urls" {
  description = "ECR repository URLs by name, for `skopeo copy` and the environment's image variables."
  value       = { for k, r in aws_ecr_repository.images : k => r.repository_url }
}

output "ecr_repository_arns" {
  description = "ECR repository ARNs; pass them to ../environment as ecr_repository_arns."
  value       = [for r in aws_ecr_repository.images : r.arn]
}

output "account_budget_name" {
  description = "The account budget the environments check; pass it to ../environment as account_budget_name."
  value       = aws_budgets_budget.account.name
}

output "image_publisher_role_arn" {
  description = "Role the image workflow assumes through GitHub OIDC, or null without github_repository."
  value       = var.github_repository == null ? null : aws_iam_role.image_publisher[0].arn
}
