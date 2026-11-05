# tflint settings for every Terraform module under deploy/terraform.
# Run: tflint --init && tflint --recursive  (from deploy/terraform)

config {
  call_module_type = "local"
}

plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.50.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

plugin "google" {
  enabled = true
  version = "0.41.0"
  source  = "github.com/terraform-linters/tflint-ruleset-google"
}

plugin "azurerm" {
  enabled = true
  version = "0.33.0"
  source  = "github.com/terraform-linters/tflint-ruleset-azurerm"
}

# `prevent_destroy` cannot depend on a variable, so the modules protect data
# with switchable guards instead (deletion_protection: provider deletion
# protection, or a CanNotDelete management lock on Azure), which the module
# tests assert.
rule "azurerm_resources_missing_prevent_destroy" {
  enabled = false
}
