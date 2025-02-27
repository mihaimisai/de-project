terraform {
    required_providers {

        aws = {
            source = "hashicorp/aws"
            version = "~> 5.0"

        }
    }
    backend "s3" {
        bucket = "de-project-terraform-state"
        key = "terraform.tfstate"
        region = "eu-west-2"
    }
}

provider "aws"{
    region = "eu-west-2"
    default_tags{
        tags = {
          ProjectName = "DE_Project_Banshee"
          Team = "Banshee"
          DeployedFrom = "Terraform"
          Repository = "de-project"
          Environment = "dev"
        
        }
    }
}
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}