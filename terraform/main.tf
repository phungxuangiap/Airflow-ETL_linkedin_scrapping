terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.14.1"
    }
  }
}

provider "google" {
  project = var.google_project_name
  region  = var.google_region
}

module "gcp_warehouse" {
  source = "./modules/gcp_infra"
}