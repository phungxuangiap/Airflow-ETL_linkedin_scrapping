resource "google_storage_bucket" "linkedin-scrapping-bucket" {
  name     = var.bucket_name
  location = var.location
}