terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# GCS Bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  name          = "${var.gcp_project_id}-data-lake"
  location      = var.gcp_region
  force_destroy = true
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  # Delete files older than 30 days
    }
  }
}

# BigQuery Dataset: raw_data
resource "google_bigquery_dataset" "raw_data" {
  dataset_id    = "raw_data"
  friendly_name = "Raw Data from Sources"
  description   = "Raw data from Kaggle and API sources"
  location      = var.gcp_region
  
  delete_contents_on_destroy = true
}

# BigQuery Dataset: staging
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "staging"
  friendly_name = "Staging Layer"
  description   = "Cleaned and standardized data from dbt"
  location      = var.gcp_region
  
  delete_contents_on_destroy = true
}

# BigQuery Dataset: intermediate
resource "google_bigquery_dataset" "intermediate" {
  dataset_id    = "intermediate"
  friendly_name = "Intermediate Layer"
  description   = "Joined and enriched data from dbt"
  location      = var.gcp_region
  
  delete_contents_on_destroy = true
}

# BigQuery Dataset: marts
resource "google_bigquery_dataset" "marts" {
  dataset_id    = "marts"
  friendly_name = "Data Marts"
  description   = "Business-ready data marts for ML and analytics"
  location      = var.gcp_region
  
  delete_contents_on_destroy = true
}

# BigQuery Dataset: ml_models
resource "google_bigquery_dataset" "ml_models" {
  dataset_id    = "ml_models"
  friendly_name = "ML Models"
  description   = "BigQuery ML models for predictions"
  location      = var.gcp_region
  
  delete_contents_on_destroy = true
}

# BigQuery Dataset: predictions
resource "google_bigquery_dataset" "predictions" {
  dataset_id    = "predictions"
  friendly_name = "Predictions"
  description   = "ML prediction results"
  location      = var.gcp_region
  
  delete_contents_on_destroy = true
}
