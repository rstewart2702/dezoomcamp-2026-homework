terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = 
  project = "datatalks-dezoomcamp2026"
  region  = "us-central1"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "datatalks-dezoomcamp2026-puddle-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "datatalks_dezoomcamp2026_dataset"
  project    = "datatalks-dezoomcamp2026"
  location   = "US"
}

# [2026-02-10 Tue 10:10] I *think* that I may need to set up another
# dataset, just for homework 4?  It might be nice.
#
# Can I build out tables herein as well?  Harder to say...
# Big Query tables were created using Kestra:  GCP interaction, or postgresql plugins
# or Python programs running against GCP.
# But building out datasets for bigquery could be useful here, eh?
resource "google_bigquery_dataset" "nytaxi-04" {
  dataset_id = "nytaxi_04"
  project    = "datatalks-dezoomcamp2026"
  location   = "US"
}
