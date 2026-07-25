variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "node_count" {
  description = "Initial node count for GKE cluster"
  type        = number
  default     = 3
}

variable "min_node_count" {
  description = "Minimum node count for autoscaling"
  type        = number
  default     = 2
}

variable "max_node_count" {
  description = "Maximum node count for autoscaling"
  type        = number
  default     = 10
}

variable "machine_type" {
  description = "GKE node machine type"
  type        = string
  default     = "e2-standard-4"
}

variable "disk_size_gb" {
  description = "Disk size for GKE nodes in GB"
  type        = number
  default     = 100
}

variable "clickhouse_tier" {
  description = "Cloud SQL tier for ClickHouse"
  type        = string
  default     = "db-custom-4-16384"
}

variable "clickhouse_disk_size" {
  description = "Cloud SQL disk size in GB"
  type        = number
  default     = 100
}

variable "redis_memory_size" {
  description = "Memorystore Redis memory size in GB"
  type        = number
  default     = 5
}

variable "redis_tier" {
  description = "Memorystore Redis tier"
  type        = string
  default     = "STANDARD_HA"

  validation {
    condition     = contains(["BASIC", "STANDARD_HA"], var.redis_tier)
    error_message = "Redis tier must be BASIC or STANDARD_HA."
  }
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default = {
    managed_by  = "terraform"
    project     = "analytics-platform"
  }
}
