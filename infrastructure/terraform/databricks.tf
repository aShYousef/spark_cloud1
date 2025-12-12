# Databricks workspace configuration
# Requires Databricks provider configuration

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  default     = ""
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
  default     = ""
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Cluster policy for cost control
resource "databricks_cluster_policy" "spark_processor_policy" {
  count = var.databricks_host != "" ? 1 : 0
  
  name = "spark-data-processor-policy"
  definition = jsonencode({
    "autotermination_minutes" : {
      "type" : "fixed",
      "value" : 30
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["i3.xlarge", "i3.2xlarge", "m5.xlarge", "m5.2xlarge"]
    },
    "num_workers" : {
      "type" : "range",
      "minValue" : 1,
      "maxValue" : 8
    },
    "spark_version" : {
      "type" : "regex",
      "pattern" : "13\\.[0-9]+\\.x-scala2\\.12"
    }
  })
}

# Sample cluster configuration
resource "databricks_cluster" "spark_processor_cluster" {
  count = var.databricks_host != "" ? 1 : 0
  
  cluster_name            = "spark-data-processor-cluster"
  spark_version           = "13.3.x-scala2.12"
  node_type_id           = "i3.xlarge"
  driver_node_type_id    = "i3.xlarge"
  autotermination_minutes = 30
  num_workers            = 2

  spark_conf = {
    "spark.speculation" = "true"
  }

  custom_tags = {
    "Project"     = "spark-data-processor"
    "Environment" = var.environment
  }
}

# Notebook directory
resource "databricks_directory" "spark_processor_notebooks" {
  count = var.databricks_host != "" ? 1 : 0
  path  = "/Shared/spark-data-processor"
}

output "databricks_cluster_id" {
  value       = var.databricks_host != "" ? databricks_cluster.spark_processor_cluster[0].id : ""
  description = "Databricks cluster ID"
}
