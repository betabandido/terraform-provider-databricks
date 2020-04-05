// Test resource declaration

provider "databricks" {
  domain = ""
  token = ""
}

resource "databricks_notebook" "notebook" {
  path = "/Users/user_name/tf-test"
  language = "PYTHON"
  content = base64encode("print('generated by terraform')")
}

resource "databricks_cluster" "cluster" {
  name                    = "tf-test"
  spark_version           = "4.1.x-scala2.11"
  node_type_id            = "m4.large"
  num_workers             = 1
  autotermination_minutes = 10

  aws_attributes {
      zone_id = "eu-west-1c"
      ebs_volume_type = "GENERAL_PURPOSE_SSD"
      ebs_volume_count = 1
      ebs_volume_size = 100
    }

}