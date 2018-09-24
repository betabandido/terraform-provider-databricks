package databricks

import (
	"errors"
	"fmt"
	"github.com/betabandido/databricks-sdk-go/client"
	"github.com/betabandido/databricks-sdk-go/models"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"os"
	"strings"
	"testing"
)

func TestAccDatabricksCluster_basic(t *testing.T) {
	if v := os.Getenv("TEST_AWS"); v == "1" {
		t.Skip("Skipping test as TEST_AWS is set")
		return
	}

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckDatabricksClusterDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDatabricksClusterConfig(),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabricksClusterExists("databricks_cluster.cluster"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "name", "tf-test-cluster"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_version", "4.2.x-scala2.11"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "node_type_id", "Standard_D3_v2"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "num_workers", "1"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "autotermination_minutes", "10"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_env_vars.PYSPARK_PYTHON", "/databricks/python3/bin/python3"),
				),
			},
			{
				Config: testAccDatabricksClusterConfigUpdate(),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabricksClusterExists("databricks_cluster.cluster"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "name", "tf-test-cluster-renamed"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_version", "4.2.x-scala2.11"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "node_type_id", "Standard_D3_v2"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "num_workers", "2"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "autotermination_minutes", "15"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_env_vars.PYSPARK_PYTHON", "/databricks/python3/bin/python3"),
				),
			},
		},
	})
}

func TestAccDatabricksCluster_basic_aws(t *testing.T) {
	if v := os.Getenv("TEST_AWS"); v == "" {
		t.Skip("Skipping test as TEST_AWS is not set")
		return
	}

	awsARNString := os.Getenv("AWS_ARN_ROLE")

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckDatabricksClusterDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDatabricksClusterConfigAws(awsARNString),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabricksClusterExists("databricks_cluster.cluster"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "name", "tf-test-cluster"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_version", "4.2.x-scala2.11"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "node_type_id", "m4.large"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "aws_attributes.2335671095.instance_profile_arn", awsARNString),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "num_workers", "1"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "autotermination_minutes", "10"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_env_vars.PYSPARK_PYTHON", "/databricks/python3/bin/python3"),
				),
			},
			{
				Config: testAccDatabricksClusterConfigUpdateAws(awsARNString),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckDatabricksClusterExists("databricks_cluster.cluster"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "name", "tf-test-cluster-renamed"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_version", "4.2.x-scala2.11"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "node_type_id", "m4.large"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "aws_attributes.2335671095.instance_profile_arn", awsARNString),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "num_workers", "2"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "autotermination_minutes", "15"),
					resource.TestCheckResourceAttr(
						"databricks_cluster.cluster", "spark_env_vars.PYSPARK_PYTHON", "/databricks/python3/bin/python3"),
				),
			},
		},
	})
}

func testAccCheckDatabricksClusterExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("no ID is set")
		}

		conn := testAccProvider.Meta().(*Client).clusters

		_, err := conn.Get(&models.ClustersGetRequest{
			ClusterId: rs.Primary.ID,
		})
		if err != nil {
			return nil
		}

		return nil
	}
}

func testAccCheckDatabricksClusterDestroy(s *terraform.State) error {
	endpoint := testAccProvider.Meta().(*Client).clusters

	clusterId := s.RootModule().Resources["databricks_cluster.cluster"].Primary.ID

	_, err := endpoint.Get(&models.ClustersGetRequest{
		ClusterId: clusterId,
	})

	if err == nil {
		return errors.New("cluster still exists")
	}

	if !resourceDatabricksClusterNotExistsError(err) {
		return err
	}

	return nil
}

func testAccDatabricksClusterConfig() string {
	return `
resource "databricks_cluster" "cluster" {
  name                          = "tf-test-cluster"
  spark_version                 = "4.2.x-scala2.11"
  node_type_id                  = "m4.large"
  num_workers                   = 1
  autotermination_minutes       = 10
  permanently_delete            = true
  spark_env_vars {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }
} 
`
}

func testAccDatabricksClusterConfigAws(awsARNString string) string {

	awsTestConfig := `
resource "databricks_cluster" "cluster" {
  name                          = "tf-test-cluster"
  spark_version                 = "4.2.x-scala2.11"
  node_type_id                  = "m4.large"
  num_workers                   = 1
  autotermination_minutes       = 10
  permanently_delete            = true
  spark_env_vars = {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }
  aws_attributes = {
    instance_profile_arn = "AWSARNSTRING"
    zone_id              = "eu-west-1a"
    ebs_volume_type      = "THROUGHPUT_OPTIMIZED_HDD"
    ebs_volume_count     = 1
    ebs_volume_size      = 500
  }
}
`
	awsTestConfig = strings.Replace(awsTestConfig, "AWSARNSTRING", awsARNString, 1)
	return awsTestConfig

}

func testAccDatabricksClusterConfigUpdate() string {
	return `
resource "databricks_cluster" "cluster" {
  name                          = "tf-test-cluster-renamed"
  spark_version                 = "4.2.x-scala2.11"
  node_type_id                  = "Standard_D3_v2"
  num_workers                   = 2
  autotermination_minutes       = 15
  permanently_delete            = true
  spark_env_vars = {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }
} 
`
}

func testAccDatabricksClusterConfigUpdateAws(awsARNString string) string {
	awsTestConfig := `
resource "databricks_cluster" "cluster" {
  name                          = "tf-test-cluster-renamed"
  spark_version                 = "4.2.x-scala2.11"
  node_type_id                  = "m4.large"
  num_workers                   = 2
  autotermination_minutes       = 15
  permanently_delete            = true
  spark_env_vars = {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }
  aws_attributes = {
    instance_profile_arn = "AWSARNSTRING"
    zone_id              = "eu-west-1a"
    ebs_volume_type      = "THROUGHPUT_OPTIMIZED_HDD"
    ebs_volume_count     = 1
    ebs_volume_size      = 500
  }
}
`
	awsTestConfig = strings.Replace(awsTestConfig, "AWSARNSTRING", awsARNString, 1)
	return awsTestConfig
}

func TestDatabricksCluster_handlesNonExistingClusterError(t *testing.T) {
	if resourceDatabricksClusterNotExistsError(errors.New("an error")) {
		t.Fatal("An error was incorrectly classified as non-existing-cluster error")
	}

	if !resourceDatabricksClusterNotExistsError(client.NewError(
		models.ErrorResponse{
			ErrorCode: "INVALID_PARAMETER_VALUE",
			Message:   "Cluster foobar does not exist",
		},
		400,
	)) {
		t.Fatal("A non-existing-cluster error was not detected")
	}
}
