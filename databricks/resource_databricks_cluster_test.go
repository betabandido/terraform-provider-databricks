package databricks

import (
	"errors"
	"fmt"
	"github.com/betabandido/databricks-sdk-go/client"
	"github.com/betabandido/databricks-sdk-go/models"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"testing"
)

func TestAccDatabricksCluster_basic(t *testing.T) {
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
	name                    = "tf-test-cluster"
	spark_version           = "4.2.x-scala2.11"
	node_type_id            = "Standard_D3_v2"
	num_workers             = 1
	autotermination_minutes = 10
} 
`
}

func testAccDatabricksClusterConfigUpdate() string {
	return `
resource "databricks_cluster" "cluster" {
	name                    = "tf-test-cluster-renamed"
	spark_version           = "4.2.x-scala2.11"
	node_type_id            = "Standard_D3_v2"
	num_workers             = 2
	autotermination_minutes = 15
} 
`
}

func TestDatabricksCluster_handlesNonExistingClusterError(t *testing.T) {
	if resourceDatabricksClusterNotExistsError(errors.New("an error")) {
		t.Fatal("An error was incorrectly classified as non-existing-cluster error")
	}

	if !resourceDatabricksClusterNotExistsError(client.Error{
		ErrorResponse: models.ErrorResponse{
			ErrorCode: "INVALID_PARAMETER_VALUE",
			Message:   "Cluster foobar does not exist",
		},
	}) {
		t.Fatal("A non-existing-cluster error was not detected")
	}
}
