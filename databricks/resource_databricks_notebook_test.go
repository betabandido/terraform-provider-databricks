package databricks

import (
	"fmt"
	"github.com/betabandido/databricks-sdk-go/client"
	"github.com/betabandido/databricks-sdk-go/models"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"os"
	"testing"
)

func TestAccDatabricksNotebook_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckDatabricksNotebookDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccDatabricksNotebookConfig,
			},
		},
	})
}

func testAccCheckDatabricksNotebookDestroy(s *terraform.State) error {
	endpoint := testAccProvider.Meta().(*Client).workspace

	path := s.RootModule().Resources["databricks_notebook.notebook"].Primary.ID

	_, err := endpoint.GetStatus(&models.WorkspaceGetStatusRequest{
		Path: path,
	})

	if err == nil {
		return fmt.Errorf("notebook still exists")
	}

	respErr, ok := err.(client.Error)
	if !ok {
		return err
	}

	if respErr.Code() != "RESOURCE_DOES_NOT_EXIST" {
		return err
	}

	return nil
}

var testAccDatabricksNotebookConfig = fmt.Sprintf(`
resource "databricks_notebook" "notebook" {
    path = "%s/tf-test"
    language = "PYTHON"
    content = "${base64encode("print('generated from terraform')")}"
}
`, os.Getenv("DATABRICKS_WORKSPACE"))
