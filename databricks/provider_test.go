package databricks

import (
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
	"os"
	"testing"
)

var testAccProviders map[string]terraform.ResourceProvider
var testAccProvider *schema.Provider

func init() {
	testAccProvider = Provider()
	testAccProviders = map[string]terraform.ResourceProvider{
		"databricks": testAccProvider,
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ = Provider()
}

func testAccPreCheck(t *testing.T) {
	if v := os.Getenv("DATABRICKS_DOMAIN"); v == "" {
		t.Fatal("DATABRICKS_DOMAIN must be set for acceptance tests")
	}

	if v := os.Getenv("DATABRICKS_TOKEN"); v == "" {
		t.Fatal("DATABRICKS_TOKEN must be set for acceptance tests")
	}

	if v := os.Getenv("DATABRICKS_WORKSPACE"); v == "" {
		t.Fatal("DATABRICKS_WORKSPACE must be set for acceptance tests")
	}

	if v := os.Getenv("TEST_AWS"); v == "1" {
		if arn := os.Getenv("AWS_ARN_ROLE"); arn == "" {
			t.Fatal("AWS_ARN_ROLE must be set for acceptance tests with AWS")
		}
	}

}
